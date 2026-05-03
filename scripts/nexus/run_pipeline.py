import asyncio
import sys
from loguru import logger
from supabase import create_client, Client
from config import get_settings
from nexus_agent import CompetitiveIntelligenceAgent

def setup_supabase_logging():
    try:
        settings = get_settings()
        sb: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
        
        def supabase_sink(message):
            record = message.record
            try:
                sb.table('agent_logs').insert({
                    'level': record['level'].name,
                    'module': record['name'],
                    'message': record['message']
                }).execute()
            except Exception:
                pass
                
        # enqueue=True ensures it's non-blocking
        logger.add(supabase_sink, enqueue=True)
    except Exception:
        pass

# Initialize once
setup_supabase_logging()

async def run_pipeline(batch_size=5, active_only=True):
    settings = get_settings()
    supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    
    logger.info(f"Fetching merchants from Supabase (active_only={active_only})...")
    
    if active_only:
        # Use the RPC function to get competitors due for scraping (respecting frequency and priority)
        response = supabase.rpc('get_competitors_for_scraping', {}).limit(batch_size).execute()
    else:
        # Fetch any competitors regardless of status/frequency (e.g. for manual diagnostic runs)
        response = supabase.table('competitors_directory').select('id,competitor_name,instagram_url,facebook_url').limit(batch_size).execute()
        
    merchants = response.data
    
    # Map 'competitor_id' back to 'id' if the RPC returns it that way
    if merchants:
        for m in merchants:
            if 'competitor_id' in m and 'id' not in m:
                m['id'] = m['competitor_id']
    
    if not merchants:
        logger.warning("No merchants found matching the criteria.")
        return
        
    logger.info(f"Found {len(merchants)} merchants. Initializing agent...")
    
    agent = CompetitiveIntelligenceAgent(settings)
    
    # Start the browser context
    await agent.start_browser()
    
    try:
        # Create a semaphore to limit concurrent processing
        semaphore = asyncio.Semaphore(batch_size)
        
        async def process_with_semaphore(merchant):
            async with semaphore:
                try:
                    # Final check: Ensure merchant hasn't been paused while the job was waiting
                    if active_only:
                        check = supabase.table('competitors_directory').select('is_active').eq('id', merchant['id']).execute()
                        if not check.data or not check.data[0]['is_active']:
                            logger.warning(f"Skipping {merchant['competitor_name']} - Merchant was paused during job queueing.")
                            return

                    # Rename id to competitor_id to match the agent's expected structure
                    merchant_data = merchant.copy()
                    merchant_data['competitor_id'] = merchant_data['id']
                    
                    logger.info(f"Starting orchestration for: {merchant_data['competitor_name']}")
                    await agent.process_competitor(merchant_data)
                    logger.success(f"Finished orchestration for: {merchant_data['competitor_name']}")
                except Exception as e:
                    logger.error(f"Failed processing {merchant['competitor_name']}: {e}")
                    
        # Create and gather tasks
        tasks = [process_with_semaphore(m) for m in merchants]
        await asyncio.gather(*tasks)
        
    finally:
        if agent.browser:
            await agent.browser.close()
        logger.info("Pipeline execution completed.")

import time
from datetime import datetime, timezone

async def poll_jobs():
    settings = get_settings()
    supabase: Client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    
    logger.info("Starting pipeline daemon. Listening for jobs from dashboard...")
    
    while True:
        try:
            # Check for pending jobs
            res = supabase.table('pipeline_jobs').select('*').eq('status', 'pending').order('created_at').limit(1).execute()
            jobs = res.data
            
            if jobs:
                job = jobs[0]
                job_id = job['id']
                batch_size = job.get('batch_size', 5)
                active_only = job.get('active_only', True) # Default to True if column is missing or null
                
                logger.info(f"Picked up new job {job_id} | batch size: {batch_size} | active_only: {active_only}")
                
                # Update status to running
                supabase.table('pipeline_jobs').update({
                    'status': 'running',
                    'started_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', job_id).execute()
                
                # Execute pipeline
                try:
                    await run_pipeline(batch_size=batch_size, active_only=active_only)
                    
                    # Mark complete
                    supabase.table('pipeline_jobs').update({
                        'status': 'completed',
                        'completed_at': datetime.now(timezone.utc).isoformat()
                    }).eq('id', job_id).execute()
                    logger.success(f"Job {job_id} completed successfully.")
                    
                except Exception as e:
                    logger.error(f"Job {job_id} failed: {e}")
                    supabase.table('pipeline_jobs').update({
                        'status': 'failed',
                        'error_message': str(e),
                        'completed_at': datetime.now(timezone.utc).isoformat()
                    }).eq('id', job_id).execute()
                    
        except Exception as e:
            logger.error(f"Daemon error checking jobs: {e}")
            
        time.sleep(10)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        asyncio.run(poll_jobs())
    else:
        batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 5
        asyncio.run(run_pipeline(batch_size))
