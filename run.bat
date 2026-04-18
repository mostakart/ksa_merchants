@echo off
echo ============================================
echo   KSA Merchant Web Intelligence Scraper
echo ============================================
echo.

echo [1/3] Running batch scraper...
python -m scraper.batch_runner %*
if errorlevel 1 goto error

echo.
echo [2/3] Merging results into Excel files...
python -m scraper.merger
if errorlevel 1 goto error

echo.
echo [3/3] Generating intelligence report...
python -m scraper.intel_report
if errorlevel 1 goto error

echo.
echo ============================================
echo   All done!
echo   Output files:
echo     scraped_urls.json       (checkpoint)
echo     scraped_results.json    (raw results)
echo     scraped_results.csv     (flat CSV)
echo     *_ENRICHED.xlsx         (enriched Excel)
echo     intel_report.md         (summary report)
echo ============================================
goto :EOF

:error
echo.
echo ============================================
echo   [ERROR] The pipeline failed!
echo ============================================
exit /b 1
