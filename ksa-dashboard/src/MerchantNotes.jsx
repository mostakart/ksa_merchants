import React, { useState, useEffect, useRef } from 'react';
import EmojiPicker from 'emoji-picker-react';
import ReactQuill from 'react-quill-new';
import 'react-quill-new/dist/quill.snow.css';

const SB_URL = process.env.REACT_APP_SUPABASE_URL || "https://omowdfzyudedrtcuhnvy.supabase.co";

const sbH = (key) => ({
  apikey: key,
  Authorization: `Bearer ${key}`,
  "Content-Type": "application/json",
  "Prefer": "return=representation"
});

const mapLegacyReactions = (reactions) => {
  if (!reactions) return {};
  const mapped = { ...reactions };
  if (mapped.thumbsUp !== undefined) { mapped['👍'] = mapped.thumbsUp; delete mapped.thumbsUp; }
  if (mapped.heart !== undefined) { mapped['❤️'] = mapped.heart; delete mapped.heart; }
  if (mapped.check !== undefined) { mapped['✅'] = mapped.check; delete mapped.check; }
  
  for (const [key, val] of Object.entries(mapped)) {
    if (typeof val === 'number') {
      mapped[key] = Array(val).fill('Legacy User');
    }
  }
  return mapped;
};

const modules = {
  toolbar: [
    ['bold', 'italic', 'underline', 'strike'],
    [{ 'list': 'ordered'}, { 'list': 'bullet' }],
    ['link'],
    ['clean']
  ],
};

export default function MerchantNotes({ merchantId, authorName, anonKey, userRole, region = "KSA" }) {
  const canEditNotes = userRole === 'admin' || userRole === 'global_bd' || (userRole === 'ksa_bd' && region === 'KSA') || (userRole === 'oman_bd' && region === 'Oman');
  const [notes, setNotes] = useState([]);
  const [newNote, setNewNote] = useState('');
  const [attachments, setAttachments] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [loading, setLoading] = useState(false);
  
  const [activePickerId, setActivePickerId] = useState(null);
  const [menuOpenId, setMenuOpenId] = useState(null);
  
  const pickerRef = useRef(null);
  const menuRef = useRef(null);
  const fileInputRef = useRef(null);

  const fetchNotes = async () => {
    if (!merchantId || !anonKey) return;
    setLoading(true);
    try {
      const res = await fetch(`${SB_URL}/rest/v1/merchant_notes?merchant_id=eq.${encodeURIComponent(merchantId)}&order=created_at.desc`, {
        headers: sbH(anonKey)
      });
      if (res.ok) {
        const data = await res.json();
        const normalizedData = data.map(n => ({ 
          ...n, 
          reactions: mapLegacyReactions(n.reactions),
          attachments: n.attachments || []
        }));
        setNotes(normalizedData);
      }
    } catch (e) {
      console.error("Failed to fetch notes", e);
    }
    setLoading(false);
  };

  useEffect(() => {
    fetchNotes();
    setNewNote('');
    setAttachments([]);
  }, [merchantId, anonKey]);

  useEffect(() => {
    function handleClickOutside(event) {
      if (pickerRef.current && !pickerRef.current.contains(event.target)) setActivePickerId(null);
      if (menuRef.current && !menuRef.current.contains(event.target)) setMenuOpenId(null);
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleFileUpload = async (e) => {
    if (!anonKey) {
      alert("Missing API Key. Cannot upload.");
      return;
    }
    const file = e.target.files[0];
    if (!file) return;

    if (file.size > 2 * 1024 * 1024) {
      alert("File is too large. Maximum size is 2MB.");
      return;
    }

    setUploading(true);
    const fileExt = file.name.split('.').pop();
    const fileName = `${merchantId.replace(/[^a-zA-Z0-9]/g, '_')}_${Date.now()}.${fileExt}`;

    try {
      const res = await fetch(`${SB_URL}/storage/v1/object/merchant_attachments/${fileName}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${anonKey}`,
          'apikey': anonKey,
          'Content-Type': file.type
        },
        body: file
      });

      if (!res.ok) throw new Error(await res.text());

      const fileUrl = `${SB_URL}/storage/v1/object/public/merchant_attachments/${fileName}`;
      setAttachments([...attachments, { name: file.name, url: fileUrl, type: file.type }]);
    } catch (err) {
      console.error(err);
      alert("Failed to upload file. Check storage policies.");
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleRemoveAttachment = (index) => {
    setAttachments(attachments.filter((_, i) => i !== index));
  };

  const handleAddNote = async () => {
    if (!anonKey) {
      alert("Missing API Key. Cannot save note.");
      return;
    }
    
    const textContent = newNote.replace(/<[^>]*>/g, '').trim();
    const isActuallyEmpty = textContent.length === 0 && attachments.length === 0;
    
    if (isActuallyEmpty) return;

    setLoading(true);
    try {
      const res = await fetch(`${SB_URL}/rest/v1/merchant_notes`, {
        method: "POST",
        headers: sbH(anonKey),
        body: JSON.stringify({
          merchant_id: merchantId,
          author_name: authorName,
          content: newNote,
          reactions: {},
          attachments: attachments
        })
      });
      if (res.ok) {
        setNewNote('');
        setAttachments([]);
        fetchNotes();
      } else {
        const errTxt = await res.text();
        console.error("Save failed:", errTxt);
        alert("Failed to save note: " + errTxt);
      }
    } catch (e) {
      console.error("Failed to add note", e);
      alert("Error saving note. Check your connection.");
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteNote = async (id) => {
    setMenuOpenId(null);
    if (!window.confirm("Are you sure you want to delete this note?")) return;
    try {
      const res = await fetch(`${SB_URL}/rest/v1/merchant_notes?id=eq.${id}`, {
        method: "DELETE",
        headers: sbH(anonKey)
      });
      if (res.ok) fetchNotes();
    } catch (e) {
      console.error("Failed to delete note", e);
    }
  };

  const handleReaction = async (note, emojiStr) => {
    const currentArray = note.reactions[emojiStr] || [];
    let newArray;

    if (currentArray.includes(authorName)) {
      newArray = currentArray.filter(u => u !== authorName);
    } else {
      newArray = [...currentArray, authorName];
    }

    const updatedReactions = { ...note.reactions, [emojiStr]: newArray };
    if (newArray.length === 0) delete updatedReactions[emojiStr];

    setNotes(notes.map(n => n.id === note.id ? { ...n, reactions: updatedReactions } : n));
    setActivePickerId(null);

    try {
      await fetch(`${SB_URL}/rest/v1/merchant_notes?id=eq.${note.id}`, {
        method: "PATCH",
        headers: sbH(anonKey),
        body: JSON.stringify({ reactions: updatedReactions })
      });
    } catch (e) {
      console.error("Failed to update reaction", e);
    }
  };

  const timeAgo = (dateStr) => {
    const diffMs = new Date() - new Date(dateStr);
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHrs = Math.floor(diffMins / 60);
    if (diffHrs < 24) return `${diffHrs}h ago`;
    return `${Math.floor(diffHrs / 24)}d ago`;
  };

  const isEditorEmpty = newNote.replace(/<[^>]*>/g, '').trim().length === 0 && attachments.length === 0;

  return (
    <div style={styles.container}>
      <style>{`
        .ql-container { font-family: inherit !important; font-size: 14px; border: none !important; }
        .ql-toolbar { border: none !important; border-bottom: 1px solid #E5E7EB !important; background: #F9FAFB; border-top-left-radius: 8px; border-top-right-radius: 8px; direction: ltr; }
        .ql-editor { min-height: 100px; text-align: start; unicode-bidi: plaintext; padding: 12px 16px; }
        .ql-editor p { direction: auto; }
        .note-more-btn:hover { background: #F3F4F6 !important; color: #374151 !important; }
        .note-dropdown-item:hover { background: #F9FAFB !important; }
      `}</style>
      
      <h3 style={styles.header}>Team Notes & Comments</h3>
      
      {canEditNotes && (
        <div style={styles.inputArea}>
        <div style={styles.avatarPlaceholder}>{authorName.charAt(0).toUpperCase()}</div>
        
        <div style={styles.editorBox}>
          <ReactQuill 
            theme="snow" 
            value={newNote} 
            onChange={setNewNote} 
            modules={modules}
            placeholder="Add a comment or attach files..."
          />

          {attachments.length > 0 && (
            <div style={styles.attachmentPreviewArea}>
              {attachments.map((att, idx) => (
                <div key={idx} style={styles.attachmentChip}>
                  <span style={styles.attachmentName}>{att.name}</span>
                  <button style={styles.attachmentRemove} onClick={() => handleRemoveAttachment(idx)}>×</button>
                </div>
              ))}
            </div>
          )}

          <div style={{...styles.actionRow, position: 'relative', zIndex: 10}}>
            <div style={{ display: 'flex', alignItems: 'center' }}>
              <input 
                type="file" 
                ref={fileInputRef} 
                onChange={handleFileUpload} 
                style={{ display: 'none' }} 
              />
              <button 
                style={styles.attachBtn} 
                onClick={(e) => { 
                  e.preventDefault(); 
                  console.log("Attach File clicked");
                  fileInputRef.current.click(); 
                }}
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: 6 }}>
                  <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48" />
                </svg>
                {uploading ? 'Uploading...' : 'Attach File'}
              </button>
            </div>
            
            <button
              style={{ 
                ...styles.submitBtn, 
                opacity: isEditorEmpty || loading ? 0.6 : 1,
                cursor: isEditorEmpty || loading ? 'not-allowed' : 'pointer'
              }}
              onClick={(e) => { 
                e.preventDefault(); 
                console.log("Add Note clicked");
                if (isEditorEmpty) {
                  return; // Don't do anything if empty, or could alert
                }
                handleAddNote(); 
              }}
            >
              {loading ? 'Adding...' : 'Add Note'}
            </button>
          </div>
        </div>
      )}

      <div style={styles.notesList}>
        {loading && notes.length === 0 ? (
          <div style={styles.loading}>Loading notes...</div>
        ) : notes.length === 0 ? (
          <div style={styles.empty}>No notes yet. Be the first to comment!</div>
        ) : (
          notes.map(note => (
            <div key={note.id} style={styles.noteItem}>
              <div style={styles.avatarPlaceholderSmall}>{note.author_name.charAt(0).toUpperCase()}</div>
              
              <div style={styles.noteContentWrapper}>
                <div style={styles.noteBubble}>
                  <div style={styles.noteHeader}>
                    <span style={styles.noteAuthor}>{note.author_name}</span>
                    <span style={styles.noteTime}>{timeAgo(note.created_at)}</span>
                    
                    <div style={styles.menuContainer}>
                      <button 
                        className="note-more-btn"
                        style={styles.moreBtn} 
                        onClick={() => setMenuOpenId(menuOpenId === note.id ? null : note.id)}
                      >
                        ⋮
                      </button>
                      {menuOpenId === note.id && (
                        <div style={styles.dropdownMenu} ref={menuRef}>
                          {(note.author_name === authorName || userRole === 'admin') && (
                            <button className="note-dropdown-item" style={styles.dropdownItem} onClick={() => handleDeleteNote(note.id)}>
                              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#DC2626" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                <polyline points="3 6 5 6 21 6"></polyline>
                                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                                <line x1="10" y1="11" x2="10" y2="17"></line>
                                <line x1="14" y1="11" x2="14" y2="17"></line>
                              </svg>
                              <span style={{ color: '#DC2626' }}>Delete note</span>
                            </button>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
                  
                  <div style={styles.noteText} dir="auto" dangerouslySetInnerHTML={{ __html: note.content }}></div>
                  
                  {note.attachments && note.attachments.length > 0 && (
                    <div style={styles.noteAttachments}>
                      {note.attachments.map((att, idx) => (
                        <a key={idx} href={att.url} target="_blank" rel="noreferrer" style={styles.attachmentLink}>
                          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: 6 }}>
                            <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48" />
                          </svg>
                          {att.name}
                        </a>
                      ))}
                    </div>
                  )}
                </div>

                <div style={styles.reactionsArea}>
                  <div style={styles.reactionsList}>
                    {Object.entries(note.reactions || {}).filter(([_, arr]) => arr && arr.length > 0).map(([emoji, arr]) => {
                      const userReacted = arr.includes(authorName);
                      return (
                        <button 
                          key={emoji} 
                          style={{ ...styles.reactionBadge, background: userReacted ? '#E5E7EB' : '#FFFFFF', borderColor: userReacted ? '#D1D5DB' : '#E5E7EB' }} 
                          onClick={() => handleReaction(note, emoji)}
                          title={arr.join(', ')}
                        >
                          <span style={{ fontSize: 13 }}>{emoji}</span> 
                          <span style={{ fontSize: 12, fontWeight: 500, color: '#4A4A4A' }}>{arr.length}</span>
                        </button>
                      );
                    })}
                    
                    <div style={{ position: 'relative' }}>
                      <button 
                        style={styles.addReactionBtn} 
                        onClick={() => setActivePickerId(activePickerId === note.id ? null : note.id)}
                      >
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                          <circle cx="12" cy="12" r="10" />
                          <path d="M8 14s1.5 2 4 2 4-2 4-2" />
                          <line x1="9" y1="9" x2="9.01" y2="9" />
                          <line x1="15" y1="9" x2="15.01" y2="9" />
                        </svg>
                        <span style={{ fontSize: 16, lineHeight: 1, marginTop: -2 }}>+</span>
                      </button>
                      
                      {activePickerId === note.id && (
                        <div style={styles.pickerPopup} ref={pickerRef}>
                          <EmojiPicker 
                            onEmojiClick={(emojiObj) => handleReaction(note, emojiObj.emoji)}
                            width={300}
                            height={400}
                            searchDisabled={false}
                          />
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

const styles = {
  container: {
    background: '#FFFFFF',
    border: '1px solid #E8E4DF',
    borderRadius: 12,
    padding: 24,
    fontFamily: 'system-ui, -apple-system, sans-serif',
    marginTop: 16
  },
  header: {
    margin: '0 0 20px',
    fontSize: 18,
    fontWeight: 700,
    color: '#111827',
    letterSpacing: '-0.01em'
  },
  inputArea: {
    display: 'flex',
    gap: 16,
    marginBottom: 32
  },
  avatarPlaceholder: {
    width: 36,
    height: 36,
    borderRadius: '50%',
    background: 'linear-gradient(135deg, #FF5A00 0%, #E65100 100%)',
    color: '#FFFFFF',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontWeight: 700,
    fontSize: 15,
    flexShrink: 0,
    boxShadow: '0 2px 4px rgba(255, 90, 0, 0.2)'
  },
  avatarPlaceholderSmall: {
    width: 32,
    height: 32,
    borderRadius: '50%',
    background: 'linear-gradient(135deg, #FF5A00 0%, #E65100 100%)',
    color: '#FFFFFF',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontWeight: 700,
    fontSize: 14,
    flexShrink: 0,
  },
  editorBox: {
    flex: 1,
    border: '1px solid #E5E7EB',
    borderRadius: 8,
    overflow: 'hidden',
    background: '#FFF',
    display: 'flex',
    flexDirection: 'column',
    boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
  },
  actionRow: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: '8px 12px',
    background: '#F9FAFB',
    borderTop: '1px solid #E5E7EB',
  },
  attachBtn: {
    background: '#FFF',
    border: '1px solid #D1D5DB',
    padding: '6px 12px',
    borderRadius: 6,
    fontSize: 12,
    color: '#4B5563',
    cursor: 'pointer',
    fontWeight: 500,
    display: 'flex',
    alignItems: 'center',
    transition: 'all 0.2s',
  },
  submitBtn: {
    background: '#FF5A00',
    color: '#FFF',
    border: 'none',
    padding: '8px 24px',
    borderRadius: 6,
    fontSize: 13,
    fontWeight: 600,
    cursor: 'pointer',
    transition: 'all 0.2s',
    boxShadow: '0 2px 4px rgba(255, 90, 0, 0.1)'
  },
  attachmentPreviewArea: {
    padding: '8px 16px',
    background: '#FFF',
    display: 'flex',
    flexWrap: 'wrap',
    gap: 8,
    borderTop: '1px solid #F3F4F6'
  },
  attachmentChip: {
    background: '#F3F4F6',
    border: '1px solid #E5E7EB',
    borderRadius: 16,
    padding: '4px 10px',
    fontSize: 11,
    display: 'flex',
    alignItems: 'center',
    gap: 6
  },
  attachmentName: {
    maxWidth: 180,
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    color: '#374151'
  },
  attachmentRemove: {
    background: 'none',
    border: 'none',
    color: '#9CA3AF',
    cursor: 'pointer',
    fontSize: 14,
    padding: 0,
    lineHeight: 1
  },
  notesList: {
    display: 'flex',
    flexDirection: 'column',
    gap: 20
  },
  noteItem: {
    display: 'flex',
    gap: 16,
  },
  menuContainer: {
    position: 'relative',
    marginLeft: 'auto'
  },
  moreBtn: {
    background: 'none',
    border: 'none',
    color: '#9CA3AF',
    fontSize: 18,
    cursor: 'pointer',
    padding: '0 4px',
    lineHeight: 1,
    borderRadius: 4,
    transition: 'all 0.2s',
  },
  dropdownMenu: {
    position: 'absolute',
    top: '100%',
    right: 0,
    background: '#FFF',
    border: '1px solid #E5E7EB',
    borderRadius: 8,
    boxShadow: '0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)',
    zIndex: 100,
    minWidth: 140,
    marginTop: 4,
    padding: 4,
    overflow: 'hidden'
  },
  dropdownItem: {
    width: '100%',
    textAlign: 'left',
    background: 'none',
    border: 'none',
    padding: '8px 12px',
    fontSize: 13,
    fontWeight: 500,
    cursor: 'pointer',
    borderRadius: 6,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    transition: 'background 0.2s',
    color: '#374151'
  },
  noteContentWrapper: {
    flex: 1,
    display: 'flex',
    flexDirection: 'column',
  },
  noteBubble: {
    background: '#F3F4F6',
    borderRadius: '0 12px 12px 12px',
    padding: '12px 16px',
    width: '100%',
    boxSizing: 'border-box'
  },
  noteHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 10,
    marginBottom: 6
  },
  noteAuthor: {
    fontSize: 13,
    fontWeight: 700,
    color: '#111827'
  },
  noteTime: {
    fontSize: 11,
    color: '#6B7280'
  },
  noteText: {
    fontSize: 14,
    color: '#374151',
    lineHeight: 1.5,
    wordBreak: 'break-word',
  },
  noteAttachments: {
    marginTop: 12,
    display: 'flex',
    flexWrap: 'wrap',
    gap: 8,
    paddingTop: 10,
    borderTop: '1px dashed #D1D5DB'
  },
  attachmentLink: {
    display: 'inline-flex',
    alignItems: 'center',
    background: '#FFF',
    border: '1px solid #E5E7EB',
    padding: '4px 10px',
    borderRadius: 4,
    fontSize: 11,
    color: '#2563EB',
    textDecoration: 'none',
    fontWeight: 500
  },
  reactionsArea: {
    marginTop: 6,
    marginLeft: 4
  },
  reactionsList: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 6,
    alignItems: 'center'
  },
  reactionBadge: {
    background: '#FFFFFF',
    border: '1px solid #E5E7EB',
    padding: '3px 8px',
    borderRadius: 20,
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    cursor: 'pointer',
  },
  addReactionBtn: {
    background: '#FFFFFF',
    border: '1px solid #E5E7EB',
    color: '#9CA3AF',
    width: 28,
    height: 28,
    borderRadius: '50%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    cursor: 'pointer',
    padding: 0
  },
  pickerPopup: {
    position: 'absolute',
    top: '100%',
    left: 0,
    zIndex: 1000,
    marginTop: 8,
    boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
  },
  loading: { fontSize: 14, color: '#6B7280', textAlign: 'center', padding: 30 },
  empty: { fontSize: 14, color: '#6B7280', textAlign: 'center', padding: 40, background: '#F9FAFB', borderRadius: 12, border: '1px dashed #E5E7EB' }
};
