import React, { useState, useEffect } from 'react';
import { Database, Edit3, Tag, History, User, ShieldCheck, Loader2 } from 'lucide-react';
import './style.css';

const MetadataEvolution: React.FC = () => {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const response = await fetch('http://localhost:8000/api/evolution/customers');
        if (!response.ok) throw new Error('Failed to fetch evolution data');
        const json = await response.json();
        setData(json);
        setSelectedIndex(json.length - 1);
        setLoading(false);
      } catch (err: any) {
        setError(err.message);
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return (
    <div className="evolution-container" style={{ justifyContent: 'center', alignItems: 'center' }}>
      <Loader2 className="animate-spin" size={48} color="#60a5fa" />
      <div style={{ marginLeft: '16px', fontSize: '1.2rem' }}>Fetching live metadata evolution...</div>
    </div>
  );

  if (error) return (
    <div className="evolution-container" style={{ justifyContent: 'center', alignItems: 'center', color: '#ef4444' }}>
      Error: {error}
    </div>
  );

  if (data.length === 0) return (
    <div className="evolution-container" style={{ justifyContent: 'center', alignItems: 'center' }}>
      No evolution data found for this table.
    </div>
  );

  const currentEvent = data[selectedIndex];
  const previousEvent = selectedIndex > 0 ? data[selectedIndex - 1] : null;

  const formatDate = (iso: string) => new Date(iso).toLocaleString();

  const getChangeSummary = (current: any, previous: any) => {
    if (!previous) return "Initial metadata capture";
    
    const changes: string[] = [];
    
    // Schema changes
    const curFields = current.snapshot.fields.map((f: any) => f.name);
    const prevFields = previous.snapshot.fields.map((f: any) => f.name);
    const added = curFields.filter((f: string) => !prevFields.includes(f));
    const removed = prevFields.filter((f: string) => !curFields.includes(f));
    
    if (added.length > 0) changes.push(`Added: ${added.join(', ')}`);
    if (removed.length > 0) changes.push(`Removed: ${removed.join(', ')}`);
    
    // Check for type changes or metadata changes in existing fields
    const modifiedFields: string[] = [];
    curFields.forEach((fn: string) => {
      if (prevFields.includes(fn)) {
        const curF = current.snapshot.fields.find((f: any) => f.name === fn);
        const prevF = previous.snapshot.fields.find((f: any) => f.name === fn);
        if (curF.dataType !== prevF.dataType) modifiedFields.push(`${fn} type`);
      }
    });
    
    // Aspect changes (PII tags)
    const curColAspects = current.snapshot.column_aspects || {};
    const prevColAspects = previous.snapshot.column_aspects || {};
    Object.keys(curColAspects).forEach(col => {
      const curTags = curColAspects[col] || [];
      const prevTags = prevColAspects[col] || [];
      if (JSON.stringify(curTags.sort()) !== JSON.stringify(prevTags.sort())) {
        modifiedFields.push(`${col} tags`);
      }
    });

    if (modifiedFields.length > 0) changes.push(`Updated: ${modifiedFields.join(', ')}`);

    // Governance changes
    const curGov = current.snapshot.aspects["data-governance-aspect"] || {};
    const prevGov = previous.snapshot.aspects["data-governance-aspect"] || {};
    if (JSON.stringify(curGov) !== JSON.stringify(prevGov)) {
      changes.push("Governance metadata updated");
    }

    return changes.length > 0 ? changes.join(' • ') : "Metadata verification (no changes)";
  };

  const getIcon = (summary: string, type: string) => {
    if (type === 'CREATE') return <Database size={16} />;
    if (summary.includes('Added') || summary.includes('Removed')) return <History size={16} />;
    if (summary.includes('tag') || summary.includes('Governance')) return <Tag size={16} />;
    return <Edit3 size={16} />;
  };

  return (
    <div className="evolution-container">
      {/* Sidebar Timeline */}
      <aside className="timeline-sidebar">
        <h2 className="timeline-title">Activity Feed</h2>
        <div className="timeline-list">
          {data.map((event, idx) => {
            const prev = idx > 0 ? data[idx - 1] : null;
            const summary = getChangeSummary(event, prev);
            return (
              <div 
                key={event.id} 
                className={`timeline-item ${idx === selectedIndex ? 'active' : ''}`}
                onClick={() => setSelectedIndex(idx)}
              >
                <div className="timeline-content">
                  <div className="timeline-summary">
                    <span style={{ marginRight: '8px', verticalAlign: 'middle' }}>{getIcon(summary, event.type)}</span>
                    {summary}
                  </div>
                  <div className="timeline-meta">
                    <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                      <User size={12} /> {(event.user || 'system@google.com').split('@')[0]}
                    </span>
                    <div>{formatDate(event.timestamp)}</div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </aside>

      {/* Main Diff View */}
      <main className="main-view">
        <div className="diff-card">
          <header className="diff-header">
            <div>
              <h1 style={{ margin: 0, fontSize: '1.8rem' }}>Evolution: customers</h1>
              <p style={{ color: '#94a3b8', margin: '4px 0' }}>
                {getChangeSummary(currentEvent, previousEvent)} • {formatDate(currentEvent.timestamp)}
              </p>
            </div>
            <div className="badge badge-added">Version {selectedIndex + 1}.0</div>
          </header>

          <div className="diff-grid">
            {/* Previous State */}
            <div className="panel">
              <h3 style={{ marginTop: 0, fontSize: '0.9rem', color: '#94a3b8' }}>PREVIOUS</h3>
              {previousEvent ? (
                <ul className="field-list">
                  {previousEvent.snapshot.fields.map((f: any) => (
                    <li key={f.name} className="field-item">
                      <span>{f.name}</span>
                      <span style={{ color: '#60a5fa' }}>{f.type}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <div style={{ padding: '40px', textAlign: 'center', color: '#475569' }}>Genesis Block</div>
              )}
            </div>

            {/* Current State */}
            <div className="panel">
              <h3 style={{ marginTop: 0, fontSize: '0.9rem', color: '#94a3b8' }}>CURRENT</h3>
              <ul className="field-list">
                {currentEvent.snapshot.fields.map((f: any) => {
                  const prevF = previousEvent?.snapshot.fields.find((pf: any) => pf.name === f.name);
                  const isNew = previousEvent && !prevF;
                  
                  const curTags = currentEvent.snapshot.column_aspects?.[f.name] || [];
                  const prevTags = previousEvent?.snapshot.column_aspects?.[f.name] || [];
                  const tagsChanged = JSON.stringify(curTags.sort()) !== JSON.stringify(prevTags.sort());
                  const typeChanged = prevF && prevF.dataType !== f.dataType;
                  const isModified = tagsChanged || typeChanged;

                  return (
                    <li key={f.name} className={`field-item ${isNew ? 'added' : ''} ${isModified ? 'modified' : ''}`}>
                      <div style={{ display: 'flex', flexDirection: 'column', width: '100%' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <span>
                            {f.name} 
                            {isNew && <span className="badge badge-added" style={{ marginLeft: '8px' }}>NEW</span>}
                            {isModified && <span className="badge" style={{ marginLeft: '8px', background: '#3b82f6', color: 'white' }}>MODIFIED</span>}
                          </span>
                          <span style={{ color: typeChanged ? '#fbbf24' : '#60a5fa' }}>{f.dataType || f.type}</span>
                        </div>
                        {curTags.length > 0 && (
                          <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                            {curTags.map((tag: string) => {
                              const isNewTag = !prevTags.includes(tag);
                              return (
                                <span key={tag} className="badge" style={{ 
                                  fontSize: '0.65rem', 
                                  background: isNewTag ? '#1e40af' : '#334155', 
                                  border: isNewTag ? '1px solid #60a5fa' : 'none',
                                  padding: '2px 6px' 
                                }}>
                                  <Tag size={8} style={{ marginRight: '4px' }} /> {tag}
                                </span>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    </li>
                  );
                })}
              </ul>
            </div>
          </div>

          {/* Aspects Section */}
          <div className="aspect-section">
            <h3 style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <ShieldCheck size={20} color="#a855f7" /> Governance Aspects
            </h3>
            <div className="aspect-grid">
              {Object.entries(currentEvent.snapshot.aspects).map(([key, value]: [string, any]) => (
                <div key={key} className="aspect-card glass">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
                    <ShieldCheck size={16} color="#a855f7" />
                    <span style={{ fontWeight: 700, fontSize: '0.8rem', color: '#a855f7' }}>
                      {key.split('.').pop()?.toUpperCase()}
                    </span>
                  </div>
                  <div style={{ display: 'grid', gap: '8px' }}>
                    {Object.entries(value).map(([fKey, fVal]: [string, any]) => {
                      const prevVal = previousEvent?.snapshot.aspects[key]?.[fKey];
                      const isModified = previousEvent && prevVal !== undefined && prevVal !== fVal;
                      return (
                        <div key={fKey} className={isModified ? 'aspect-field-modified' : ''} style={{ fontSize: '0.85rem', display: 'flex', justifyContent: 'space-between', padding: isModified ? '2px 4px' : '0', borderRadius: '4px' }}>
                          <span style={{ color: '#94a3b8' }}>{fKey}:</span>
                          <span style={{ fontWeight: 500, color: isModified ? '#fbbf24' : 'inherit' }}>
                            {String(fVal)}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}
              {Object.keys(currentEvent.snapshot.aspects).length === 0 && (
                <div style={{ color: '#475569', fontSize: '0.9rem', padding: '20px' }}>No domain-level governance aspects associated at this point.</div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
};

export default MetadataEvolution;
