import React, { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { io } from 'socket.io-client';
import './style.css';

const endpoint = import.meta.env.VITE_API_URL || 'http://localhost:3000';
const names = ['APPROVED', 'REVIEW', 'BLOCKED'];

function App() {
  const [items, setItems] = useState([]);
  const [counts, setCounts] = useState({ APPROVED: 0, REVIEW: 0, BLOCKED: 0 });
  const [connected, setConnected] = useState(false);
  const [selected, setSelected] = useState('ALL');
  useEffect(() => {
    fetch(endpoint + '/api/recent').then(r => r.json()).then(data => {
      setItems(data.items || []); setCounts(data.counts || { APPROVED: 0, REVIEW: 0, BLOCKED: 0 });
    }).catch(() => {});
    const socket = io(endpoint, { transports: ['websocket', 'polling'] });
    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));
    socket.on('fraud:decision', decision => {
      setItems(before => [decision, ...before.filter(row => row.eventId !== decision.eventId)].slice(0, 100));
      setCounts(before => ({ ...before, [decision.decision]: (before[decision.decision] || 0) + 1 }));
    });
    return () => socket.disconnect();
  }, []);
  const total = names.reduce((sum, name) => sum + (counts[name] || 0), 0);
  const filtered = selected === 'ALL' ? items : items.filter(row => row.decision === selected);
  return <main>
    <header className="topbar"><div className="brand"><span className="mark">S</span><span>SIGNAL <small>FRAUD OPERATIONS</small></span></div>
      <div className={'connection ' + (connected ? 'online' : '')}><span className="dot"/>{connected ? 'LIVE STREAM' : 'RECONNECTING'}</div></header>
    <section className="hero"><div><div className="eyebrow">PAYMENT RISK · SIMULATED DATA</div>
      <h1>Transaction decisions<br/><em>as they happen.</em></h1>
      <p>Watch scored events move through the streaming pipeline. Values shown here come from synthetic card tokens.</p></div>
      <div className="hero-number"><strong>{total.toLocaleString()}</strong><span>decisions received in this dashboard session</span></div></section>
    <section className="cards">{names.map(name => <button key={name} onClick={() => setSelected(selected === name ? 'ALL' : name)}
      className={'card ' + name.toLowerCase() + (selected === name ? ' chosen' : '')}>
      <span className="label">{name}</span><strong>{(counts[name] || 0).toLocaleString()}</strong>
      <span className="share">{total ? ((counts[name] || 0) / total * 100).toFixed(1) : '0.0'}% of decisions</span>
      <div className="track"><span style={{ width: (total ? (counts[name] || 0) / total * 100 : 0) + '%' }}/></div>
    </button>)}</section>
    <section className="feed"><div className="feed-header"><div><div className="eyebrow">EVENT MONITOR</div><h2>Decision feed</h2></div>
      <button className="filter" onClick={() => setSelected('ALL')}>{selected === 'ALL' ? 'Showing all' : selected + ' · Clear filter'}</button></div>
      <div className="table-wrap"><table><thead><tr><th>EVENT / CARD</th><th>DECISION</th><th>RISK SCORE</th><th>BEHAVIOR</th><th>PIPELINE TIME</th></tr></thead>
      <tbody>{filtered.map(row => <tr key={row.eventId}><td><b>{row.cardId}</b><small>{row.eventId.slice(0, 13)}…</small></td>
        <td><span className={'pill ' + row.decision.toLowerCase()}>{row.decision}</span>{row.circuitBreaker && <small className="circuit">Circuit triggered</small>}</td>
        <td className="mono">{(row.probability * 100).toFixed(1)}%</td>
        <td><b>{row.velocity5m} tx / 5 min</b><small>{row.distanceKm} km since previous</small></td>
        <td className="mono">{Math.max(0, row.processedAt - row.ingestedAt).toLocaleString()} ms</td></tr>)}
      {!filtered.length && <tr><td colSpan="5" className="empty">No decisions yet. Send simulated events with the demo script to populate this feed.</td></tr>}</tbody></table></div></section>
    <footer>Signal demo · Model scores are experimental · Pipeline time excludes browser delivery</footer>
  </main>;
}

createRoot(document.getElementById('root')).render(<App />);
