"use client";

import { useState } from "react";
import { useRaftCluster } from "../hooks/useRaftCluster";
import NodeCard from "../components/NodeCard";
import EventFeed from "../components/EventFeed";

const NODE_IDS = [1, 2, 3, 4, 5];

export default function Home() {
  const { cluster, events, connected, sendCommand, crashNode, restartNode } = useRaftCluster();
  const [cmd, setCmd]         = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError]     = useState<string | null>(null);
  const [crashed, setCrashed] = useState<Set<number>>(new Set());

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = cmd.trim();
    if (!trimmed || !connected) return;
    setSending(true);
    setError(null);
    try {
      await sendCommand(trimmed);
      setCmd("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to send command");
      console.error("Command failed:", err);
    } finally {
      setSending(false);
    }
  }

  return (
    <div className="flex h-screen flex-col gap-4 p-4 overflow-hidden">

      {/* ── Header ──────────────────────────────────────────────────────── */}
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold tracking-tight text-white">
            Raft Consensus Visualizer
          </h1>
          <p className="text-xs text-slate-500">
            Real implementation · 5 nodes · in-process mpsc channels
          </p>
        </div>

        {/* Connection badge */}
        <div className={`flex items-center gap-2 rounded-full px-3 py-1 text-sm font-medium ring-1 ${
          connected
            ? "bg-emerald-950 ring-emerald-600 text-emerald-400"
            : "bg-red-950 ring-red-700 text-red-400"
        }`}>
          <span className={`h-2 w-2 rounded-full ${connected ? "bg-emerald-400 animate-pulse" : "bg-red-500"}`} />
          {connected ? "Connected" : "Disconnected — retrying…"}
        </div>
      </header>

      {/* ── Node grid ───────────────────────────────────────────────────── */}
      <div className="grid grid-cols-5 gap-3 flex-shrink-0">
        {NODE_IDS.map((id) => (
          <NodeCard
            key={id}
            node={cluster[id]}
            crashed={crashed.has(id)}
            onCrash={async () => {
              await crashNode(id);
              setCrashed((s) => new Set(s).add(id));
            }}
            onRestart={async () => {
              await restartNode(id);
              setCrashed((s) => { const n = new Set(s); n.delete(id); return n; });
            }}
          />
        ))}
      </div>

      {/* ── Command input ───────────────────────────────────────────────── */}
      <div className="flex-shrink-0 space-y-2">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            value={cmd}
            onChange={(e) => setCmd(e.target.value)}
            placeholder='e.g.  set x 42  or  del y'
            disabled={!connected || sending}
            className="flex-1 rounded-lg bg-slate-800 px-4 py-2 text-sm text-white
                       placeholder-slate-600 ring-1 ring-slate-700 outline-none
                       focus:ring-emerald-500 disabled:opacity-40 transition"
          />
          <button
            type="submit"
            disabled={!connected || sending || !cmd.trim()}
            className="rounded-lg bg-emerald-700 px-5 py-2 text-sm font-semibold
                       text-white hover:bg-emerald-600 disabled:opacity-40 transition"
          >
            {sending ? "Sending…" : "Send Command"}
          </button>
        </form>
        {error && (
          <div className="flex items-center gap-2 rounded-lg bg-red-950 px-4 py-2 text-sm text-red-400 ring-1 ring-red-800">
            <span>⚠️</span>
            <span>{error}</span>
            <button
              onClick={() => setError(null)}
              className="ml-auto text-red-600 hover:text-red-400"
            >
              ✕
            </button>
          </div>
        )}
      </div>

      {/* ── Event feed ──────────────────────────────────────────────────── */}
      <div className="flex-1 min-h-0">
        <EventFeed events={events} />
      </div>

    </div>
  );
}
