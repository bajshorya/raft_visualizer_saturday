"use client";

import { useState } from "react";
import { useRaftCluster } from "../hooks/useRaftCluster";
import NodeCard from "../components/NodeCard";
import EventFeed from "../components/EventFeed";
import MessageArrows from "../components/MessageArrows";

const NODE_IDS = [1, 2, 3, 4, 5];

export default function Home() {
  const { cluster, events, arrows, connected, sendCommand, crashNode, restartNode } = useRaftCluster();
  const [cmd, setCmd] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
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
    <div className="flex h-screen flex-col gap-4 p-6 overflow-hidden bg-gradient-to-br from-slate-950 via-slate-900 to-black font-mono">

      {/* Header */}
      <header className="flex items-center justify-between border-b border-slate-800 pb-4">
        <div>
          <div className="flex items-center gap-3">
            <div className="h-3 w-3 bg-cyan-500 animate-pulse" />
            <h1 className="text-2xl font-bold tracking-wider text-white">
              RAFT<span className="text-cyan-400">.CLUSTER</span>
            </h1>
          </div>
          <p className="text-[10px] text-slate-600 tracking-wider mt-1 ml-6">
            CONSENSUS ALGORITHM VISUALIZER · RUST + TOKIO · 5 NODES
          </p>
        </div>

        {/* Connection status */}
        <div className={`
          flex items-center gap-2 px-4 py-2 text-xs font-bold tracking-widest
          border transition-all
          ${connected
            ? "border-cyan-700/50 bg-cyan-950/30 text-cyan-400"
            : "border-red-700/50 bg-red-950/30 text-red-400"
          }
        `}>
          <div className={`h-2 w-2 ${connected ? "bg-cyan-400 animate-pulse" : "bg-red-500"}`} />
          {connected ? "ONLINE" : "OFFLINE"}
        </div>
      </header>

      {/* Node grid with message arrows */}
      <div className="relative grid grid-cols-5 gap-4 flex-shrink-0">
        <MessageArrows arrows={arrows} />
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

      {/* Command input */}
      <div className="flex-shrink-0 space-y-2">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            value={cmd}
            onChange={(e) => setCmd(e.target.value)}
            placeholder='> SET key value | DEL key'
            disabled={!connected || sending}
            className="flex-1 px-4 py-2 text-sm text-white
                       bg-black/40 border border-slate-800
                       placeholder-slate-700 outline-none
                       focus:border-cyan-700 disabled:opacity-30 transition
                       font-mono"
          />
          <button
            type="submit"
            disabled={!connected || sending || !cmd.trim()}
            className="px-6 py-2 text-xs font-bold tracking-widest
                       border border-cyan-700/50 bg-cyan-950/30 text-cyan-400
                       hover:bg-cyan-900/50 hover:shadow-cyan-500/20
                       disabled:opacity-30 transition"
          >
            {sending ? "SENDING..." : "EXECUTE"}
          </button>
        </form>
        {error && (
          <div className="flex items-center gap-3 px-4 py-2 text-xs
                          border border-red-700/50 bg-red-950/30 text-red-400">
            <span className="font-bold">ERROR:</span>
            <span className="flex-1">{error}</span>
            <button
              onClick={() => setError(null)}
              className="text-red-600 hover:text-red-400 font-bold"
            >
              ✕
            </button>
          </div>
        )}
      </div>

      {/* Event feed */}
      <div className="flex-1 min-h-0">
        <EventFeed events={events} />
      </div>

    </div>
  );
}
