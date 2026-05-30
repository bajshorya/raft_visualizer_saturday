import type { TimestampedEvent } from "../types/raft";

// ── Event type → colour / summary ────────────────────────────────────────────

function summary(e: TimestampedEvent): string {
  switch (e.type) {
    case "role_change":
      return `Node ${e.node_id} → ${e.role}  (term ${e.term})`;
    case "election_start":
      return `Node ${e.node_id} started election  (term ${e.term})`;
    case "vote_cast":
      return `Node ${e.node_id} voted for Node ${e.voted_for}  (term ${e.term})`;
    case "vote_received":
      return `Node ${e.node_id} ← ${e.granted ? "✓ granted" : "✗ denied"}  from Node ${e.from}`;
    case "log_append":
      return `Node ${e.node_id} appended [${e.entry.index}] "${e.entry.command}"`;
    case "commit":
      return `Node ${e.node_id} committed up to index ${e.commit_index}`;
    case "heartbeat":
      return `Node ${e.leader_id} heartbeat  (term ${e.term})`;
  }
}

const TYPE_COLOR: Record<string, string> = {
  role_change:    "text-purple-400",
  election_start: "text-amber-400",
  vote_cast:      "text-blue-400",
  vote_received:  "text-indigo-400",
  log_append:     "text-cyan-400",
  commit:         "text-emerald-400",
  heartbeat:      "text-slate-500",
};

interface Props {
  events: TimestampedEvent[];
}

export default function EventFeed({ events }: Props) {
  return (
    <div className="flex flex-col h-full overflow-hidden rounded-xl bg-slate-800 ring-2 ring-slate-700">
      <div className="border-b border-slate-700 px-4 py-2 text-xs font-semibold uppercase tracking-widest text-slate-400">
        Live Events
      </div>
      <div className="flex-1 overflow-y-auto px-3 py-2 space-y-0.5 font-mono text-xs">
        {events.length === 0 && (
          <p className="text-slate-600 pt-4 text-center">waiting for events…</p>
        )}
        {events.map((e, i) => (
          <div key={i} className="flex items-baseline gap-2">
            <span className="shrink-0 text-slate-600">
              {new Date(e.ts).toLocaleTimeString("en", { hour12: false })}
            </span>
            <span className={`shrink-0 w-24 ${TYPE_COLOR[e.type] ?? "text-slate-400"}`}>
              {e.type}
            </span>
            <span className="text-slate-300 truncate">{summary(e)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
