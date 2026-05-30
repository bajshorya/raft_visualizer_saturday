import type { TimestampedEvent } from "../types/raft";

interface Props {
  events: TimestampedEvent[];
}

const EVENT_META = {
  role_change:    { prefix: "STATE", color: "text-blue-400",    symbol: "→" },
  log_append:     { prefix: "LOG",   color: "text-purple-400",  symbol: "+" },
  commit:         { prefix: "COMMIT", color: "text-cyan-400",   symbol: "✓" },
  vote_cast:      { prefix: "VOTE",  color: "text-amber-400",   symbol: "◆" },
  vote_received:  { prefix: "VOTE",  color: "text-yellow-400",  symbol: "◇" },
  heartbeat:      { prefix: "HB",    color: "text-emerald-400", symbol: "♥" },
  election_start: { prefix: "ELECT", color: "text-orange-400",  symbol: "⚡" },
  message_sent:   { prefix: "MSG",   color: "text-slate-500",   symbol: "→" },
};

export default function EventFeed({ events }: Props) {
  return (
    <div className="flex flex-col h-full bg-black/40 backdrop-blur-sm border border-slate-800">

      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-2 border-b border-slate-800 font-mono">
        <div className="flex items-center gap-2">
          <div className="h-2 w-2 bg-cyan-500 animate-pulse" />
          <span className="text-xs font-bold tracking-widest text-slate-400">
            EVENT STREAM
          </span>
        </div>
        <span className="ml-auto text-[10px] text-slate-600 tabular-nums">
          [{events.length.toString().padStart(3, "0")}]
        </span>
      </div>

      {/* Event list */}
      <div className="flex-1 overflow-y-auto px-3 py-2 space-y-1">
        {events.length === 0 ? (
          <div className="text-center text-[10px] text-slate-700 py-8 border border-dashed border-slate-800">
            WAITING FOR EVENTS...
          </div>
        ) : (
          events.map((e, idx) => {
            const meta = EVENT_META[e.type as keyof typeof EVENT_META];
            if (!meta) return null;

            const time = new Date(e.ts).toLocaleTimeString("en-GB", {
              hour12: false,
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
            });

            return (
              <div
                key={idx}
                className="flex items-start gap-3 px-2 py-1 border-l-2 border-slate-800 hover:border-slate-700 hover:bg-slate-900/30 transition-colors font-mono"
              >
                <span className="text-[9px] text-slate-600 tabular-nums shrink-0 pt-0.5">
                  {time}
                </span>
                <span className={`text-[9px] font-bold ${meta.color} shrink-0 w-10 pt-0.5`}>
                  {meta.prefix}
                </span>
                <span className="text-[10px] text-slate-400 leading-relaxed">
                  {formatEvent(e)}
                </span>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

function formatEvent(e: TimestampedEvent): string {
  switch (e.type) {
    case "role_change":
      return `N${e.node_id} → ${e.role.toUpperCase()} [t${e.term}]`;
    case "log_append":
      return `N${e.node_id} append [${e.entry.index}:t${e.entry.term}] "${e.entry.command}"`;
    case "commit":
      return `N${e.node_id} commit idx=${e.commit_index}`;
    case "vote_cast":
      return `N${e.node_id} → N${e.voted_for} [t${e.term}]`;
    case "vote_received":
      return `N${e.node_id} ← N${e.from} ${e.granted ? "GRANT" : "DENY"}`;
    case "heartbeat":
      return `N${e.leader_id} → broadcast [t${e.term}]`;
    case "election_start":
      return `N${e.node_id} start election [t${e.term}]`;
    case "message_sent":
      return `N${e.from} → N${e.to} ${e.message_type.replace(/_/g, " ").toUpperCase()}`;
    default:
      return JSON.stringify(e);
  }
}
