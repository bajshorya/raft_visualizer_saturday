import type { NodeState } from "../types/raft";

const ROLE = {
  Follower:  {
    border: "border-slate-700/50",
    glow: "shadow-slate-500/20",
    dot: "bg-slate-500",
    label: "FOLLOWER",
    text: "text-slate-400",
    accent: "text-slate-300"
  },
  Candidate: {
    border: "border-amber-500/50",
    glow: "shadow-amber-500/30",
    dot: "bg-amber-400",
    label: "CANDIDATE",
    text: "text-amber-400",
    accent: "text-amber-300"
  },
  Leader: {
    border: "border-cyan-500/50",
    glow: "shadow-cyan-500/30",
    dot: "bg-cyan-400",
    label: "LEADER",
    text: "text-cyan-400",
    accent: "text-cyan-300"
  },
};

interface Props {
  node: NodeState;
  crashed: boolean;
  onCrash: () => void;
  onRestart: () => void;
}

export default function NodeCard({ node, crashed, onCrash, onRestart }: Props) {
  const r = ROLE[node.role];
  const visible = node.log.slice(-5);

  return (
    <div className={`
      relative flex flex-col gap-3 p-4
      bg-black/40 backdrop-blur-sm
      border ${r.border} shadow-lg ${r.glow}
      transition-all duration-300
      ${crashed ? "opacity-30 grayscale" : ""}
      font-mono
    `}>

      {/* Corner accents */}
      <div className="absolute top-0 left-0 w-2 h-2 border-t-2 border-l-2 border-current opacity-30" />
      <div className="absolute top-0 right-0 w-2 h-2 border-t-2 border-r-2 border-current opacity-30" />
      <div className="absolute bottom-0 left-0 w-2 h-2 border-b-2 border-l-2 border-current opacity-30" />
      <div className="absolute bottom-0 right-0 w-2 h-2 border-b-2 border-r-2 border-current opacity-30" />

      {/* Header */}
      <div className="flex items-center gap-3 pb-2 border-b border-slate-800">
        <div className={`relative h-2 w-2 ${r.dot} ${crashed ? "" : "animate-pulse"}`}>
          {!crashed && <div className={`absolute inset-0 ${r.dot} blur-sm opacity-70`} />}
        </div>
        <span className="text-sm font-bold tracking-wider text-white">
          NODE<span className={r.accent}>.{node.id}</span>
        </span>
        {crashed && (
          <span className="text-[10px] px-1.5 py-0.5 bg-red-900/50 border border-red-700/50 text-red-400">
            OFFLINE
          </span>
        )}
        <span className={`ml-auto text-[10px] font-bold tracking-widest ${r.text}`}>
          [{r.label}]
        </span>
      </div>

      {/* Control button */}
      <button
        onClick={crashed ? onRestart : onCrash}
        className={`
          w-full py-1.5 text-[10px] font-bold tracking-wider
          border transition-all
          ${crashed
            ? "border-cyan-700/50 bg-cyan-950/30 text-cyan-400 hover:bg-cyan-900/50 hover:shadow-cyan-500/20"
            : "border-red-700/50 bg-red-950/30 text-red-400 hover:bg-red-900/50 hover:shadow-red-500/20"
          }
        `}
      >
        {crashed ? "↻ RESTART" : "× CRASH"}
      </button>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-2 text-[10px]">
        <Stat label="TERM" value={node.term} accent={r.accent} />
        <Stat label="COMMIT" value={node.commitIndex} accent={r.accent} />
        <Stat label="VOTE" value={node.votedFor ?? "—"} accent={r.accent} />
      </div>

      {/* Log entries */}
      <div className="space-y-1 min-h-[80px]">
        <div className="text-[9px] text-slate-600 tracking-wider mb-1">LOG [{node.log.length}]</div>
        {node.log.length === 0 ? (
          <div className="text-[10px] text-slate-700 text-center py-4 border border-dashed border-slate-800">
            EMPTY
          </div>
        ) : (
          <>
            {visible.map((entry) => {
              const committed = entry.index <= node.commitIndex;
              return (
                <div
                  key={entry.index}
                  className={`
                    flex items-center gap-2 px-2 py-1 text-[10px]
                    border-l-2 transition-colors
                    ${committed
                      ? "border-cyan-500 bg-cyan-950/20 text-cyan-300"
                      : "border-amber-600 bg-amber-950/10 text-amber-400/70"
                    }
                  `}
                >
                  <span className="shrink-0 text-slate-600">
                    {entry.index}:t{entry.term}
                  </span>
                  <span className="truncate flex-1">{entry.command}</span>
                  <span className="shrink-0 text-[8px]">
                    {committed ? "✓" : "·"}
                  </span>
                </div>
              );
            })}
            {node.log.length > 5 && (
              <div className="text-[9px] text-slate-700 text-center">
                +{node.log.length - 5} more
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value, accent }: { label: string; value: string | number; accent: string }) {
  return (
    <div className="bg-black/30 border border-slate-800 px-2 py-1 text-center">
      <div className="text-slate-600 text-[8px] tracking-wider">{label}</div>
      <div className={`font-bold text-xs ${accent}`}>{value}</div>
    </div>
  );
}
