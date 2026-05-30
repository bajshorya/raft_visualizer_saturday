import type { NodeState } from "../types/raft";

// ── Role colour tokens ────────────────────────────────────────────────────────

const ROLE = {
  Follower:  { ring: "ring-slate-600",   bg: "bg-slate-800",   dot: "bg-slate-500",   label: "FOLLOWER",  text: "text-slate-400"  },
  Candidate: { ring: "ring-amber-500",   bg: "bg-amber-950",   dot: "bg-amber-400",   label: "CANDIDATE", text: "text-amber-300"  },
  Leader:    { ring: "ring-emerald-500", bg: "bg-emerald-950", dot: "bg-emerald-400", label: "LEADER",    text: "text-emerald-300" },
};

interface Props {
  node: NodeState;
  crashed: boolean;
  onCrash: () => void;
  onRestart: () => void;
}

export default function NodeCard({ node, crashed, onCrash, onRestart }: Props) {
  const r = ROLE[node.role];
  const visible = node.log.slice(-6);

  return (
    <div className={`flex flex-col gap-3 rounded-xl p-4 ${r.bg} ring-2 ${r.ring} transition-all duration-300 ${
      crashed ? "opacity-40 grayscale" : ""
    }`}>

      {/* ── Header ──────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-2">
        {/* Role indicator dot */}
        <div className={`h-3 w-3 rounded-full ${r.dot} ${crashed ? "" : "animate-pulse"}`} />
        <span className="text-lg font-bold text-white">Node {node.id}</span>
        {crashed && <span className="text-xs text-red-500 font-semibold">CRASHED</span>}
        <span className={`ml-auto text-xs font-semibold tracking-widest ${r.text}`}>
          {r.label}
        </span>
      </div>

      {/* ── Crash/Restart button ────────────────────────────────────────── */}
      <button
        onClick={crashed ? onRestart : onCrash}
        className={`w-full rounded px-2 py-1 text-xs font-semibold transition ${
          crashed
            ? "bg-emerald-700 hover:bg-emerald-600 text-white"
            : "bg-red-700 hover:bg-red-600 text-white"
        }`}
      >
        {crashed ? "🔄 Restart" : "💥 Crash"}
      </button>

      {/* ── Stats row ───────────────────────────────────────────────────── */}
      <div className="grid grid-cols-3 gap-1 text-center text-xs">
        <Stat label="Term"    value={node.term}                       />
        <Stat label="Commit"  value={node.commitIndex}                />
        <Stat label="VotedFor" value={node.votedFor ?? "—"}           />
      </div>

      {/* ── Log entries ─────────────────────────────────────────────────── */}
      <div className="min-h-[5rem] space-y-1">
        {node.log.length === 0 ? (
          <p className="text-center text-xs text-slate-600 py-3">empty log</p>
        ) : (
          visible.map((entry) => {
            const committed = entry.index <= node.commitIndex;
            return (
              <div
                key={entry.index}
                className={`flex items-center gap-2 rounded px-2 py-1 text-xs font-mono ${
                  committed
                    ? "bg-emerald-900/50 text-emerald-300"
                    : "bg-amber-900/30 text-amber-400"
                }`}
              >
                <span className="shrink-0 text-slate-500">
                  [{entry.index}·t{entry.term}]
                </span>
                <span className="truncate">{entry.command}</span>
                <span className="ml-auto shrink-0">
                  {committed ? "✓" : "⏳"}
                </span>
              </div>
            );
          })
        )}
        {node.log.length > 6 && (
          <p className="text-center text-xs text-slate-600">
            +{node.log.length - 6} earlier
          </p>
        )}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded bg-slate-900/60 px-2 py-1">
      <div className="text-slate-500">{label}</div>
      <div className="font-semibold text-white">{value}</div>
    </div>
  );
}
