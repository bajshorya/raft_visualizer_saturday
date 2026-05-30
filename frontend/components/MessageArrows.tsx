import type { MessageArrow, MessageType } from "../types/raft";

interface Props {
  arrows: MessageArrow[];
}

// Node center positions (x%, y%) - 5 nodes in a row
const NODE_POSITIONS: Record<number, { x: number; y: number }> = {
  1: { x: 10,  y: 50 },
  2: { x: 30,  y: 50 },
  3: { x: 50,  y: 50 },
  4: { x: 70,  y: 50 },
  5: { x: 90,  y: 50 },
};

// Cyan theme for Raft messages
const MESSAGE_STYLES: Record<MessageType, { color: string; dash: string }> = {
  request_vote:              { color: "#fbbf24", dash: "4,2" },    // amber, dashed
  request_vote_response:     { color: "#fcd34d", dash: "2,2" },    // lighter amber, dotted
  append_entries:            { color: "#22d3ee", dash: "0" },       // cyan, solid
  append_entries_response:   { color: "#67e8f9", dash: "2,2" },    // lighter cyan, dotted
};

export default function MessageArrows({ arrows }: Props) {
  return (
    <svg
      className="absolute inset-0 pointer-events-none z-10"
      style={{ width: "100%", height: "100%" }}
    >
      <defs>
        {/* Glowing arrowhead markers */}
        {Object.entries(MESSAGE_STYLES).map(([type, style]) => (
          <marker
            key={type}
            id={`arrow-${type}`}
            viewBox="0 0 10 10"
            refX="8"
            refY="5"
            markerWidth="5"
            markerHeight="5"
            orient="auto-start-reverse"
          >
            <path
              d="M 0 0 L 10 5 L 0 10 z"
              fill={style.color}
              opacity="0.9"
            />
          </marker>
        ))}

        {/* Glow filter */}
        <filter id="glow">
          <feGaussianBlur stdDeviation="1.5" result="coloredBlur"/>
          <feMerge>
            <feMergeNode in="coloredBlur"/>
            <feMergeNode in="SourceGraphic"/>
          </feMerge>
        </filter>
      </defs>

      {arrows.map((arrow) => {
        const from = NODE_POSITIONS[arrow.from];
        const to = NODE_POSITIONS[arrow.to];
        if (!from || !to) return null;

        const style = MESSAGE_STYLES[arrow.message_type];
        const age = Date.now() - arrow.createdAt;
        const opacity = Math.max(0, 1 - age / 1000);

        return (
          <line
            key={arrow.id}
            x1={`${from.x}%`}
            y1={`${from.y}%`}
            x2={`${to.x}%`}
            y2={`${to.y}%`}
            stroke={style.color}
            strokeWidth="1.5"
            strokeOpacity={opacity * 0.7}
            strokeDasharray={style.dash}
            markerEnd={`url(#arrow-${arrow.message_type})`}
            filter="url(#glow)"
            className="transition-opacity duration-300"
          />
        );
      })}
    </svg>
  );
}
