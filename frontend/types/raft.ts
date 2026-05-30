// types/raft.ts — TypeScript mirror of the Rust StateEvent enum and data structures.
// Field names match the Rust serde output exactly (snake_case).

export type Role = "Follower" | "Candidate" | "Leader";

export interface LogEntry {
  index:   number;
  term:    number;
  command: string;
}

// One node's observable state — built up from the stream of StateEvents.
export interface NodeState {
  id:          number;
  role:        Role;
  term:        number;
  commitIndex: number;
  log:         LogEntry[];
  votedFor:    number | null;
}

export type ClusterState = Record<number, NodeState>;

// ── StateEvent variants — must match events.rs serde output ──────────────────

export type MessageType = "request_vote" | "request_vote_response" | "append_entries" | "append_entries_response";

export type StateEvent =
  | { type: "role_change";    node_id: number; role: Role;   term: number }
  | { type: "log_append";     node_id: number; entry: LogEntry }
  | { type: "commit";         node_id: number; commit_index: number }
  | { type: "vote_cast";      node_id: number; voted_for: number; term: number }
  | { type: "vote_received";  node_id: number; from: number; granted: boolean }
  | { type: "heartbeat";      leader_id: number; term: number }
  | { type: "election_start"; node_id: number; term: number }
  | { type: "message_sent";   from: number; to: number; message_type: MessageType };

// Each event as it appears in the live event feed (with a client-side timestamp).
export type TimestampedEvent = StateEvent & { ts: number };

// Active message arrow for visualization (auto-expires after 1s)
export interface MessageArrow {
  id:           string;
  from:         number;
  to:           number;
  message_type: MessageType;
  createdAt:    number;
}
