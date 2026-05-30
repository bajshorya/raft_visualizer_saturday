// ── message.rs — all message and response types ───────────────────────────────
//
// Everything that can travel over the wire or sit in a node's inbox.
// No Tokio, no async — pure data.

use crate::state::{NodeId, LogEntry};

// ── Inbox messages ────────────────────────────────────────────────────────────

// Every type of message a node can receive in its inbox channel.
// The event loop matches on this and routes to the appropriate handler.
#[derive(Debug)]
pub enum Message {
    Rpc           { from: NodeId, payload: RpcMessage },
    RpcResponse   (RpcResponse),
    Timeout       (TimeoutKind),
    ClientCommand (String),
    Restart,      // Internal: reset state to initial Follower
}

// ── RPC payloads (what we receive) ───────────────────────────────────────────

#[derive(Debug)]
pub enum RpcMessage {
    // §5.2 — sent by a Candidate to every peer asking for a vote.
    RequestVote {
        term:           u64,
        candidate_id:   NodeId,
        last_log_index: usize,  // candidate's last log entry index
        last_log_term:  u64,    // candidate's last log entry term
    },

    // §5.3 — sent by the Leader to replicate entries; also used as heartbeat
    // (entries = []) to prevent followers from timing out.
    AppendEntries {
        term:           u64,
        leader_id:      NodeId,
        prev_log_index: usize,       // index of entry immediately before new ones
        prev_log_term:  u64,         // term of prev_log_index entry
        entries:        Vec<LogEntry>,
        leader_commit:  usize,       // leader's current commit_index
    },
}

// ── RPC responses (what we receive back) ─────────────────────────────────────

#[derive(Debug)]
pub enum RpcResponse {
    RequestVoteResponse {
        from:        NodeId,
        term:        u64,
        vote_granted: bool,
    },

    AppendEntriesResponse {
        from:        NodeId,
        term:        u64,
        success:     bool,
        match_index: usize, // highest index now replicated on the follower (on success)
    },
}

// ── Timer signals ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TimeoutKind {
    // Fired when no heartbeat arrives within [150ms, 300ms] — start an election.
    Election,
    // Fired on the leader at ~50ms intervals — send heartbeats / replicate entries.
    Heartbeat,
}

// ── Outbound messages (what handlers return) ──────────────────────────────────

// Handlers never send on channels directly — they return a Vec<OutboundMsg>.
// The event loop dispatches these after every handle_message() call.
// This keeps handlers pure and trivially unit-testable.
#[derive(Debug)]
pub enum OutboundMsg {
    Rpc      { to: NodeId, payload: RpcMessage  }, // new outbound RPC
    Response { to: NodeId, payload: RpcResponse }, // reply to an incoming RPC
}
