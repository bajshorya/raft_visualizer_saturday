// ── state.rs — core data structures ──────────────────────────────────────────
//
// Pure data — no async, no networking, no Tokio-specific logic.
// Everything here is what gets persisted, replicated, and reasoned about.

use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use crate::message::Message;

// Newtype over u64 so node IDs can never be confused with raw numbers, log
// indices, or terms at compile time.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId(pub u64);

// Each node is always in exactly one role.  Leader-only state (next_index /
// match_index) lives *inside* the Leader variant — the compiler makes it
// impossible to access those fields unless the node is actually a Leader.
#[derive(Debug)]
pub enum Role {
    Follower,

    Candidate {
        // Peers that have granted us a vote this election.
        // Seeded with self.id on election start (we always vote for ourselves).
        votes_received: HashSet<NodeId>,
    },

    Leader {
        // next_index[peer]: the log index we plan to send next to that peer.
        // Starts optimistically at last_log_index + 1 on election win.
        next_index:  HashMap<NodeId, usize>,

        // match_index[peer]: highest log index confirmed replicated on that peer.
        // Starts at 0 (nothing confirmed). Advanced by successful AppendEntries responses.
        match_index: HashMap<NodeId, usize>,
    },
}

// One slot in the replicated log.
// log[0] is always a sentinel (term=0, empty command) so that prev_log_index=0
// is always a valid anchor — avoids off-by-one special casing everywhere.
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub index:   usize,
    pub term:    u64,
    pub command: String,
}

// The complete state of one Raft node.
#[derive(Debug)]
pub struct RaftNode {
    // ── Identity ─────────────────────────────────────────────────────────────
    pub id:    NodeId,
    pub peers: Vec<NodeId>,

    // ── Role (carries leader-only state inside the Leader variant) ────────────
    pub role: Role,

    // ── Persistent state ──────────────────────────────────────────────────────
    // Must be written to stable storage before replying to any RPC.
    // Losing these after a crash violates Raft's safety guarantees.
    pub current_term: u64,
    pub voted_for:    Option<NodeId>,
    pub log:          Vec<LogEntry>,

    // ── Volatile state ────────────────────────────────────────────────────────
    // Safe to reconstruct from scratch after a crash.
    pub commit_index: usize, // highest log index known to be committed
    pub last_applied: usize, // highest log index applied to the state machine

    // Receive end of this node's inbox channel.
    // The async event loop reads from here; handlers never touch it directly.
    pub inbox: mpsc::Receiver<Message>,
}

impl RaftNode {
    /// Reset to initial state as if the node just crashed and restarted.
    /// Simulates losing all volatile state (commit_index, role) while keeping
    /// the sentinel log[0] that ensures prev_log_index=0 works.
    /// In a real system, persistent state (term, voted_for, log) would be
    /// restored from disk, but for this demo we reset everything.
    pub fn reset_to_initial(&mut self) {
        self.role = Role::Follower;
        self.current_term = 0;
        self.voted_for = None;
        self.commit_index = 0;
        self.last_applied = 0;
        // Keep only the sentinel log[0]
        self.log.truncate(1);
    }
}
