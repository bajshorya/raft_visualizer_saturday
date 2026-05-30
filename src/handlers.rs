// ── handlers.rs — RaftNode logic and tests ────────────────────────────────────
//
// All Raft state-machine logic lives here as methods on RaftNode.
// Every handler takes &mut RaftNode and returns Vec<OutboundMsg> — no I/O,
// no channel sends, no async.  The event loop (raft-node crate, future step)
// owns all I/O and dispatches whatever handlers return.

use std::collections::{HashMap, HashSet};
use crate::state::{LogEntry, NodeId, RaftNode, Role};
use crate::message::{Message, OutboundMsg, RpcMessage, RpcResponse, TimeoutKind};

impl RaftNode {
    // ── Helpers ───────────────────────────────────────────────────────────────

    pub fn last_log_index(&self) -> usize {
        // sentinel at [0] guarantees log.len() >= 1 always.
        self.log.len() - 1
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    // §5.4.1 — "at least as up-to-date" rule:
    // Compare last-entry terms first.  If equal, the longer log wins.
    // This check prevents a candidate with a stale log from winning an election.
    fn is_log_up_to_date(&self, candidate_last_index: usize, candidate_last_term: u64) -> bool {
        let my_term = self.last_log_term();
        if candidate_last_term != my_term {
            candidate_last_term > my_term
        } else {
            candidate_last_index >= self.last_log_index()
        }
    }

    // Majority = any vote count strictly greater than half the cluster size.
    // For 5 nodes: threshold = 2, so 3 votes (> 2) wins.
    fn majority_threshold(&self) -> usize {
        (self.peers.len() + 1) / 2
    }

    // §5.1 — highest-priority rule: any message with a higher term means we are
    // behind.  Immediately revert to Follower and clear the stale vote.
    // Called at the top of every handler before any other logic.
    fn step_down_if_stale(&mut self, incoming_term: u64) {
        if incoming_term > self.current_term {
            self.current_term = incoming_term;
            self.role = Role::Follower;
            self.voted_for = None;
        }
    }

    // Transition from Candidate to Leader.
    // next_index starts optimistically at last+1 for every peer — we back down
    // when a peer rejects AppendEntries.  match_index starts at 0.
    fn become_leader(&mut self) -> Vec<OutboundMsg> {
        let last = self.last_log_index();
        self.role = Role::Leader {
            next_index:  self.peers.iter().map(|&p| (p, last + 1)).collect(),
            match_index: self.peers.iter().map(|&p| (p, 0)).collect(),
        };
        // Assert leadership immediately so followers don't time out before the
        // first scheduled heartbeat fires.
        self.send_heartbeats()
    }

    // Build one AppendEntries RPC per peer, carrying any un-replicated entries.
    // entries=[] means pure heartbeat.  Takes &self — read-only.
    fn send_heartbeats(&self) -> Vec<OutboundMsg> {
        // let-else: destructure self.role as Leader and bind next_index.
        // If self.role is NOT a Leader the else block fires, returning early.
        // This lets the rest of the function safely assume we are a Leader and
        // access next_index without further guards.
        let Role::Leader { ref next_index, .. } = self.role else {
            return vec![];
        };

        // Clone next_index into an owned map so the closure below can freely
        // borrow self.log without keeping the Role borrow alive.
        // (Two simultaneous immutable borrows of different fields of self would
        // work here too, but cloning makes the intent explicit and avoids any
        // future borrow-checker surprises when the closure grows.)
        let next_index: HashMap<NodeId, usize> = next_index.clone();

        let term         = self.current_term;
        let leader_id    = self.id;
        let leader_commit = self.commit_index;

        self.peers
            .iter()
            .map(|&peer| {
                let next           = *next_index.get(&peer).unwrap_or(&1);
                let prev_log_index = next - 1;
                let prev_log_term  = self.log[prev_log_index].term;
                // Slice from next onward — empty when peer is fully caught up.
                let entries        = self.log[next..].to_vec();
                OutboundMsg::Rpc {
                    to: peer,
                    payload: RpcMessage::AppendEntries {
                        term, leader_id, prev_log_index, prev_log_term,
                        entries, leader_commit,
                    },
                }
            })
            .collect()
    }

    // §5.3 / §5.4 — after updating match_index, scan downward from the last log
    // entry for the highest N satisfying all three conditions:
    //   1. N > commit_index          (not already committed)
    //   2. majority of match_index >= N  (replicated on enough nodes)
    //   3. log[N].term == current_term   (Figure 8 safety — see RAFT_EXPLAINER.md)
    fn try_advance_commit_index(&mut self) {
        let last = self.last_log_index();

        // Snapshot match_index values to avoid a split borrow:
        // the for-loop below needs &self.log and self.commit_index simultaneously.
        let match_indices: Vec<usize> = match &self.role {
            Role::Leader { match_index, .. } => match_index.values().copied().collect(),
            _ => return,
        };

        for n in (self.commit_index + 1..=last).rev() {
            // Figure 8: never commit an entry from a prior term by counting replicas.
            if self.log[n].term != self.current_term {
                continue;
            }
            // Self always has the entry; count peers whose match_index >= n.
            let replicas = 1 + match_indices.iter().filter(|&&mi| mi >= n).count();
            if replicas > self.majority_threshold() {
                self.commit_index = n;
                break; // descending scan — first hit is the highest safe N
            }
        }
    }

    // ── Message router ────────────────────────────────────────────────────────

    // Entry point called by the event loop.  Zero Raft logic here — routing only.
    pub fn handle_message(&mut self, msg: Message) -> Vec<OutboundMsg> {
        match msg {
            Message::Rpc { from, payload }  => self.handle_rpc(from, payload),
            Message::RpcResponse(resp)      => self.handle_response(resp),
            Message::Timeout(kind)          => self.handle_timeout(kind),
            Message::ClientCommand(cmd)     => self.handle_command(cmd),
            Message::Restart                => vec![], // Handled by event loop, not Raft
        }
    }

    // ── Handler 1: incoming RPCs ──────────────────────────────────────────────

    fn handle_rpc(&mut self, from: NodeId, payload: RpcMessage) -> Vec<OutboundMsg> {
        match payload {

            // ── RequestVote ───────────────────────────────────────────────────
            // A Candidate is asking for our vote.  Grant only if:
            //   (a) we haven't voted for a different candidate this term, AND
            //   (b) the candidate's log is at least as up-to-date as ours.
            RpcMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                // §5.1: step down first so a higher-term request can win our vote
                // in the same call.
                self.step_down_if_stale(term);

                // Reject if the candidate is behind us.
                if term < self.current_term {
                    return vec![OutboundMsg::Response {
                        to: from,
                        payload: RpcResponse::RequestVoteResponse {
                            from: self.id,
                            term: self.current_term,
                            vote_granted: false,
                        },
                    }];
                }

                // voted_for == Some(candidate_id) makes retransmitted requests
                // idempotent — we re-grant if we already granted.
                let can_vote = self.voted_for.is_none()
                    || self.voted_for == Some(candidate_id);
                let log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
                let vote_granted = can_vote && log_ok;

                if vote_granted {
                    self.voted_for = Some(candidate_id);
                    // The event loop must reset the election timer here to avoid
                    // starting a competing election right after granting.
                }

                vec![OutboundMsg::Response {
                    to: from,
                    payload: RpcResponse::RequestVoteResponse {
                        from: self.id,
                        term: self.current_term,
                        vote_granted,
                    },
                }]
            }

            // ── AppendEntries ─────────────────────────────────────────────────
            // Leader is sending entries (or heartbeat).  Accept only if term is
            // current AND our log matches at prev_log_index.
            // Rejection tells the leader to back up and retry with an earlier prefix.
            RpcMessage::AppendEntries {
                term, leader_id: _, prev_log_index, prev_log_term, entries, leader_commit,
            } => {
                self.step_down_if_stale(term);

                // Reject stale leaders.
                if term < self.current_term {
                    return vec![OutboundMsg::Response {
                        to: from,
                        payload: RpcResponse::AppendEntriesResponse {
                            from: self.id,
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                        },
                    }];
                }

                // A valid same-term AppendEntries means a leader has won.
                // If we're a Candidate we lost — step down without clearing voted_for
                // (vote is already cast this term; that's correct).
                if matches!(self.role, Role::Candidate { .. }) {
                    self.role = Role::Follower;
                }
                // The event loop must also reset the election timer here.

                // §5.3 consistency check: our log must have an entry at prev_log_index
                // with term == prev_log_term.  The sentinel at [0] (term=0) ensures
                // prev_log_index=0 always passes — first AppendEntries needs no special case.
                let log_matches = self.log
                    .get(prev_log_index)
                    .map_or(false, |e| e.term == prev_log_term);

                if !log_matches {
                    return vec![OutboundMsg::Response {
                        to: from,
                        payload: RpcResponse::AppendEntriesResponse {
                            from: self.id,
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                        },
                    }];
                }

                // §5.3: truncate everything after prev_log_index (removes conflicts),
                // then extend with the new entries.  Idempotent: re-sending the same
                // entries after a crash yields the same log.
                self.log.truncate(prev_log_index + 1);
                self.log.extend(entries);

                let new_last_index = self.last_log_index();

                // Advance commit pointer if leader is ahead — clamped to what we
                // actually have (we may not have received all entries yet).
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(new_last_index);
                }

                vec![OutboundMsg::Response {
                    to: from,
                    payload: RpcResponse::AppendEntriesResponse {
                        from: self.id,
                        term: self.current_term,
                        success: true,
                        match_index: new_last_index,
                    },
                }]
            }
        }
    }

    // ── Handler 2: RPC responses ──────────────────────────────────────────────

    fn handle_response(&mut self, resp: RpcResponse) -> Vec<OutboundMsg> {
        match resp {

            // ── RequestVoteResponse ───────────────────────────────────────────
            // A peer is replying to our RequestVote.  Collect votes; win on majority.
            RpcResponse::RequestVoteResponse { from, term, vote_granted } => {
                self.step_down_if_stale(term);

                // Stale or irrelevant — no longer a Candidate, or vote was denied.
                if !vote_granted || !matches!(self.role, Role::Candidate { .. }) {
                    return vec![];
                }

                let threshold = self.majority_threshold();

                // Insert vote and check count inside an explicit scope so the
                // mutable borrow on self.role ends before we call become_leader().
                let won = if let Role::Candidate { ref mut votes_received } = self.role {
                    votes_received.insert(from);
                    // votes_received already contains self.id (seeded on election start),
                    // so len() is the true total vote count including our own.
                    votes_received.len() > threshold
                } else {
                    false
                }; // ← mutable borrow of self.role ends here

                if won {
                    return self.become_leader();
                }
                vec![]
            }

            // ── AppendEntriesResponse ─────────────────────────────────────────
            // A follower confirmed (or rejected) our AppendEntries.
            // Success → update tracking, try to advance commit_index.
            // Failure → back next_index down by 1, retry with longer prefix next heartbeat.
            RpcResponse::AppendEntriesResponse {
                from, term, success,
                match_index: follower_match_index, // renamed to avoid clash with Role field
            } => {
                self.step_down_if_stale(term);

                if success {
                    if let Role::Leader { ref mut next_index, ref mut match_index } = self.role {
                        match_index.insert(from, follower_match_index);
                        next_index.insert(from, follower_match_index + 1);
                    }
                    // Borrow above must end before try_advance_commit_index takes &mut self.
                    self.try_advance_commit_index();
                } else {
                    // Back up by one and retry.  (Optimisation: jump to conflicting
                    // term directly using hints in the response — §5.3, future work.)
                    if let Role::Leader { ref mut next_index, .. } = self.role {
                        let ni = next_index.entry(from).or_insert(1);
                        if *ni > 1 { *ni -= 1; }
                    }
                }
                vec![]
            }
        }
    }

    // ── Handler 3: timer events ───────────────────────────────────────────────

    fn handle_timeout(&mut self, kind: TimeoutKind) -> Vec<OutboundMsg> {
        match kind {

            // ── Election timeout ──────────────────────────────────────────────
            // No heartbeat within the election window.  Start a new election.
            // Random timeout range [150ms, 300ms] reduces split-vote probability.
            TimeoutKind::Election => {
                // Term MUST be incremented before sending RequestVote so peers
                // recognise this as a brand-new election.
                self.current_term += 1;
                self.voted_for = Some(self.id);

                let mut votes_received = HashSet::new();
                votes_received.insert(self.id); // seed with our own vote
                self.role = Role::Candidate { votes_received };

                let term           = self.current_term;
                let candidate_id   = self.id;
                let last_log_index = self.last_log_index();
                let last_log_term  = self.last_log_term();

                self.peers
                    .iter()
                    .map(|&peer| OutboundMsg::Rpc {
                        to: peer,
                        payload: RpcMessage::RequestVote {
                            term, candidate_id, last_log_index, last_log_term,
                        },
                    })
                    .collect()
            }

            // ── Heartbeat timeout ─────────────────────────────────────────────
            // Leaders send periodic AppendEntries so followers never time out.
            // Also doubles as log replication — missing entries ride along.
            TimeoutKind::Heartbeat => {
                // Non-leaders should have had their heartbeat timer cancelled on
                // step-down.  This branch guards against a cancel/fire race.
                if !matches!(self.role, Role::Leader { .. }) {
                    return vec![];
                }
                self.send_heartbeats()
            }
        }
    }

    // ── Handler 4: client commands ────────────────────────────────────────────

    // Append a client command to the log.  Only the Leader can do this.
    // Replication to followers happens on the next heartbeat — no immediate RPCs.
    pub fn handle_command(&mut self, cmd: String) -> Vec<OutboundMsg> {
        if !matches!(self.role, Role::Leader { .. }) {
            return vec![]; // production: return known leader's NodeId for redirect
        }
        let index = self.log.len(); // log[0] is sentinel, so this is correct
        self.log.push(LogEntry { index, term: self.current_term, command: cmd });
        vec![]
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────
//
// Handlers are pure (&mut RaftNode → Vec<OutboundMsg>) so tests need no async
// runtime, no mocking, and no channels beyond construction.
// Strategy: build a node, call handle_message(), assert on the returned Vec
// and the mutated node state.
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // ── Node constructors ─────────────────────────────────────────────────────

    fn make_node(id: u64, peer_ids: &[u64]) -> RaftNode {
        let (_tx, rx) = mpsc::channel(1);
        RaftNode {
            id:           NodeId(id),
            peers:        peer_ids.iter().map(|&p| NodeId(p)).collect(),
            role:         Role::Follower,
            current_term: 0,
            voted_for:    None,
            log:          vec![LogEntry { index: 0, term: 0, command: String::new() }],
            commit_index: 0,
            last_applied: 0,
            inbox:        rx,
        }
    }

    // Follower promoted straight to Leader; next_index = 1 for all peers
    // (log has only the sentinel at this point).
    fn make_leader(id: u64, peer_ids: &[u64], term: u64) -> RaftNode {
        let mut node = make_node(id, peer_ids);
        node.current_term = term;
        node.role = Role::Leader {
            next_index:  peer_ids.iter().map(|&p| (NodeId(p), 1)).collect(),
            match_index: peer_ids.iter().map(|&p| (NodeId(p), 0)).collect(),
        };
        node
    }

    fn push_entry(node: &mut RaftNode, term: u64, cmd: &str) {
        let index = node.log.len();
        node.log.push(LogEntry { index, term, command: cmd.to_string() });
    }

    // ── Message constructors ──────────────────────────────────────────────────

    fn rv(from: u64, term: u64, last_idx: usize, last_term: u64) -> Message {
        Message::Rpc {
            from: NodeId(from),
            payload: RpcMessage::RequestVote {
                term,
                candidate_id:   NodeId(from),
                last_log_index: last_idx,
                last_log_term:  last_term,
            },
        }
    }

    fn ae(from: u64, term: u64, prev_idx: usize, prev_term: u64,
          entries: Vec<LogEntry>, commit: usize) -> Message {
        Message::Rpc {
            from: NodeId(from),
            payload: RpcMessage::AppendEntries {
                term,
                leader_id:      NodeId(from),
                prev_log_index: prev_idx,
                prev_log_term:  prev_term,
                entries,
                leader_commit:  commit,
            },
        }
    }

    fn rv_resp(from: u64, term: u64, granted: bool) -> Message {
        Message::RpcResponse(RpcResponse::RequestVoteResponse {
            from: NodeId(from), term, vote_granted: granted,
        })
    }

    fn ae_resp(from: u64, term: u64, success: bool, match_index: usize) -> Message {
        Message::RpcResponse(RpcResponse::AppendEntriesResponse {
            from: NodeId(from), term, success, match_index,
        })
    }

    // ── Outbound message inspectors ───────────────────────────────────────────

    fn is_follower(n: &RaftNode)  -> bool { matches!(n.role, Role::Follower) }
    fn is_candidate(n: &RaftNode) -> bool { matches!(n.role, Role::Candidate { .. }) }
    fn is_leader(n: &RaftNode)    -> bool { matches!(n.role, Role::Leader { .. }) }

    fn vote_granted_in(msgs: &[OutboundMsg]) -> bool {
        msgs.iter().any(|m| matches!(
            m,
            OutboundMsg::Response {
                payload: RpcResponse::RequestVoteResponse { vote_granted: true, .. }, ..
            }
        ))
    }

    fn append_success_in(msgs: &[OutboundMsg]) -> bool {
        msgs.iter().any(|m| matches!(
            m,
            OutboundMsg::Response {
                payload: RpcResponse::AppendEntriesResponse { success: true, .. }, ..
            }
        ))
    }

    fn count_rv_rpcs(msgs: &[OutboundMsg]) -> usize {
        msgs.iter().filter(|m| matches!(
            m, OutboundMsg::Rpc { payload: RpcMessage::RequestVote { .. }, .. }
        )).count()
    }

    fn count_ae_rpcs(msgs: &[OutboundMsg]) -> usize {
        msgs.iter().filter(|m| matches!(
            m, OutboundMsg::Rpc { payload: RpcMessage::AppendEntries { .. }, .. }
        )).count()
    }

    // ── Election timeout ──────────────────────────────────────────────────────

    #[test]
    fn election_timeout_becomes_candidate() {
        let mut node = make_node(1, &[2, 3, 4, 5]);
        let msgs = node.handle_message(Message::Timeout(TimeoutKind::Election));

        assert!(is_candidate(&node));
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(NodeId(1)));
        assert_eq!(count_rv_rpcs(&msgs), 4, "one RequestVote per peer");
    }

    #[test]
    fn election_timeout_rv_carries_incremented_term() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 5;
        let msgs = node.handle_message(Message::Timeout(TimeoutKind::Election));

        assert_eq!(node.current_term, 6);
        for m in &msgs {
            if let OutboundMsg::Rpc { payload: RpcMessage::RequestVote { term, .. }, .. } = m {
                assert_eq!(*term, 6);
            }
        }
    }

    #[test]
    fn heartbeat_timeout_ignored_by_follower() {
        let mut node = make_node(1, &[2, 3]);
        let msgs = node.handle_message(Message::Timeout(TimeoutKind::Heartbeat));
        assert!(msgs.is_empty());
        assert!(is_follower(&node));
    }

    #[test]
    fn heartbeat_sends_append_entries_to_all_peers() {
        let mut node = make_leader(1, &[2, 3, 4], 1);
        let msgs = node.handle_message(Message::Timeout(TimeoutKind::Heartbeat));
        assert_eq!(count_ae_rpcs(&msgs), 3);
    }

    #[test]
    fn heartbeat_carries_unreplicated_entries() {
        let mut node = make_leader(1, &[2, 3], 1);
        push_entry(&mut node, 1, "cmd");

        let msgs = node.handle_message(Message::Timeout(TimeoutKind::Heartbeat));

        for m in &msgs {
            if let OutboundMsg::Rpc { payload: RpcMessage::AppendEntries { entries, .. }, .. } = m {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].command, "cmd");
            }
        }
    }

    // ── RequestVote ───────────────────────────────────────────────────────────

    #[test]
    fn request_vote_granted_when_eligible() {
        let mut node = make_node(1, &[2, 3]);
        let msgs = node.handle_message(rv(2, 1, 0, 0));

        assert!(vote_granted_in(&msgs));
        assert_eq!(node.voted_for, Some(NodeId(2)));
        assert_eq!(node.current_term, 1);
    }

    #[test]
    fn request_vote_denied_stale_term() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 5;
        let msgs = node.handle_message(rv(2, 3, 0, 0));

        assert!(!vote_granted_in(&msgs));
        assert_eq!(node.voted_for, None);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn request_vote_denied_already_voted_for_other() {
        let mut node = make_node(1, &[2, 3, 4, 5]);
        node.current_term = 1;
        node.voted_for = Some(NodeId(3));

        let msgs = node.handle_message(rv(2, 1, 0, 0));
        assert!(!vote_granted_in(&msgs));
    }

    #[test]
    fn request_vote_granted_to_same_candidate_again() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        node.voted_for = Some(NodeId(2));

        let msgs = node.handle_message(rv(2, 1, 0, 0));
        assert!(vote_granted_in(&msgs));
    }

    #[test]
    fn request_vote_denied_candidate_log_behind_in_term() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 2;
        push_entry(&mut node, 2, "x");

        let msgs = node.handle_message(rv(2, 2, 1, 1)); // candidate's last term=1, ours=2
        assert!(!vote_granted_in(&msgs));
    }

    #[test]
    fn request_vote_denied_candidate_log_shorter() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 2;
        push_entry(&mut node, 2, "a");
        push_entry(&mut node, 2, "b"); // our log: indices 0..=2

        let msgs = node.handle_message(rv(2, 2, 1, 2)); // candidate only has index 1
        assert!(!vote_granted_in(&msgs));
    }

    #[test]
    fn request_vote_leader_steps_down_on_higher_term() {
        let mut node = make_leader(1, &[2, 3], 3);
        let msgs = node.handle_message(rv(2, 4, 0, 0));

        assert!(is_follower(&node));
        assert_eq!(node.current_term, 4);
        assert!(vote_granted_in(&msgs)); // after stepping down we can grant
    }

    // ── AppendEntries ─────────────────────────────────────────────────────────

    #[test]
    fn append_entries_heartbeat_accepted() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let msgs = node.handle_message(ae(2, 1, 0, 0, vec![], 0));
        assert!(append_success_in(&msgs));
    }

    #[test]
    fn append_entries_denied_stale_term() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 5;
        let msgs = node.handle_message(ae(2, 3, 0, 0, vec![], 0));
        assert!(!append_success_in(&msgs));
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn append_entries_denied_consistency_check_wrong_term() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 2;
        push_entry(&mut node, 1, "a");

        let msgs = node.handle_message(ae(2, 2, 1, 2, vec![], 0)); // prev_term=2 but ours=1
        assert!(!append_success_in(&msgs));
        assert_eq!(node.log.len(), 2);
    }

    #[test]
    fn append_entries_denied_prev_index_missing() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let msgs = node.handle_message(ae(2, 1, 1, 1, vec![], 0)); // prev_idx=1 doesn't exist
        assert!(!append_success_in(&msgs));
    }

    #[test]
    fn append_entries_appends_new_entry() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let e = LogEntry { index: 1, term: 1, command: "set x 1".to_string() };
        let msgs = node.handle_message(ae(2, 1, 0, 0, vec![e], 0));

        assert!(append_success_in(&msgs));
        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].command, "set x 1");
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 2;
        push_entry(&mut node, 1, "old_a"); // log[1] term=1 — survives
        push_entry(&mut node, 1, "old_b"); // log[2] term=1 — overwritten

        let new_entry = LogEntry { index: 2, term: 2, command: "new_b".to_string() };
        node.handle_message(ae(2, 2, 1, 1, vec![new_entry], 0));

        assert_eq!(node.log.len(), 3);
        assert_eq!(node.log[1].command, "old_a"); // preserved
        assert_eq!(node.log[2].term,    2);
        assert_eq!(node.log[2].command, "new_b"); // replaced
    }

    #[test]
    fn append_entries_idempotent_resend() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let e = LogEntry { index: 1, term: 1, command: "x".to_string() };
        node.handle_message(ae(2, 1, 0, 0, vec![e.clone()], 0));
        node.handle_message(ae(2, 1, 0, 0, vec![e.clone()], 0));

        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].command, "x");
    }

    #[test]
    fn append_entries_advances_commit_index() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let e = LogEntry { index: 1, term: 1, command: "x".to_string() };
        node.handle_message(ae(2, 1, 0, 0, vec![e], 1)); // leader_commit=1

        assert_eq!(node.commit_index, 1);
    }

    #[test]
    fn append_entries_commit_index_clamped_to_our_last() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        let e = LogEntry { index: 1, term: 1, command: "x".to_string() };
        node.handle_message(ae(2, 1, 0, 0, vec![e], 99)); // leader claims far ahead

        assert_eq!(node.commit_index, 1); // clamped to new_last_index=1
    }

    #[test]
    fn append_entries_candidate_steps_down_to_follower() {
        let mut node = make_node(1, &[2, 3]);
        node.current_term = 1;
        node.voted_for = Some(NodeId(1));
        let mut votes = HashSet::new();
        votes.insert(NodeId(1));
        node.role = Role::Candidate { votes_received: votes };

        node.handle_message(ae(2, 1, 0, 0, vec![], 0));

        assert!(is_follower(&node));
    }

    // ── Vote responses / Leader election ──────────────────────────────────────

    #[test]
    fn becomes_leader_on_majority_votes_3node() {
        let mut node = make_node(1, &[2, 3]);
        node.handle_message(Message::Timeout(TimeoutKind::Election));
        // votes_received={1}; threshold=1; need len > 1

        let msgs = node.handle_message(rv_resp(2, 1, true));
        // votes_received={1,2}; 2 > 1 → Leader

        assert!(is_leader(&node));
        assert_eq!(count_ae_rpcs(&msgs), 2, "immediate heartbeats to both peers");
    }

    #[test]
    fn stays_candidate_without_majority_5node() {
        let mut node = make_node(1, &[2, 3, 4, 5]);
        node.handle_message(Message::Timeout(TimeoutKind::Election));

        node.handle_message(rv_resp(2, 1, true)); // votes={1,2}; 2 > 2? No

        assert!(is_candidate(&node));
    }

    #[test]
    fn denied_vote_response_is_ignored() {
        let mut node = make_node(1, &[2, 3]);
        node.handle_message(Message::Timeout(TimeoutKind::Election));
        node.handle_message(rv_resp(2, 1, false));
        assert!(is_candidate(&node));
    }

    #[test]
    fn vote_response_higher_term_forces_step_down() {
        let mut node = make_node(1, &[2, 3]);
        node.handle_message(Message::Timeout(TimeoutKind::Election)); // term → 1
        node.handle_message(rv_resp(2, 5, false));

        assert!(is_follower(&node));
        assert_eq!(node.current_term, 5);
    }

    // ── AppendEntries responses / commit ──────────────────────────────────────

    #[test]
    fn ae_response_success_updates_peer_tracking() {
        let mut node = make_leader(1, &[2, 3], 1);
        push_entry(&mut node, 1, "cmd");

        node.handle_message(ae_resp(2, 1, true, 1));

        let Role::Leader { ref next_index, ref match_index } = node.role else {
            panic!("should still be Leader");
        };
        assert_eq!(*match_index.get(&NodeId(2)).unwrap(), 1);
        assert_eq!(*next_index.get(&NodeId(2)).unwrap(), 2);
    }

    #[test]
    fn ae_response_failure_decrements_next_index() {
        let mut node = make_leader(1, &[2, 3], 1);
        push_entry(&mut node, 1, "a");
        push_entry(&mut node, 1, "b");
        if let Role::Leader { ref mut next_index, .. } = node.role {
            next_index.insert(NodeId(2), 3); // simulate having sent both entries
        }

        node.handle_message(ae_resp(2, 1, false, 0));

        let Role::Leader { ref next_index, .. } = node.role else {
            panic!("should still be Leader");
        };
        assert_eq!(*next_index.get(&NodeId(2)).unwrap(), 2); // 3 → 2
    }

    #[test]
    fn commit_index_advances_on_majority_5node() {
        let mut node = make_leader(1, &[2, 3, 4, 5], 1);
        push_entry(&mut node, 1, "cmd");

        node.handle_message(ae_resp(2, 1, true, 1));
        assert_eq!(node.commit_index, 0, "self + peer2 = 2 replicas, not > threshold 2");

        node.handle_message(ae_resp(3, 1, true, 1));
        assert_eq!(node.commit_index, 1, "self + peers 2 and 3 = 3 replicas, majority reached");
    }

    #[test]
    fn ae_response_higher_term_steps_down_leader() {
        let mut node = make_leader(1, &[2, 3], 1);
        node.handle_message(ae_resp(2, 5, false, 0));

        assert!(is_follower(&node));
        assert_eq!(node.current_term, 5);
    }

    // ── Figure 8 safety ───────────────────────────────────────────────────────

    #[test]
    fn figure_8_old_term_entry_not_committed_by_count() {
        let mut node = make_leader(1, &[2, 3], 2); // current_term=2
        push_entry(&mut node, 1, "stale");          // index=1, term=1 (old)

        node.handle_message(ae_resp(2, 2, true, 1));
        node.handle_message(ae_resp(3, 2, true, 1)); // majority have it — but wrong term

        assert_eq!(node.commit_index, 0, "prior-term entry must not be committed alone");
    }

    #[test]
    fn figure_8_current_term_commits_old_prefix_implicitly() {
        let mut node = make_leader(1, &[2, 3], 2);
        push_entry(&mut node, 1, "stale");    // index=1, term=1
        push_entry(&mut node, 2, "current");  // index=2, term=2

        node.handle_message(ae_resp(2, 2, true, 2));
        node.handle_message(ae_resp(3, 2, true, 2));

        assert_eq!(node.commit_index, 2); // index=1 committed implicitly via prefix
    }

    // ── handle_command ────────────────────────────────────────────────────────

    #[test]
    fn command_appended_to_leader_log() {
        let mut node = make_leader(1, &[2, 3], 1);
        let msgs = node.handle_message(Message::ClientCommand("set x 99".to_string()));

        assert_eq!(node.log.len(), 2);
        assert_eq!(node.log[1].command, "set x 99");
        assert_eq!(node.log[1].term, 1);
        assert!(msgs.is_empty(), "replication deferred to heartbeat");
    }

    #[test]
    fn command_ignored_by_follower() {
        let mut node = make_node(1, &[2, 3]);
        let msgs = node.handle_message(Message::ClientCommand("set x 1".to_string()));
        assert_eq!(node.log.len(), 1);
        assert!(msgs.is_empty());
    }

    #[test]
    fn command_ignored_by_candidate() {
        let mut node = make_node(1, &[2, 3]);
        node.handle_message(Message::Timeout(TimeoutKind::Election));
        let msgs = node.handle_message(Message::ClientCommand("set x 1".to_string()));
        assert_eq!(node.log.len(), 1);
        assert!(msgs.is_empty());
    }
}
