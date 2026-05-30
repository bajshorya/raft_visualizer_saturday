// ── node.rs — async event loop, timer tasks, cluster wiring ──────────────────
//
// This is the async shell around the pure Raft logic in handlers.rs.
// NodeRunner owns one RaftNode and drives it:
//
//   inbox.recv() → handle_message() → emit events → dispatch outbound → timers
//
// The Raft state machine (RaftNode) stays completely synchronous.
// All async work — channel I/O, sleeping, spawning — lives here.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rand::Rng;
use tokio::sync::{broadcast, mpsc, watch, RwLock};
use tokio::task::AbortHandle;
use tokio::time::{sleep, Duration};

use crate::events::{EntrySnapshot, RoleLabel, StateEvent};
use crate::message::{Message, OutboundMsg, RpcMessage, RpcResponse, TimeoutKind};
use crate::state::{LogEntry, NodeId, RaftNode, Role};

const ELECTION_MIN_MS: u64 = 150;
const ELECTION_MAX_MS: u64 = 300;
const HEARTBEAT_MS:    u64 = 50;

// ── NodeRunner ────────────────────────────────────────────────────────────────

pub struct NodeRunner {
    pub node: RaftNode,

    // Send-end of our OWN inbox — timer tasks clone this to inject Timeouts.
    self_tx: mpsc::Sender<Message>,

    // Send-ends of every peer's inbox for dispatching outbound messages.
    peers: HashMap<NodeId, mpsc::Sender<Message>>,

    // Publishes commit_index advances (watched by integration tests).
    commit_tx: watch::Sender<usize>,

    // Broadcasts every observable Raft event (consumed by the WebSocket bridge).
    event_tx: broadcast::Sender<StateEvent>,

    // Shared set of crashed node IDs — if our ID is in here, ignore all messages.
    crashed: Arc<RwLock<HashSet<NodeId>>>,

    election_timer:  Option<AbortHandle>,
    heartbeat_timer: Option<AbortHandle>,
}

impl NodeRunner {
    pub fn new(
        node:      RaftNode,
        self_tx:   mpsc::Sender<Message>,
        peers:     HashMap<NodeId, mpsc::Sender<Message>>,
        commit_tx: watch::Sender<usize>,
        event_tx:  broadcast::Sender<StateEvent>,
        crashed:   Arc<RwLock<HashSet<NodeId>>>,
    ) -> Self {
        Self {
            node, self_tx, peers, commit_tx, event_tx, crashed,
            election_timer: None, heartbeat_timer: None,
        }
    }

    // ── Main event loop ───────────────────────────────────────────────────────

    pub async fn run(&mut self) {
        self.reset_election_timer();
        eprintln!("[node {}] started as Follower", self.node.id.0);

        while let Some(msg) = self.node.inbox.recv().await {
            // Handle restart message first — resets state and resumes.
            if matches!(msg, Message::Restart) {
                self.node.reset_to_initial();
                self.cancel_heartbeat_timer();
                self.reset_election_timer();
                eprintln!("[node {}] restarted as Follower (term 0)", self.node.id.0);
                self.emit(StateEvent::RoleChange {
                    node_id: self.node.id.0,
                    role: RoleLabel::Follower,
                    term: 0,
                });
                // Notify watch channel
                let _ = self.commit_tx.send(0);
                continue;
            }

            // If we're crashed, drop all messages and timers.
            if self.crashed.read().await.contains(&self.node.id) {
                self.cancel_election_timer();
                self.cancel_heartbeat_timer();
                continue;
            }

            // ── Snapshot and extract from msg BEFORE consuming it ─────────────
            let log_len_before   = self.node.log.len();
            let was_leader       = matches!(self.node.role, Role::Leader    { .. });
            let was_candidate    = matches!(self.node.role, Role::Candidate { .. });
            let voted_for_before = self.node.voted_for;
            let commit_before    = self.node.commit_index;

            // Is this an election timeout? (term will be incremented by handler)
            let is_election_timeout = matches!(msg, Message::Timeout(TimeoutKind::Election));

            // Vote response data — needed for VoteReceived event.
            let incoming_vote: Option<(NodeId, bool)> = match &msg {
                Message::RpcResponse(RpcResponse::RequestVoteResponse {
                    from, vote_granted, ..
                }) => Some((*from, *vote_granted)),
                _ => None,
            };

            // AppendEntries data — needed for Heartbeat event.
            let incoming_ae: Option<(NodeId, u64)> = match &msg {
                Message::Rpc {
                    payload: RpcMessage::AppendEntries { leader_id, term, .. }, ..
                } => Some((*leader_id, *term)),
                _ => None,
            };

            // Log election start before the term is incremented.
            if is_election_timeout {
                eprintln!(
                    "[node {}] election timeout → starting term {}",
                    self.node.id.0, self.node.current_term + 1
                );
            }

            // ── Core Raft dispatch (pure, sync) ───────────────────────────────
            let outbound = self.node.handle_message(msg);

            // ── Detect what changed ───────────────────────────────────────────
            let is_now_leader    = matches!(self.node.role, Role::Leader    { .. });
            let is_now_candidate = matches!(self.node.role, Role::Candidate { .. });
            let vote_just_granted = self.node.voted_for != voted_for_before;
            let accepted_ae = outbound.iter().any(|m| matches!(
                m,
                OutboundMsg::Response {
                    payload: RpcResponse::AppendEntriesResponse { success: true, .. }, ..
                }
            ));

            // ── Emit state events ─────────────────────────────────────────────

            // ElectionStart — term is already incremented by handle_timeout.
            if is_election_timeout {
                self.emit(StateEvent::ElectionStart {
                    node_id: self.node.id.0,
                    term:    self.node.current_term,
                });
            }

            // RoleChange — detect any role transition.
            let role_before_idx = u8::from(was_leader) * 2 + u8::from(was_candidate);
            let role_after_idx  = u8::from(is_now_leader) * 2 + u8::from(is_now_candidate);
            if role_before_idx != role_after_idx {
                let role = if is_now_leader    { RoleLabel::Leader    }
                           else if is_now_candidate { RoleLabel::Candidate }
                           else                { RoleLabel::Follower  };
                self.emit(StateEvent::RoleChange {
                    node_id: self.node.id.0,
                    role,
                    term: self.node.current_term,
                });
            }

            // VoteCast — voted_for changed.
            if vote_just_granted {
                if let Some(voted_for) = self.node.voted_for {
                    self.emit(StateEvent::VoteCast {
                        node_id:   self.node.id.0,
                        voted_for: voted_for.0,
                        term:      self.node.current_term,
                    });
                }
            }

            // VoteReceived — always emit so the frontend can animate vote arrows.
            if let Some((from, granted)) = incoming_vote {
                self.emit(StateEvent::VoteReceived {
                    node_id: self.node.id.0,
                    from:    from.0,
                    granted,
                });
            }

            // LogAppend — one event per new entry.
            for entry in &self.node.log[log_len_before..] {
                self.emit(StateEvent::LogAppend {
                    node_id: self.node.id.0,
                    entry: EntrySnapshot {
                        index:   entry.index,
                        term:    entry.term,
                        command: entry.command.clone(),
                    },
                });
            }

            // Commit — advance and publish.
            if self.node.commit_index > commit_before {
                eprintln!(
                    "[node {}] committed log[{}..={}] (term {})",
                    self.node.id.0, commit_before + 1,
                    self.node.commit_index, self.node.current_term
                );
                self.emit(StateEvent::Commit {
                    node_id:      self.node.id.0,
                    commit_index: self.node.commit_index,
                });
                let _ = self.commit_tx.send(self.node.commit_index);
            }

            // Heartbeat — follower accepted an AppendEntries from the current leader.
            if accepted_ae {
                if let Some((leader_id, term)) = incoming_ae {
                    self.emit(StateEvent::Heartbeat { leader_id: leader_id.0, term });
                }
            }

            // Log terminal role transitions for the console.
            if !was_leader && is_now_leader {
                eprintln!("[node {}] >>> LEADER (term {})", self.node.id.0, self.node.current_term);
            } else if was_leader && !is_now_leader {
                eprintln!("[node {}] <<< stepped down (term {})", self.node.id.0, self.node.current_term);
            }

            // ── Dispatch outbound messages to peers ───────────────────────────
            self.dispatch_all(outbound).await;

            // ── Timer management ──────────────────────────────────────────────

            // Check if we received a valid AppendEntries (same/higher term).
            // This resets the election timer even if the consistency check failed,
            // because a failed check just means the leader will retry with an earlier
            // prev_log_index. We still recognize it as the legitimate leader.
            let received_valid_ae = incoming_ae
                .map_or(false, |(_, term)| term >= self.node.current_term);

            if !was_leader && is_now_leader {
                self.cancel_election_timer();
                self.start_heartbeat_timer();
            } else if was_leader && !is_now_leader {
                self.cancel_heartbeat_timer();
                self.reset_election_timer();
            } else if vote_just_granted || accepted_ae || received_valid_ae {
                self.reset_election_timer();
            }
        }
        eprintln!("[node {}] shutting down", self.node.id.0);
    }

    // ── Event emission ────────────────────────────────────────────────────────

    fn emit(&self, event: StateEvent) {
        // Error means zero subscribers (no WS clients connected). That's normal.
        let _ = self.event_tx.send(event);
    }

    // ── Outbound dispatch ─────────────────────────────────────────────────────

    async fn dispatch_all(&self, msgs: Vec<OutboundMsg>) {
        for m in msgs {
            match m {
                OutboundMsg::Rpc { to, payload } => {
                    if let Some(tx) = self.peers.get(&to) {
                        let _ = tx.send(Message::Rpc { from: self.node.id, payload }).await;
                    }
                }
                OutboundMsg::Response { to, payload } => {
                    if let Some(tx) = self.peers.get(&to) {
                        let _ = tx.send(Message::RpcResponse(payload)).await;
                    }
                }
            }
        }
    }

    // ── Timer helpers ─────────────────────────────────────────────────────────

    fn reset_election_timer(&mut self) {
        if let Some(h) = self.election_timer.take() { h.abort(); }
        let tx    = self.self_tx.clone();
        let delay = rand::thread_rng().gen_range(ELECTION_MIN_MS..=ELECTION_MAX_MS);
        self.election_timer = Some(
            tokio::spawn(async move {
                sleep(Duration::from_millis(delay)).await;
                let _ = tx.send(Message::Timeout(TimeoutKind::Election)).await;
            })
            .abort_handle(),
        );
    }

    fn start_heartbeat_timer(&mut self) {
        if let Some(h) = self.heartbeat_timer.take() { h.abort(); }
        let tx = self.self_tx.clone();
        self.heartbeat_timer = Some(
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_millis(HEARTBEAT_MS)).await;
                    if tx.send(Message::Timeout(TimeoutKind::Heartbeat)).await.is_err() {
                        break;
                    }
                }
            })
            .abort_handle(),
        );
    }

    fn cancel_election_timer(&mut self) {
        if let Some(h) = self.election_timer.take() { h.abort(); }
    }

    fn cancel_heartbeat_timer(&mut self) {
        if let Some(h) = self.heartbeat_timer.take() { h.abort(); }
    }
}

// ── Cluster factory ───────────────────────────────────────────────────────────

/// Spawn `n` Raft nodes wired with in-process channels.
///
/// Returns:
/// - `Vec<Sender<Message>>` — inject messages from outside (tests, HTTP handlers)
/// - `Vec<watch::Receiver<usize>>` — observe each node's commit_index
/// - `broadcast::Sender<StateEvent>` — subscribe to all cluster state events
///   (pass to the WebSocket bridge; clone it for multiple subscribers)
pub fn spawn_cluster(n: usize) -> (
    Vec<mpsc::Sender<Message>>,
    Vec<watch::Receiver<usize>>,
    broadcast::Sender<StateEvent>,
    Arc<RwLock<HashSet<NodeId>>>,
) {
    let ids: Vec<NodeId> = (1..=n as u64).map(NodeId).collect();

    let channels: Vec<(mpsc::Sender<Message>, mpsc::Receiver<Message>)> =
        (0..n).map(|_| mpsc::channel::<Message>(128)).collect();

    let senders: Vec<mpsc::Sender<Message>> =
        channels.iter().map(|(tx, _)| tx.clone()).collect();

    let watches: Vec<(watch::Sender<usize>, watch::Receiver<usize>)> =
        (0..n).map(|_| watch::channel(0_usize)).collect();

    let commit_rxs: Vec<watch::Receiver<usize>> =
        watches.iter().map(|(_, rx)| rx.clone()).collect();

    // One broadcast channel shared by all nodes.
    // Capacity 256: enough headroom for bursty event streams.
    // If a subscriber is too slow it receives Lagged — events are dropped for
    // that subscriber but the cluster is unaffected.
    let (event_tx, _) = broadcast::channel::<StateEvent>(256);

    // Shared crash state — HTTP handlers add/remove IDs; nodes check it on every message.
    let crashed = Arc::new(RwLock::new(HashSet::new()));

    for (i, ((self_tx, rx), (commit_tx, _))) in
        channels.into_iter().zip(watches).enumerate()
    {
        let id    = ids[i];
        let peers: Vec<NodeId> = ids.iter().filter(|&&p| p != id).copied().collect();

        let peer_senders: HashMap<NodeId, mpsc::Sender<Message>> = ids
            .iter()
            .filter(|&&p| p != id)
            .map(|&p| (p, senders[p.0 as usize - 1].clone()))
            .collect();

        let node = RaftNode {
            id,
            peers,
            role:         Role::Follower,
            current_term: 0,
            voted_for:    None,
            log:          vec![LogEntry { index: 0, term: 0, command: String::new() }],
            commit_index: 0,
            last_applied: 0,
            inbox:        rx,
        };

        let mut runner = NodeRunner::new(
            node, self_tx, peer_senders, commit_tx, event_tx.clone(), crashed.clone(),
        );

        tokio::spawn(async move { runner.run().await; });
    }

    (senders, commit_rxs, event_tx, crashed)
}
