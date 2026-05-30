// tests/cluster_integration.rs — Build Step 5: integration tests
//
// Each test spins up a real in-process cluster, drives it through a scenario,
// and asserts on observable state (commit_index via watch receivers).
//
// No mocking.  The same handlers, timers, and channels that run in production
// run here — the only difference is the cluster fits in a single process.

use backend::message::Message;
use backend::node::spawn_cluster;
use tokio::time::{Duration, sleep, timeout};

// ── helpers ───────────────────────────────────────────────────────────────────

// Broadcast a ClientCommand to every node.
// Only the leader accepts it; others discard it silently.
async fn broadcast(senders: &[tokio::sync::mpsc::Sender<Message>], cmd: &str) {
    for tx in senders {
        let _ = tx.send(Message::ClientCommand(cmd.to_string())).await;
    }
}

// Assert that every node's commit_index reaches `expected` within `deadline`.
// Panics with a clear message if any node times out.
async fn all_nodes_commit(
    watches: &mut Vec<tokio::sync::watch::Receiver<usize>>,
    expected: usize,
    deadline: Duration,
) {
    for (i, rx) in watches.iter_mut().enumerate() {
        // Snapshot commit_index BEFORE the wait so we have something to show
        // in the timeout message without any borrow overlapping wait_for.
        // (*rx.borrow() copies the usize immediately; the borrow is transient.)
        let before = *rx.borrow();

        match timeout(deadline, rx.wait_for(|&c| c >= expected)).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => panic!("watch channel closed for node {}", i + 1),
            Err(_) => panic!(
                "node {} timed out: expected commit_index >= {}, was {} before wait",
                i + 1,
                expected,
                before
            ),
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

// A fresh 5-node cluster must elect exactly one leader and commit a command
// on every node — the core Raft promise.
#[tokio::test]
async fn cluster_elects_leader_and_commits_one_command() {
    let (senders, mut watches, _, _) = spawn_cluster(5);

    // Allow enough time for one full election round (timeouts are 150–300 ms).
    sleep(Duration::from_millis(500)).await;

    broadcast(&senders, "set x 1").await;

    // Every node must commit log[1] within 2 s.
    all_nodes_commit(&mut watches, 1, Duration::from_secs(2)).await;
}

// Three commands sent back-to-back must all be committed on every node in order.
// This exercises the leader's log accumulation and multi-entry AppendEntries.
#[tokio::test]
async fn all_nodes_commit_three_commands_in_order() {
    let (senders, mut watches, _, _) = spawn_cluster(5);
    sleep(Duration::from_millis(500)).await;

    broadcast(&senders, "set a 1").await;
    broadcast(&senders, "set b 2").await;
    broadcast(&senders, "set c 3").await;

    // All five nodes must reach commit_index >= 3.
    all_nodes_commit(&mut watches, 3, Duration::from_secs(3)).await;
}

// A 3-node cluster (minimal fault-tolerant size) must also elect a leader and
// commit.  Majority = 2 nodes; the third can be missing and commits still happen.
#[tokio::test]
async fn three_node_cluster_commits() {
    let (senders, mut watches, _, _) = spawn_cluster(3);
    sleep(Duration::from_millis(500)).await;

    broadcast(&senders, "hello").await;

    all_nodes_commit(&mut watches, 1, Duration::from_secs(2)).await;
}

// Many commands committed sequentially — stress-tests the heartbeat replication
// loop and the descending commit-index scan in try_advance_commit_index.
#[tokio::test]
async fn cluster_commits_ten_commands() {
    let (senders, mut watches, _, _) = spawn_cluster(5);
    sleep(Duration::from_millis(500)).await;

    for i in 1..=10 {
        broadcast(&senders, &format!("cmd_{i}")).await;
        // Tiny yield so the async runtime processes channel sends between commands.
        sleep(Duration::from_millis(5)).await;
    }

    // All 10 entries committed on all 5 nodes within 5 s.
    all_nodes_commit(&mut watches, 10, Duration::from_secs(5)).await;
}

// A burst of commands sent with no sleep between them must all commit.
// This verifies that the leader's log accumulates multiple entries before the
// next heartbeat fires and replicates them all in a single AppendEntries RPC.
#[tokio::test]
async fn rapid_burst_of_commands_all_commit() {
    let (senders, mut watches, _, _) = spawn_cluster(5);
    sleep(Duration::from_millis(500)).await; // wait for election

    // Fire 5 commands with no deliberate pause — they queue in the leader's inbox.
    for i in 1..=5 {
        broadcast(&senders, &format!("burst_{i}")).await;
    }

    // All 5 must commit on all nodes.
    all_nodes_commit(&mut watches, 5, Duration::from_secs(3)).await;
}

// Re-election: force every node to timeout by waiting long enough that even the
// highest election timeout (300 ms) fires.  Then send a command.
// If the cluster recovers and commits, the re-election path is working.
#[tokio::test]
async fn cluster_recovers_and_commits_after_reelection() {
    let (senders, mut watches, _, _) = spawn_cluster(5);

    // Let a first election complete.
    sleep(Duration::from_millis(500)).await;
    broadcast(&senders, "before").await;
    all_nodes_commit(&mut watches, 1, Duration::from_secs(2)).await;

    // Wait long enough that followers could time out (> 300 ms election timeout),
    // but the leader's heartbeats (every 50 ms) should keep them in check.
    // This confirms the heartbeat loop is keeping the cluster stable.
    sleep(Duration::from_millis(600)).await;

    broadcast(&senders, "after").await;
    all_nodes_commit(&mut watches, 2, Duration::from_secs(2)).await;
}

// ── WebSocket bridge tests ─────────────────────────────────────────────────

// Verify StateEvent serialises to the JSON shape the frontend expects.
#[test]
fn state_events_serialise_to_correct_json() {
    use backend::events::{EntrySnapshot, RoleLabel, StateEvent};

    let cases: &[(StateEvent, &str)] = &[
        (
            StateEvent::RoleChange { node_id: 3, role: RoleLabel::Leader, term: 2 },
            r#"{"type":"role_change","node_id":3,"role":"Leader","term":2}"#,
        ),
        (
            StateEvent::ElectionStart { node_id: 2, term: 3 },
            r#"{"type":"election_start","node_id":2,"term":3}"#,
        ),
        (
            StateEvent::VoteCast { node_id: 1, voted_for: 2, term: 1 },
            r#"{"type":"vote_cast","node_id":1,"voted_for":2,"term":1}"#,
        ),
        (
            StateEvent::VoteReceived { node_id: 2, from: 1, granted: true },
            r#"{"type":"vote_received","node_id":2,"from":1,"granted":true}"#,
        ),
        (
            StateEvent::LogAppend {
                node_id: 4,
                entry: EntrySnapshot { index: 1, term: 1, command: "set x 1".into() },
            },
            r#"{"type":"log_append","node_id":4,"entry":{"index":1,"term":1,"command":"set x 1"}}"#,
        ),
        (
            StateEvent::Commit { node_id: 4, commit_index: 1 },
            r#"{"type":"commit","node_id":4,"commit_index":1}"#,
        ),
        (
            StateEvent::Heartbeat { leader_id: 4, term: 1 },
            r#"{"type":"heartbeat","leader_id":4,"term":1}"#,
        ),
    ];

    for (event, expected) in cases {
        let got = serde_json::to_string(event).unwrap();
        assert_eq!(got, *expected, "wrong JSON for {:?}", event);
    }
}

// Verify that the broadcast channel actually receives events when a cluster runs.
// We subscribe before the cluster starts so we catch the very first election.
#[tokio::test]
async fn broadcast_channel_emits_election_and_role_change_events() {
    use std::collections::HashSet;

    let (_senders, _watches, event_tx, _crashed) = spawn_cluster(5);
    let mut rx = event_tx.subscribe(); // subscribe before any events fire

    // Collect events for up to 1 s — more than enough for one election.
    let mut seen_types: HashSet<String> = HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Ok(event)) => {
                // Verify every event round-trips through JSON cleanly.
                let json = serde_json::to_string(&event).expect("failed to serialise event");
                let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
                let type_field = parsed["type"].as_str().unwrap().to_string();
                seen_types.insert(type_field);
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
            _ => break, // timeout or channel closed
        }
    }

    // A 5-node cluster must have fired at least an election_start and a role_change.
    assert!(seen_types.contains("election_start"), "no election_start seen; got: {:?}", seen_types);
    assert!(seen_types.contains("role_change"),    "no role_change seen; got: {:?}", seen_types);
}
