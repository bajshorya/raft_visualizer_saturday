// http.rs — REST endpoints (non-WebSocket)

use axum::{extract::{Path, State}, http::StatusCode, Json};
use serde::Deserialize;
use crate::app_state::AppState;
use crate::message::Message;
use crate::state::NodeId;

#[derive(Deserialize)]
pub struct CommandBody {
    pub command: String,
}

// POST /command  { "command": "set x 42" }
//
// Broadcasts the command to every node's inbox.
// Only the current Leader will append it to its log; all others silently discard it.
// Returns 200 OK once the sends are dispatched (not once the command is committed).
pub async fn command_handler(
    State(state): State<AppState>,
    Json(body): Json<CommandBody>,
) -> StatusCode {
    for tx in state.node_senders.iter() {
        let _ = tx.send(Message::ClientCommand(body.command.clone())).await;
    }
    StatusCode::OK
}

// POST /crash/:id — mark node as crashed (stops processing messages)
//
// The node's event loop will ignore all incoming messages and stop sending outbound
// messages. Followers will timeout and elect a new leader if the crashed node was Leader.
pub async fn crash_node_handler(
    Path(node_id): Path<u64>,
    State(state): State<AppState>,
) -> StatusCode {
    state.crashed.write().await.insert(NodeId(node_id));
    StatusCode::OK
}

// POST /restart/:id — restart a crashed node
//
// Removes the node from the crashed set and sends a Restart message to reset its state.
// It will resume as a fresh Follower at term 0 and catch up via AppendEntries from
// the current leader.
pub async fn restart_node_handler(
    Path(node_id): Path<u64>,
    State(state): State<AppState>,
) -> StatusCode {
    state.crashed.write().await.remove(&NodeId(node_id));
    // Send Restart message to reset the node's state
    let idx = (node_id - 1) as usize;
    if let Some(tx) = state.node_senders.get(idx) {
        let _ = tx.send(Message::Restart).await;
    }
    StatusCode::OK
}
