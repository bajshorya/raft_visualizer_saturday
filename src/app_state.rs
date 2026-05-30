// app_state.rs — shared Axum router state
//
// Holds everything both HTTP handlers and the WebSocket bridge need.
// Passed to every route via .with_state(AppState { .. }).

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use crate::events::StateEvent;
use crate::message::Message;
use crate::state::NodeId;

#[derive(Clone)]
pub struct AppState {
    // Broadcasts every Raft state-change event to all WS subscribers.
    pub event_tx: broadcast::Sender<StateEvent>,
    // One sender per node — used by the /command route to inject client commands.
    pub node_senders: Arc<Vec<mpsc::Sender<Message>>>,
    // Set of crashed node IDs — these nodes ignore all messages and don't send any.
    pub crashed: Arc<RwLock<HashSet<NodeId>>>,
}
