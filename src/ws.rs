// ── ws.rs — WebSocket bridge ──────────────────────────────────────────────────
//
// Axum handler for the /ws endpoint.  Each connecting client subscribes to
// the cluster's broadcast::Sender<StateEvent> and receives a live JSON stream
// of every Raft state change.
//
// Architecture rule: this module is the ONLY place that knows about WebSocket.
// The Raft state machine (handlers.rs) and the event loop (node.rs) never
// import axum or tungstenite — they only write to a broadcast channel.

use axum::extract::State;
use axum::extract::ws::{Message as WsMsg, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use tokio::sync::broadcast;

use crate::app_state::AppState;
use crate::events::StateEvent;

// ── Handler ───────────────────────────────────────────────────────────────────

// Axum calls this when a client connects to GET /ws.
// WebSocketUpgrade performs the HTTP → WS handshake, then calls drive_socket.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // subscribe() before the upgrade so we miss no events during the handshake.
    let rx = state.event_tx.subscribe();
    ws.on_upgrade(move |socket| drive_socket(socket, rx))
}

// ── Per-connection driver ─────────────────────────────────────────────────────

async fn drive_socket(mut socket: WebSocket, mut rx: broadcast::Receiver<StateEvent>) {
    loop {
        tokio::select! {
            // ── Cluster event → forward to browser ───────────────────────────
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        match serde_json::to_string(&event) {
                            Ok(json) => {
                                // Axum 0.8 wraps text frames in Utf8Bytes.
                                if socket.send(WsMsg::Text(json.into())).await.is_err() {
                                    break; // client disconnected
                                }
                            }
                            Err(e) => eprintln!("[ws] serialize error: {e}"),
                        }
                    }
                    // Client is consuming too slowly — some events were skipped.
                    // Log it and continue; the frontend will show a gap but stays live.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[ws] subscriber lagged — dropped {n} events");
                    }
                    // Broadcast channel closed (cluster shut down).
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            // ── Browser message → handle (ping/close/unexpected text) ─────────
            msg = socket.recv() => {
                match msg {
                    Some(Ok(WsMsg::Close(_))) | None => break,
                    Some(Ok(WsMsg::Ping(data))) => {
                        // Axum auto-handles Ping with a Pong, but we still drain them.
                        let _ = socket.send(WsMsg::Pong(data)).await;
                    }
                    _ => {} // ignore Text/Binary from browser (read-only stream)
                }
            }
        }
    }
}
