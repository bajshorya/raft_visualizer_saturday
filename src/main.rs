use std::sync::Arc;
use axum::{routing::{get, post}, Router};
use tower_http::cors::CorsLayer;
use tokio::time::{sleep, Duration};
use backend::app_state::AppState;
use backend::http::{command_handler, crash_node_handler, restart_node_handler};
use backend::node::spawn_cluster;
use backend::ws::ws_handler;

#[tokio::main]
async fn main() {
    println!("=== Raft cluster starting (5 nodes) ===");
    let (senders, _watches, event_tx, crashed) = spawn_cluster(5);

    let state = AppState {
        event_tx,
        node_senders: Arc::new(senders.clone()),
        crashed,
    };

    // CorsLayer::permissive() handles the OPTIONS preflight correctly —
    // sets Allow-Origin/Methods/Headers/* and responds to preflight requests
    // so the browser lets the POST through from localhost:3000.
    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/ws",           get(ws_handler))
        .route("/command",      post(command_handler))
        .route("/crash/{id}",    post(crash_node_handler))
        .route("/restart/{id}",  post(restart_node_handler))
        .layer(cors)
        .with_state(state);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
            .await
            .expect("port 3001 already in use — is another instance running?");
        println!("Backend listening on http://localhost:3001");
        println!("  WebSocket : ws://localhost:3001/ws");
        println!("  Commands  : POST http://localhost:3001/command");
        axum::serve(listener, app).await.unwrap();
    });

    // Keep the runtime alive.
    println!("Cluster running. Start the frontend: cd frontend && npm run dev");
    loop {
        sleep(Duration::from_secs(3600)).await;
    }
}
