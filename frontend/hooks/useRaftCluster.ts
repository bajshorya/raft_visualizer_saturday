"use client";

import { useCallback, useEffect, useReducer } from "react";
import type {
  ClusterState,
  NodeState,
  StateEvent,
  TimestampedEvent,
} from "../types/raft";

const WS_URL   = "ws://localhost:3001/ws";
const HTTP_URL = "http://localhost:3001";
const NODE_IDS = [1, 2, 3, 4, 5];
const MAX_EVENTS = 120; // how many events to keep in the feed

// ── Initial state ─────────────────────────────────────────────────────────────

function makeNode(id: number): NodeState {
  return { id, role: "Follower", term: 0, commitIndex: 0, log: [], votedFor: null };
}

function initialCluster(): ClusterState {
  return Object.fromEntries(NODE_IDS.map((id) => [id, makeNode(id)]));
}

// ── State reducer ─────────────────────────────────────────────────────────────

interface State {
  cluster:   ClusterState;
  events:    TimestampedEvent[];
  connected: boolean;
}

type Action =
  | { kind: "event";       event: StateEvent }
  | { kind: "connected"   }
  | { kind: "disconnected" };

function applyEvent(cluster: ClusterState, e: StateEvent): ClusterState {
  // Helper: get existing node or create a default one.
  const get = (id: number): NodeState => cluster[id] ?? makeNode(id);

  switch (e.type) {
    case "role_change":
      return { ...cluster, [e.node_id]: { ...get(e.node_id), role: e.role, term: e.term } };

    case "log_append":
      return {
        ...cluster,
        [e.node_id]: {
          ...get(e.node_id),
          log: [...get(e.node_id).log, e.entry],
        },
      };

    case "commit":
      return { ...cluster, [e.node_id]: { ...get(e.node_id), commitIndex: e.commit_index } };

    case "vote_cast":
      return {
        ...cluster,
        [e.node_id]: { ...get(e.node_id), votedFor: e.voted_for, term: e.term },
      };

    case "election_start":
      // term advances; role stays Follower until the handler fires next
      return { ...cluster, [e.node_id]: { ...get(e.node_id), term: e.term } };

    case "heartbeat":
      // Only leaders send heartbeats. If we receive one, update that node to Leader.
      // This handles late-joining clients who missed the initial election events.
      return {
        ...cluster,
        [e.leader_id]: { ...get(e.leader_id), role: "Leader", term: e.term },
      };

    case "vote_received":
      // This event is for visualization (animating vote arrows) — doesn't change node state.
      return cluster;

    default:
      return cluster;
  }
}

function reducer(state: State, action: Action): State {
  switch (action.kind) {
    case "event":
      return {
        ...state,
        cluster: applyEvent(state.cluster, action.event),
        events:  [{ ...action.event, ts: Date.now() }, ...state.events].slice(0, MAX_EVENTS),
      };
    case "connected":
      return { ...state, connected: true };
    case "disconnected":
      return { ...state, connected: false, cluster: initialCluster() };
  }
}

// ── Hook ──────────────────────────────────────────────────────────────────────

export function useRaftCluster() {
  const [state, dispatch] = useReducer(reducer, {
    cluster:   initialCluster(),
    events:    [],
    connected: false,
  });

  // WebSocket connection with automatic reconnect.
  useEffect(() => {
    let ws: WebSocket | null = null;
    let destroyed = false;

    function connect() {
      ws = new WebSocket(WS_URL);

      ws.onopen = () => dispatch({ kind: "connected" });

      ws.onclose = () => {
        dispatch({ kind: "disconnected" });
        if (!destroyed) setTimeout(connect, 2000);
      };

      ws.onerror = () => ws?.close();

      ws.onmessage = (msg) => {
        try {
          const event = JSON.parse(msg.data as string) as StateEvent;
          dispatch({ kind: "event", event });
        } catch {
          // ignore malformed messages
        }
      };
    }

    connect();
    return () => {
      destroyed = true;
      ws?.close();
    };
  }, []);

  // POST a command to the backend — the current leader will accept it.
  const sendCommand = useCallback(async (command: string): Promise<void> => {
    const res = await fetch(`${HTTP_URL}/command`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ command }),
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    }
  }, []);

  const crashNode = useCallback(async (nodeId: number): Promise<void> => {
    await fetch(`${HTTP_URL}/crash/${nodeId}`, { method: "POST" });
  }, []);

  const restartNode = useCallback(async (nodeId: number): Promise<void> => {
    await fetch(`${HTTP_URL}/restart/${nodeId}`, { method: "POST" });
  }, []);

  return { ...state, sendCommand, crashNode, restartNode };
}
