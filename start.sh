#!/bin/bash

# Raft Cluster Quick Start Script

set -e

echo "╔══════════════════════════════════════════════════════════╗"
echo "║          RAFT CONSENSUS CLUSTER - QUICK START           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Check if running in Docker mode
if [ "$1" = "docker" ]; then
    echo "🐳 Starting with Docker Compose..."
    docker-compose up --build
    exit 0
fi

# Check dependencies
echo "🔍 Checking dependencies..."

if ! command -v cargo &> /dev/null; then
    echo "❌ Rust not found. Install from https://rustup.rs/"
    exit 1
fi

if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Install from https://nodejs.org/"
    exit 1
fi

echo "✅ All dependencies found"
echo ""

# Build backend
echo "🦀 Building Rust backend..."
cargo build --release
echo "✅ Backend built"
echo ""

# Setup frontend
echo "⚛️  Setting up Next.js frontend..."
cd frontend

if [ ! -d "node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    npm install
fi

if [ ! -f ".env.local" ]; then
    echo "📝 Creating .env.local from example..."
    cp .env.example .env.local
fi

echo "✅ Frontend ready"
cd ..
echo ""

# Start services
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                  STARTING SERVICES                       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "🚀 Backend: http://localhost:3001"
echo "🚀 Frontend: http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop both services"
echo ""

# Start backend in background
./target/release/backend &
BACKEND_PID=$!

# Give backend time to start
sleep 2

# Start frontend
cd frontend
npm run dev &
FRONTEND_PID=$!

# Cleanup on exit
trap "echo ''; echo 'Stopping services...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM

# Wait for both processes
wait
