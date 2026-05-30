# Deployment Guide

## Quick Start (Development)

### Terminal 1: Backend
```bash
cd backend
cargo run
# Listens on http://localhost:3001
```

### Terminal 2: Frontend
```bash
cd backend/frontend
npm install
npm run dev
# Opens on http://localhost:3000
```

---

## Production Deployment

### Option 1: Docker (Recommended)

**Build and run:**
```bash
docker-compose up --build
```

Access at `http://localhost:3000`

**Background mode:**
```bash
docker-compose up -d
```

**Stop:**
```bash
docker-compose down
```

---

### Option 2: Manual Production Build

#### Backend
```bash
cd backend
cargo build --release
./target/release/backend
```

#### Frontend
```bash
cd backend/frontend
npm install
npm run build
npm start
```

Configure frontend to point to your backend URL in `frontend/hooks/useRaftCluster.ts`:
```typescript
const WS_URL   = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:3001/ws";
const HTTP_URL = process.env.NEXT_PUBLIC_HTTP_URL || "http://localhost:3001";
```

---

### Option 3: VPS / Cloud Deployment

#### On Ubuntu/Debian Server

**1. Install dependencies:**
```bash
# Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Node.js
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# Build essentials
sudo apt-get install build-essential pkg-config libssl-dev
```

**2. Clone and build:**
```bash
git clone <your-repo>
cd DevDrift/backend

# Backend
cargo build --release

# Frontend
cd frontend
npm install
npm run build
```

**3. Run with systemd (persistent service):**

Create `/etc/systemd/system/raft-backend.service`:
```ini
[Unit]
Description=Raft Backend
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/DevDrift/backend
ExecStart=/home/ubuntu/DevDrift/backend/target/release/backend
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/raft-frontend.service`:
```ini
[Unit]
Description=Raft Frontend
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/DevDrift/backend/frontend
ExecStart=/usr/bin/npm start
Environment="NEXT_PUBLIC_WS_URL=ws://YOUR_SERVER_IP:3001/ws"
Environment="NEXT_PUBLIC_HTTP_URL=http://YOUR_SERVER_IP:3001"
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable raft-backend raft-frontend
sudo systemctl start raft-backend raft-frontend
```

**4. Setup Nginx reverse proxy:**

Install nginx:
```bash
sudo apt-get install nginx
```

Create `/etc/nginx/sites-available/raft`:
```nginx
server {
    listen 80;
    server_name your-domain.com;

    # Frontend
    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    # Backend API
    location /api/ {
        proxy_pass http://localhost:3001/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # WebSocket
    location /ws {
        proxy_pass http://localhost:3001/ws;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
```

Enable and restart:
```bash
sudo ln -s /etc/nginx/sites-available/raft /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

---

### Option 4: Cloud Platforms

#### Render.com
1. Create a new **Web Service** for backend
   - Build Command: `cargo build --release`
   - Start Command: `./target/release/backend`
   - Port: 3001

2. Create a new **Web Service** for frontend
   - Build Command: `cd frontend && npm install && npm run build`
   - Start Command: `cd frontend && npm start`
   - Environment Variables:
     - `NEXT_PUBLIC_WS_URL=wss://your-backend.onrender.com/ws`
     - `NEXT_PUBLIC_HTTP_URL=https://your-backend.onrender.com`

#### Fly.io
```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Deploy backend
cd backend
fly launch
fly deploy

# Deploy frontend
cd frontend
fly launch
fly deploy
```

#### Railway.app
1. Connect your GitHub repo
2. Add two services:
   - **Backend**: Root directory = `backend`, Start command = `cargo run --release`
   - **Frontend**: Root directory = `backend/frontend`, Start command = `npm start`
3. Set environment variables in frontend service

---

## Environment Variables

### Frontend (.env.local)
```bash
NEXT_PUBLIC_WS_URL=ws://localhost:3001/ws
NEXT_PUBLIC_HTTP_URL=http://localhost:3001
```

For production, replace with your actual backend URLs (use `wss://` and `https://` for SSL).

---

## Port Configuration

Default ports:
- **Backend**: 3001 (WebSocket + REST API)
- **Frontend**: 3000

To change backend port, edit `src/main.rs`:
```rust
let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
```

---

## Firewall Rules

Open these ports on your server:
```bash
sudo ufw allow 22    # SSH
sudo ufw allow 80    # HTTP
sudo ufw allow 443   # HTTPS (if using SSL)
sudo ufw allow 3000  # Frontend (if not using nginx)
sudo ufw allow 3001  # Backend (if not using nginx)
sudo ufw enable
```

---

## SSL/HTTPS (Production)

### With Nginx + Let's Encrypt:
```bash
sudo apt-get install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

Update frontend env:
```bash
NEXT_PUBLIC_WS_URL=wss://your-domain.com/ws
NEXT_PUBLIC_HTTP_URL=https://your-domain.com
```

---

## Monitoring

### Check logs
```bash
# Backend
sudo journalctl -u raft-backend -f

# Frontend
sudo journalctl -u raft-frontend -f

# Nginx
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### Resource usage
```bash
htop
# or
docker stats  # if using Docker
```

---

## Troubleshooting

**WebSocket connection fails:**
- Check CORS settings in `main.rs` (should be permissive for dev)
- Verify WebSocket URL in frontend matches backend address
- Check firewall rules
- Ensure nginx WebSocket proxy config is correct

**Port already in use:**
```bash
# Find process using port 3001
sudo lsof -i :3001
# Kill it
sudo kill -9 <PID>
```

**Backend crashes:**
```bash
# Check logs
cargo run
# or
sudo journalctl -u raft-backend -n 100
```

---

## Performance Tuning

For production, consider:

1. **Increase Rust optimization level** in `Cargo.toml`:
```toml
[profile.release]
opt-level = 3
lto = true
codegen-units = 1
```

2. **Use PM2 for frontend** (better than npm start):
```bash
npm install -g pm2
pm2 start npm --name "raft-frontend" -- start
pm2 save
pm2 startup
```

3. **Enable gzip in nginx** for frontend assets

4. **Increase OS limits** for WebSocket connections:
```bash
# Edit /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
```
