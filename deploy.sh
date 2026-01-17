#!/bin/bash
# ============================================================================
# Sentiment Data Source - VPS Deployment Script
# VPS: 72.62.192.50
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
GITHUB_REPO="git@github.com:YOUR_USERNAME/SentimentDataSource.git"  # TODO: Update with your repo
APP_DIR="/opt/sentiment-data-source"
APP_USER="sentiment"
PYTHON_VERSION="3.11"
NODE_VERSION="20"

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root"
        exit 1
    fi
}

# ============================================================================
# Step 1: System Update & Dependencies
# ============================================================================

install_system_deps() {
    log_info "Updating system packages..."
    apt-get update -y
    apt-get upgrade -y
    
    log_info "Installing system dependencies..."
    apt-get install -y \
        build-essential \
        curl \
        wget \
        git \
        vim \
        htop \
        ufw \
        nginx \
        software-properties-common \
        python${PYTHON_VERSION} \
        python${PYTHON_VERSION}-venv \
        python${PYTHON_VERSION}-dev \
        python3-pip \
        postgresql-client \
        libpq-dev
    
    log_success "System dependencies installed"
}

# ============================================================================
# Step 2: Install Node.js & PM2
# ============================================================================

install_nodejs() {
    log_info "Installing Node.js ${NODE_VERSION}..."
    
    if ! command -v node &> /dev/null; then
        curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash -
        apt-get install -y nodejs
    fi
    
    log_info "Installing PM2..."
    npm install -g pm2
    
    log_success "Node.js $(node -v) and PM2 installed"
}

# ============================================================================
# Step 3: Create Application User
# ============================================================================

create_app_user() {
    log_info "Creating application user: ${APP_USER}..."
    
    if id "${APP_USER}" &>/dev/null; then
        log_warn "User ${APP_USER} already exists"
    else
        useradd -m -s /bin/bash ${APP_USER}
        log_success "User ${APP_USER} created"
    fi
}

# ============================================================================
# Step 4: Clone/Update Repository
# ============================================================================

setup_repository() {
    log_info "Setting up repository..."
    
    # Create app directory
    mkdir -p ${APP_DIR}
    
    if [ -d "${APP_DIR}/.git" ]; then
        log_info "Repository exists, pulling latest changes..."
        cd ${APP_DIR}
        git fetch origin
        git reset --hard origin/main
    else
        log_info "Cloning repository..."
        git clone ${GITHUB_REPO} ${APP_DIR}
    fi
    
    # Set ownership
    chown -R ${APP_USER}:${APP_USER} ${APP_DIR}
    
    log_success "Repository setup complete"
}

# ============================================================================
# Step 5: Setup Python Virtual Environment
# ============================================================================

setup_python_env() {
    log_info "Setting up Python virtual environment..."
    
    cd ${APP_DIR}
    
    # Create venv if not exists
    if [ ! -d ".venv" ]; then
        python${PYTHON_VERSION} -m venv .venv
    fi
    
    # Activate and install dependencies
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    
    # Install additional production dependencies
    pip install \
        gunicorn \
        uvicorn[standard] \
        python-dotenv \
        psycopg2-binary \
        aiohttp \
        fastapi \
        pydantic
    
    deactivate
    
    # Set ownership
    chown -R ${APP_USER}:${APP_USER} ${APP_DIR}/.venv
    
    log_success "Python environment setup complete"
}

# ============================================================================
# Step 6: Setup Environment Variables
# ============================================================================

setup_env_file() {
    log_info "Setting up environment file..."
    
    ENV_FILE="${APP_DIR}/.env"
    
    if [ -f "${ENV_FILE}" ]; then
        log_warn ".env file exists, creating backup..."
        cp ${ENV_FILE} ${ENV_FILE}.backup.$(date +%Y%m%d_%H%M%S)
    fi
    
    # Create .env file from example or create new
    if [ -f "${APP_DIR}/.env.example" ]; then
        cp ${APP_DIR}/.env.example ${ENV_FILE}
        log_warn "Copied .env.example to .env - Please update with actual values!"
    else
        cat > ${ENV_FILE} << 'EOF'
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sentiment_db
DB_USER=sentiment_user
DB_PASSWORD=YOUR_DB_PASSWORD

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN
TELEGRAM_CHANNEL_ID=YOUR_CHANNEL_ID

# API Configuration
API_HOST=0.0.0.0
FLASK_PORT=5000
FASTAPI_PORT=8000

# Environment
ENVIRONMENT=production
DEBUG=false
EOF
        log_warn "Created new .env file - Please update with actual values!"
    fi
    
    chown ${APP_USER}:${APP_USER} ${ENV_FILE}
    chmod 600 ${ENV_FILE}
    
    log_success "Environment file created (remember to update values!)"
}

# ============================================================================
# Step 7: Setup PM2 Ecosystem
# ============================================================================

setup_pm2() {
    log_info "Setting up PM2 ecosystem..."
    
    # Create PM2 ecosystem file for Linux
    cat > ${APP_DIR}/ecosystem.config.js << 'EOF'
module.exports = {
  apps: [
    // Background Worker - Data Collection
    {
      name: "sentiment-worker",
      script: ".venv/bin/python",
      args: "background_worker.py",
      cwd: "/opt/sentiment-data-source",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      env: {
        NODE_ENV: "production",
      },
      error_file: "./logs/worker-error.log",
      out_file: "./logs/worker-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      merge_logs: true,
    },

    // Flask API - Sentiment Analysis (Port 5000)
    {
      name: "sentiment-api",
      script: ".venv/bin/gunicorn",
      args: "-w 4 -b 0.0.0.0:5000 api_service:app",
      cwd: "/opt/sentiment-data-source",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "300M",
      env: {
        NODE_ENV: "production",
        FLASK_ENV: "production",
      },
      error_file: "./logs/api-error.log",
      out_file: "./logs/api-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      merge_logs: true,
    },

    // FastAPI - Social Context API (Port 8000)
    {
      name: "social-context-api",
      script: ".venv/bin/uvicorn",
      args: "fastapi_social_context:app --host 0.0.0.0 --port 8000 --workers 4",
      cwd: "/opt/sentiment-data-source",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "300M",
      env: {
        NODE_ENV: "production",
      },
      error_file: "./logs/social-context-error.log",
      out_file: "./logs/social-context-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      merge_logs: true,
    },
  ],
};
EOF

    # Create logs directory
    mkdir -p ${APP_DIR}/logs
    chown -R ${APP_USER}:${APP_USER} ${APP_DIR}/logs
    
    log_success "PM2 ecosystem configured"
}

# ============================================================================
# Step 8: Setup Nginx Reverse Proxy
# ============================================================================

setup_nginx() {
    log_info "Setting up Nginx reverse proxy..."
    
    cat > /etc/nginx/sites-available/sentiment-api << 'EOF'
# Sentiment Analysis API
upstream sentiment_api {
    server 127.0.0.1:5000;
}

# Social Context API
upstream social_context_api {
    server 127.0.0.1:8000;
}

server {
    listen 80;
    server_name 72.62.192.50;

    # API Rate Limiting
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

    # Sentiment Analysis API
    location /api/v1/sentiment/ {
        limit_req zone=api_limit burst=20 nodelay;
        
        proxy_pass http://sentiment_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Social Context API
    location /api/v1/social/ {
        limit_req zone=api_limit burst=20 nodelay;
        
        proxy_pass http://social_context_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Health Check Endpoints
    location /health {
        proxy_pass http://sentiment_api/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }

    # FastAPI Docs
    location /docs {
        proxy_pass http://social_context_api/docs;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }

    location /openapi.json {
        proxy_pass http://social_context_api/openapi.json;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }

    # Default
    location / {
        return 404 '{"error": "Endpoint not found"}';
        add_header Content-Type application/json;
    }
}
EOF

    # Enable site
    ln -sf /etc/nginx/sites-available/sentiment-api /etc/nginx/sites-enabled/
    rm -f /etc/nginx/sites-enabled/default
    
    # Test and reload Nginx
    nginx -t
    systemctl reload nginx
    
    log_success "Nginx configured"
}

# ============================================================================
# Step 9: Setup Firewall
# ============================================================================

setup_firewall() {
    log_info "Setting up firewall..."
    
    ufw default deny incoming
    ufw default allow outgoing
    
    # SSH
    ufw allow 22/tcp
    
    # HTTP/HTTPS
    ufw allow 80/tcp
    ufw allow 443/tcp
    
    # Enable firewall (non-interactive)
    echo "y" | ufw enable
    
    log_success "Firewall configured"
}

# ============================================================================
# Step 10: Start Services
# ============================================================================

start_services() {
    log_info "Starting services..."
    
    cd ${APP_DIR}
    
    # Start PM2 as app user
    sudo -u ${APP_USER} pm2 start ecosystem.config.js
    
    # Save PM2 process list
    sudo -u ${APP_USER} pm2 save
    
    # Setup PM2 startup script
    env PATH=$PATH:/usr/bin pm2 startup systemd -u ${APP_USER} --hp /home/${APP_USER}
    
    log_success "Services started"
}

# ============================================================================
# Step 11: Display Status
# ============================================================================

display_status() {
    echo ""
    echo "============================================================================"
    echo -e "${GREEN}DEPLOYMENT COMPLETE!${NC}"
    echo "============================================================================"
    echo ""
    echo "VPS IP: 72.62.192.50"
    echo "App Directory: ${APP_DIR}"
    echo "App User: ${APP_USER}"
    echo ""
    echo "API Endpoints:"
    echo "  - Sentiment Analysis: http://72.62.192.50/api/v1/sentiment/analyze"
    echo "  - Social Context:     http://72.62.192.50/api/v1/social/context"
    echo "  - Health Check:       http://72.62.192.50/health"
    echo "  - API Docs:           http://72.62.192.50/docs"
    echo ""
    echo "PM2 Commands:"
    echo "  - sudo -u ${APP_USER} pm2 list"
    echo "  - sudo -u ${APP_USER} pm2 logs"
    echo "  - sudo -u ${APP_USER} pm2 monit"
    echo "  - sudo -u ${APP_USER} pm2 restart all"
    echo ""
    echo -e "${YELLOW}⚠️  IMPORTANT: Update ${APP_DIR}/.env with actual values!${NC}"
    echo ""
    echo "============================================================================"
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
    echo ""
    echo "============================================================================"
    echo "Sentiment Data Source - VPS Deployment"
    echo "Target: 72.62.192.50"
    echo "============================================================================"
    echo ""
    
    check_root
    
    install_system_deps
    install_nodejs
    create_app_user
    setup_repository
    setup_python_env
    setup_env_file
    setup_pm2
    setup_nginx
    setup_firewall
    start_services
    display_status
}

# ============================================================================
# Command Line Options
# ============================================================================

case "$1" in
    --full)
        main
        ;;
    --update)
        log_info "Updating application..."
        setup_repository
        setup_python_env
        sudo -u ${APP_USER} pm2 restart all
        log_success "Application updated"
        ;;
    --restart)
        log_info "Restarting services..."
        sudo -u ${APP_USER} pm2 restart all
        log_success "Services restarted"
        ;;
    --status)
        sudo -u ${APP_USER} pm2 list
        ;;
    --logs)
        sudo -u ${APP_USER} pm2 logs
        ;;
    *)
        echo "Usage: $0 {--full|--update|--restart|--status|--logs}"
        echo ""
        echo "Options:"
        echo "  --full     Full deployment (first time setup)"
        echo "  --update   Update code and restart services"
        echo "  --restart  Restart all services"
        echo "  --status   Show PM2 process status"
        echo "  --logs     Show PM2 logs"
        exit 1
        ;;
esac
