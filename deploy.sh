#!/bin/bash
# ============================================================================
# Sentiment Data Source - VPS Deployment Script
# VPS: 72.62.192.50
# ============================================================================
# 
# USAGE:
#   1. Clone code manually to /opt/SentimentDataSource
#   2. Run: chmod +x deploy.sh && sudo ./deploy.sh --full
#
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_DIR="/opt/SentimentDataSource"
APP_USER="sentiment"
PYTHON_VERSION="3.11"
NODE_VERSION="20"

# Ensure PostgreSQL binaries are in PATH
export PATH=$PATH:/usr/lib/postgresql/16/bin:/usr/lib/postgresql/15/bin:/usr/lib/postgresql/14/bin:/usr/bin

# Database Configuration (read from .env or use defaults)
DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_NAME="${POSTGRES_DATABASE:-sentiment_db}"
DB_USER="${POSTGRES_USER:-sentiment_user}"
DB_PASSWORD="${POSTGRES_PASSWORD:-Cuongnv123456}"

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

check_app_dir() {
    if [ ! -d "${APP_DIR}" ]; then
        log_error "Application directory not found: ${APP_DIR}"
        log_error "Please clone the repository first:"
        log_error "  git clone <your-repo-url> ${APP_DIR}"
        exit 1
    fi
}

load_env() {
    if [ -f "${APP_DIR}/.env" ]; then
        log_info "Loading environment variables from .env..."
        set -a
        source ${APP_DIR}/.env
        set +a
        
        # Update variables after loading
        DB_HOST="${POSTGRES_HOST:-localhost}"
        DB_PORT="${POSTGRES_PORT:-5432}"
        DB_NAME="${POSTGRES_DATABASE:-sentiment_db}"
        DB_USER="${POSTGRES_USER:-sentiment_user}"
        DB_PASSWORD="${POSTGRES_PASSWORD:-Cuongnv123456}"
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
        python3-pip \
        postgresql \
        postgresql-contrib \
        postgresql-client \
        libpq-dev
    
    # Install Python - try deadsnakes PPA first, fallback to system python3
    log_info "Installing Python ${PYTHON_VERSION}..."
    if ! command -v python${PYTHON_VERSION} &> /dev/null; then
        # Add deadsnakes PPA for newer Python versions
        add-apt-repository -y ppa:deadsnakes/ppa 2>/dev/null || true
        apt-get update -y
        apt-get install -y python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev 2>/dev/null
        
        # If still not available, use system Python 3
        if ! command -v python${PYTHON_VERSION} &> /dev/null; then
            log_warn "Python ${PYTHON_VERSION} not available, using system Python 3"
            PYTHON_VERSION="3"
            apt-get install -y python3 python3-venv python3-dev
        fi
    fi
    
    log_info "Using Python version: $(python${PYTHON_VERSION} --version)"
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
    
    # Set ownership of app directory
    chown -R ${APP_USER}:${APP_USER} ${APP_DIR}
}

# ============================================================================
# Step 4: Setup PostgreSQL Database
# ============================================================================

setup_postgresql() {
    log_info "Setting up PostgreSQL database..."
    
    # Find psql binary
    PSQL_BIN=$(which psql 2>/dev/null || find /usr/lib/postgresql -name psql 2>/dev/null | head -1)
    if [ -z "$PSQL_BIN" ]; then
        log_error "psql not found. Please ensure postgresql-client is installed."
        exit 1
    fi
    log_info "Using psql: $PSQL_BIN"
    
    # Start PostgreSQL if not running
    systemctl start postgresql
    systemctl enable postgresql
    
    # Create database user and database
    sudo -u postgres $PSQL_BIN -tc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1 || \
        sudo -u postgres $PSQL_BIN -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';"
    
    sudo -u postgres $PSQL_BIN -tc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 || \
        sudo -u postgres $PSQL_BIN -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};"
    
    # Grant privileges (PostgreSQL 15+ requires explicit schema permissions)
    sudo -u postgres $PSQL_BIN -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "GRANT ALL ON SCHEMA public TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "GRANT CREATE ON SCHEMA public TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "ALTER SCHEMA public OWNER TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${DB_USER};"
    sudo -u postgres $PSQL_BIN -d ${DB_NAME} -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ${DB_USER};"
    
    log_success "PostgreSQL database configured"
}

# ============================================================================
# Step 5: Database Migration - Create Tables
# ============================================================================

run_database_migration() {
    log_info "Running database migrations..."
    
    # Create migration SQL file
    cat > /tmp/migration.sql << 'EOSQL'
-- ============================================================================
-- SENTIMENT DATA SOURCE - DATABASE MIGRATION
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 1. TELEGRAM SOURCES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS telegram_sources (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(255) UNIQUE NOT NULL,
    channel_name VARCHAR(255) NOT NULL,
    asset VARCHAR(50) NOT NULL DEFAULT 'BTC',
    channel_type VARCHAR(50) NOT NULL DEFAULT 'signal',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 2. TWITTER SOURCES TABLE (with priority for batch rotation)
-- ============================================================================
CREATE TABLE IF NOT EXISTS twitter_sources (
    id SERIAL PRIMARY KEY,
    account_id VARCHAR(255) UNIQUE NOT NULL,
    handle VARCHAR(255) NOT NULL,
    asset VARCHAR(50) NOT NULL DEFAULT 'BTC',
    account_type VARCHAR(50) NOT NULL DEFAULT 'analyst',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    followers_weight REAL NOT NULL DEFAULT 1.0,
    priority SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 2.1 TWITTER MESSAGES TABLE (collected tweets)
-- ============================================================================
CREATE TABLE IF NOT EXISTS twitter_messages (
    id SERIAL PRIMARY KEY,
    tweet_id VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(255) NOT NULL,
    text TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    asset VARCHAR(50) DEFAULT 'BTC',
    like_count INTEGER DEFAULT 0,
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    engagement_weight DECIMAL(10, 4) DEFAULT 0,
    author_weight DECIMAL(10, 4) DEFAULT 0,
    velocity DECIMAL(10, 4) DEFAULT 0,
    source_reliability DECIMAL(5, 4) DEFAULT 0.5,
    fingerprint VARCHAR(64),
    is_retweet BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_twitter_messages_username ON twitter_messages(username);
CREATE INDEX IF NOT EXISTS idx_twitter_messages_event_time ON twitter_messages(event_time);
CREATE INDEX IF NOT EXISTS idx_twitter_messages_asset ON twitter_messages(asset);

-- ============================================================================
-- 3. REDDIT SOURCES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS reddit_sources (
    id SERIAL PRIMARY KEY,
    subreddit VARCHAR(255) UNIQUE NOT NULL,
    asset VARCHAR(50) NOT NULL DEFAULT 'BTC',
    role VARCHAR(50) NOT NULL DEFAULT 'discussion',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_posts_per_run INTEGER NOT NULL DEFAULT 100,
    priority SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 4. INGESTED MESSAGES TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS ingested_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) NOT NULL,
    source VARCHAR(50) NOT NULL,
    source_id INTEGER,
    source_name VARCHAR(255),
    asset VARCHAR(50) NOT NULL DEFAULT 'BTC',
    text TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    fingerprint VARCHAR(255) NOT NULL,
    engagement_weight REAL DEFAULT 0,
    author_weight REAL DEFAULT 0,
    velocity REAL DEFAULT 0,
    source_reliability REAL NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(message_id, source)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_ingested_messages_source ON ingested_messages(source, source_id);
CREATE INDEX IF NOT EXISTS idx_ingested_messages_event_time ON ingested_messages(event_time);
CREATE INDEX IF NOT EXISTS idx_ingested_messages_asset ON ingested_messages(asset);
CREATE INDEX IF NOT EXISTS idx_ingested_messages_fingerprint ON ingested_messages(fingerprint);

-- ============================================================================
-- 5. SENTIMENT RESULTS TABLE
-- ============================================================================
-- ============================================================================
-- 5. SENTIMENT RESULTS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS sentiment_results (
    id SERIAL PRIMARY KEY,
    message_id INTEGER,
    asset VARCHAR(50) NOT NULL,
    sentiment_score REAL NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,
    confidence REAL NOT NULL,
    weighted_score REAL NOT NULL,
    signal_strength REAL NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index for analysis queries
CREATE INDEX IF NOT EXISTS idx_sentiment_results_asset ON sentiment_results(asset);
CREATE INDEX IF NOT EXISTS idx_sentiment_results_created ON sentiment_results(created_at);
CREATE INDEX IF NOT EXISTS idx_sentiment_results_label ON sentiment_results(sentiment_label);
CREATE INDEX IF NOT EXISTS idx_sentiment_results_message ON sentiment_results(message_id);

-- ============================================================================
-- 6. AGGREGATED SIGNALS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS aggregated_signals (
    id SERIAL PRIMARY KEY,
    asset VARCHAR(50) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    total_messages INTEGER NOT NULL,
    bullish_count INTEGER NOT NULL DEFAULT 0,
    bearish_count INTEGER NOT NULL DEFAULT 0,
    neutral_count INTEGER NOT NULL DEFAULT 0,
    avg_sentiment REAL NOT NULL,
    weighted_sentiment REAL NOT NULL,
    signal_type VARCHAR(20) NOT NULL,
    signal_strength REAL NOT NULL,
    velocity REAL NOT NULL DEFAULT 0,
    sources_breakdown JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index for time-based queries
CREATE INDEX IF NOT EXISTS idx_aggregated_signals_asset ON aggregated_signals(asset);
CREATE INDEX IF NOT EXISTS idx_aggregated_signals_window ON aggregated_signals(window_end);

-- ============================================================================
-- 7. ALERT HISTORY TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    asset VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    signal_id INTEGER,
    message TEXT NOT NULL,
    telegram_message_id VARCHAR(255),
    sent_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_alert_history_asset ON alert_history(asset);
CREATE INDEX IF NOT EXISTS idx_alert_history_sent ON alert_history(sent_at);

-- ============================================================================
-- 8. RISK INDICATORS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS risk_indicators (
    id SERIAL PRIMARY KEY,
    asset VARCHAR(50) NOT NULL DEFAULT 'BTC',
    sentiment_reliability VARCHAR(20) DEFAULT 'normal',
    fear_greed_index REAL,
    fear_greed_zone VARCHAR(30) DEFAULT 'unknown',
    social_overheat BOOLEAN DEFAULT FALSE,
    panic_risk BOOLEAN DEFAULT FALSE,
    fomo_risk BOOLEAN DEFAULT FALSE,
    data_quality_overall VARCHAR(20) DEFAULT 'healthy',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_risk_indicators_asset ON risk_indicators(asset);
CREATE INDEX IF NOT EXISTS idx_risk_indicators_created ON risk_indicators(created_at);

-- ============================================================================
-- 9. DATA QUALITY METRICS TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    availability_status VARCHAR(20) DEFAULT 'ok',
    time_integrity_status VARCHAR(20) DEFAULT 'ok',
    volume_status VARCHAR(30) DEFAULT 'normal',
    overall_quality VARCHAR(20) DEFAULT 'healthy',
    metrics_detail JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index
CREATE INDEX IF NOT EXISTS idx_dq_metrics_source ON data_quality_metrics(source_type);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_created ON data_quality_metrics(created_at);

-- ============================================================================
-- 10. INSERT DEFAULT SOURCES (18 Telegram, 21 Twitter, 15 Reddit)
-- ============================================================================

-- Telegram Sources (18 crypto channels) - matching local database
INSERT INTO telegram_sources (channel_id, channel_name, asset, channel_type, enabled, priority) VALUES
    -- Analytics & Alerts (Priority 8)
    ('-1001524675531', 'Glassnode Alerts', 'BTC', 'analytics', TRUE, 8),
    ('-1001157066728', 'Coin Bureau', 'BTC', 'education', TRUE, 8),
    ('-1001382978349', 'The Block', 'BTC', 'news', TRUE, 8),
    -- Analytics (Priority 7)
    ('-1001221690015', 'Messari', 'BTC', 'analytics', TRUE, 7),
    ('-1001487169582', 'Santiment', 'BTC', 'analytics', TRUE, 7),
    ('-1001413670564', 'Cointelegraph', 'BTC', 'news', TRUE, 7),
    ('-1001296358563', 'CoinDesk', 'BTC', 'news', TRUE, 7),
    -- News (Priority 6)
    ('-1001225143879', 'Decrypt', 'BTC', 'news', TRUE, 6),
    ('-1001247280084', 'Bitcoin Archive', 'BTC', 'news', TRUE, 6),
    ('-1001382141207', 'Crypto Panic', 'BTC', 'aggregator', TRUE, 6),
    -- Whale Trackers (Priority 5)
    ('-1001309043988', 'Whale Alert', 'BTC', 'whale', TRUE, 5),
    ('-1001145462707', 'WhaleBot Alerts', 'BTC', 'whale', TRUE, 5),
    -- Market Data (Priority 4)
    ('-1001246846205', 'CryptoQuant Alert', 'BTC', 'analysis', TRUE, 4),
    ('-1001909196609', 'Crypto Price', 'BTC', 'price', TRUE, 4),
    ('-1001193342710', 'Bitcoin', 'BTC', 'news', TRUE, 4),
    ('-1001260161873', 'Binance Futures Liquidations', 'BTC', 'liquidation', TRUE, 4),
    -- Others (Priority 3)
    ('-1001343715577', 'CryptoQuant', 'BTC', 'analysis', TRUE, 3),
    ('-1001234079543', 'BeInCrypto Vietnam', 'BTC', 'news', TRUE, 3)
ON CONFLICT (channel_id) DO NOTHING;

-- Twitter Sources (21 crypto accounts with priority) - matching local database
INSERT INTO twitter_sources (account_id, handle, asset, account_type, enabled, priority) VALUES
    ('whale_alert', '@whale_alert', 'BTC', 'whale_tracker', TRUE, 10),
    ('BitcoinMagazine', '@BitcoinMagazine', 'BTC', 'news', TRUE, 9),
    ('Cointelegraph', '@Cointelegraph', 'BTC', 'news', TRUE, 9),
    ('BitcoinFear', '@BitcoinFear', 'BTC', 'analytics', TRUE, 8),
    ('CoinDesk', '@CoinDesk', 'BTC', 'news', TRUE, 8),
    ('glassnode', '@glassnode', 'BTC', 'analytics', TRUE, 8),
    ('TheBlock__', '@TheBlock__', 'BTC', 'news', TRUE, 7),
    ('CryptoQuant_News', '@CryptoQuant_News', 'BTC', 'analytics', TRUE, 7),
    ('WhaleChart', '@WhaleChart', 'BTC', 'whale_tracker', TRUE, 7),
    ('DocumentingBTC', '@DocumentingBTC', 'BTC', 'education', TRUE, 7),
    ('WuBlockchain', '@WuBlockchain', 'BTC', 'news', TRUE, 6),
    ('100trillionUSD', '@100trillionUSD', 'BTC', 'analyst', TRUE, 6),
    ('WClementeIII', '@WClementeIII', 'BTC', 'analyst', TRUE, 6),
    ('CryptoDonAlt', '@CryptoDonAlt', 'BTC', 'trader', TRUE, 5),
    ('Bybit_Official', '@Bybit_Official', 'BTC', 'exchange', TRUE, 5),
    ('OKX', '@OKX', 'BTC', 'exchange', TRUE, 5),
    ('binance', '@binance', 'BTC', 'exchange', TRUE, 5),
    ('coinbase', '@coinbase', 'BTC', 'exchange', TRUE, 5),
    ('CryptoGodJohn', '@CryptoGodJohn', 'BTC', 'trader', TRUE, 4),
    ('PlanB', '@PlanB', 'BTC', 'analyst', TRUE, 4),
    ('santaboris', '@santaboris', 'BTC', 'analyst', TRUE, 4)
ON CONFLICT (account_id) DO UPDATE SET 
    priority = EXCLUDED.priority,
    enabled = EXCLUDED.enabled;

-- Reddit Sources (15 subreddits) - matching local database
INSERT INTO reddit_sources (subreddit, asset, role, enabled, max_posts_per_run, priority) VALUES
    -- Primary (Priority 5-6)
    ('CryptoTechnology', 'BTC', 'technical', TRUE, 50, 6),
    ('Bitcoin', 'BTC', 'discussion', TRUE, 100, 5),
    ('BitcoinMarkets', 'BTC', 'market', TRUE, 100, 5),
    ('ethfinance', 'ETH', 'discussion', TRUE, 50, 5),
    ('CryptoCurrencyTrading', 'BTC', 'trading', TRUE, 40, 5),
    -- Secondary (Priority 4)
    ('CryptoCurrency', 'BTC', 'discussion', TRUE, 100, 4),
    ('defi', 'BTC', 'discussion', TRUE, 30, 4),
    ('BitcoinMining', 'BTC', 'mining', TRUE, 20, 4),
    -- Others (Priority 2-3)
    ('btc', 'BTC', 'discussion', TRUE, 50, 3),
    ('SatoshiStreetBets', 'BTC', 'trading', TRUE, 30, 3),
    ('CryptoMarkets', 'BTC', 'market', TRUE, 50, 3),
    ('BitcoinBeginners', 'BTC', 'education', TRUE, 30, 3),
    ('Bitcoincash', 'BCH', 'discussion', TRUE, 20, 2),
    ('CryptoMoonShots', 'BTC', 'speculation', TRUE, 20, 2),
    ('altcoin', 'BTC', 'discussion', TRUE, 20, 2)
ON CONFLICT (subreddit) DO NOTHING;

EOSQL

    # Execute migration
    # Find psql binary
    PSQL_BIN=$(which psql 2>/dev/null || find /usr/lib/postgresql -name psql 2>/dev/null | head -1)
    PGPASSWORD="${DB_PASSWORD}" $PSQL_BIN -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -f /tmp/migration.sql
    
    if [ $? -eq 0 ]; then
        log_success "Database migration completed successfully"
        rm -f /tmp/migration.sql
    else
        log_error "Database migration failed"
        exit 1
    fi
}

# ============================================================================
# Step 6: Setup Python Virtual Environment
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
    
    deactivate
    
    # Set ownership
    chown -R ${APP_USER}:${APP_USER} ${APP_DIR}/.venv
    
    log_success "Python environment setup complete"
}

# ============================================================================
# Step 7: Setup Environment Variables
# ============================================================================

setup_env_file() {
    log_info "Setting up environment file..."
    
    ENV_FILE="${APP_DIR}/.env"
    
    if [ -f "${ENV_FILE}" ]; then
        log_warn ".env file already exists, skipping..."
        return
    fi
    
    # Create .env file from example or create new
    if [ -f "${APP_DIR}/.env.example" ]; then
        cp ${APP_DIR}/.env.example ${ENV_FILE}
        log_warn "Copied .env.example to .env - Please update with actual values!"
    else
        cat > ${ENV_FILE} << EOF
# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=sentiment_db
POSTGRES_USER=sentiment_user
POSTGRES_PASSWORD=Cuongnv123456

# =============================================================================
# TELEGRAM CONFIGURATION
# =============================================================================
TELEGRAM_API_ID=YOUR_API_ID
TELEGRAM_API_HASH=YOUR_API_HASH
TELEGRAM_PHONE=+1234567890

# =============================================================================
# TELEGRAM ALERTING (Bot API for sending alerts)
# =============================================================================
TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN
TELEGRAM_CHANNEL_ID=YOUR_CHANNEL_ID

# =============================================================================
# TWITTER PROXY CONFIGURATION (SOCKS5)
# Required for Twitter Syndication API to avoid rate limits
# =============================================================================
TWITTER_PROXY_HOST=1.52.54.227
TWITTER_PROXY_PORT=50099
TWITTER_PROXY_USER=muaproxy694c57898a312
TWITTER_PROXY_PASS=fsejfbalvpjschjc
TWITTER_PROXY_TYPE=socks5

# =============================================================================
# RATE LIMITS
# =============================================================================
TELEGRAM_GLOBAL_RATE_LIMIT=100
TWITTER_GLOBAL_RATE_LIMIT=500
REDDIT_GLOBAL_RATE_LIMIT=500

# =============================================================================
# ENVIRONMENT
# =============================================================================
ENVIRONMENT=production
DEBUG=false
EOF
        log_warn "Created new .env file - Please update with actual values!"
    fi
    
    chown ${APP_USER}:${APP_USER} ${ENV_FILE}
    chmod 600 ${ENV_FILE}
    
    log_success "Environment file created"
}

# ============================================================================
# Step 8: Setup PM2 Ecosystem
# ============================================================================

setup_pm2() {
    log_info "Setting up PM2 ecosystem..."
    
    # Create PM2 ecosystem file for Linux
    cat > ${APP_DIR}/ecosystem.config.js << 'EOF'
module.exports = {
  apps: [
    // ==========================================================================
    // Production Worker - Data Collection from Twitter, Telegram, Reddit
    // ==========================================================================
    {
      name: "sentiment-worker",
      script: ".venv/bin/python",
      args: "production_worker.py",
      cwd: "/opt/SentimentDataSource",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      env: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
      },
      error_file: "./logs/worker-error.log",
      out_file: "./logs/worker-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      merge_logs: true,
      // Restart delay to prevent rapid restarts
      restart_delay: 5000,
      // Max restarts in 1 minute
      max_restarts: 10,
      min_uptime: "30s",
    },

    // ==========================================================================
    // Flask API - Sentiment Analysis (Port 5000)
    // ==========================================================================
    {
      name: "sentiment-api",
      script: ".venv/bin/python",
      args: "-m gunicorn -w 4 -b 0.0.0.0:5000 --timeout 120 api_service:app",
      cwd: "/opt/SentimentDataSource",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "300M",
      env: {
        NODE_ENV: "production",
        FLASK_ENV: "production",
        PYTHONUNBUFFERED: "1",
      },
      error_file: "./logs/api-error.log",
      out_file: "./logs/api-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss Z",
      merge_logs: true,
    },

    // ==========================================================================
    // FastAPI - Social Context API (Port 8000)
    // ==========================================================================
    {
      name: "social-context-api",
      script: ".venv/bin/python",
      args: "-m uvicorn fastapi_social_context:app --host 0.0.0.0 --port 8000 --workers 4 --timeout-keep-alive 30",
      cwd: "/opt/SentimentDataSource",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "300M",
      env: {
        NODE_ENV: "production",
        PYTHONUNBUFFERED: "1",
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
    chown ${APP_USER}:${APP_USER} ${APP_DIR}/ecosystem.config.js
    
    log_success "PM2 ecosystem configured"
}

# ============================================================================
# Step 9: Setup Nginx Reverse Proxy
# ============================================================================

setup_nginx() {
    log_info "Setting up Nginx reverse proxy..."
    
    cat > /etc/nginx/sites-available/sentiment-api << 'EOF'
# Rate Limiting Zone
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;

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
    systemctl enable nginx
    
    log_success "Nginx configured"
}

# ============================================================================
# Step 10: Setup Firewall
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
# Step 11: Start Services
# ============================================================================

start_services() {
    log_info "Starting services..."
    
    cd ${APP_DIR}
    
    # Stop existing PM2 processes if any
    sudo -u ${APP_USER} pm2 delete all 2>/dev/null || true
    
    # Start PM2 as app user
    sudo -u ${APP_USER} pm2 start ecosystem.config.js
    
    # Save PM2 process list
    sudo -u ${APP_USER} pm2 save
    
    # Setup PM2 startup script
    env PATH=$PATH:/usr/bin pm2 startup systemd -u ${APP_USER} --hp /home/${APP_USER}
    
    log_success "Services started"
}

# ============================================================================
# Step 12: Verify Deployment
# ============================================================================

verify_deployment() {
    log_info "Verifying deployment..."
    
    # Wait for services to start
    sleep 5
    
    # Check health endpoint
    if curl -s http://localhost:5000/health | grep -q "healthy"; then
        log_success "Sentiment API is healthy"
    else
        log_warn "Sentiment API health check failed"
    fi
    
    if curl -s http://localhost:8000/health | grep -q "healthy"; then
        log_success "Social Context API is healthy"
    else
        log_warn "Social Context API health check failed"
    fi
    
    # Show PM2 status
    sudo -u ${APP_USER} pm2 list
}

# ============================================================================
# Display Status
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
    echo "Database:"
    echo "  - Host: ${DB_HOST}"
    echo "  - Port: ${DB_PORT}"
    echo "  - Database: ${DB_NAME}"
    echo "  - User: ${DB_USER}"
    echo ""
    echo "Tables Created:"
    echo "  - telegram_sources (10 channels)"
    echo "  - twitter_sources (21 accounts)"
    echo "  - twitter_messages"
    echo "  - reddit_sources"
    echo "  - ingested_messages"
    echo "  - sentiment_results"
    echo "  - aggregated_signals"
    echo "  - alert_history"
    echo "  - risk_indicators"
    echo "  - data_quality_metrics"
    echo ""
    echo "Data Collection Settings:"
    echo "  - Twitter: 5 accounts/batch, 30 min interval, 5s delay/request"
    echo "  - Telegram: 20s interval"
    echo "  - Reddit: 5 min interval"
    echo "  - Proxy: SOCKS5 enabled for Twitter"
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
    echo "  - sudo -u ${APP_USER} pm2 logs sentiment-worker"
    echo "  - sudo -u ${APP_USER} pm2 monit"
    echo "  - sudo -u ${APP_USER} pm2 restart all"
    echo ""
    echo -e "${YELLOW}⚠️  IMPORTANT:${NC}"
    echo "  1. Update ${APP_DIR}/.env with Telegram API credentials"
    echo "  2. Copy telegram session file to VPS"
    echo "  3. Verify proxy connectivity from VPS"
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
    check_app_dir
    
    install_system_deps
    install_nodejs
    create_app_user
    setup_postgresql
    setup_env_file
    load_env
    run_database_migration
    setup_python_env
    setup_pm2
    setup_nginx
    setup_firewall
    start_services
    verify_deployment
    display_status
}

# ============================================================================
# Command Line Options
# ============================================================================

case "$1" in
    --full)
        main
        ;;
    --migrate)
        log_info "Running database migration only..."
        check_root
        check_app_dir
        load_env
        run_database_migration
        log_success "Migration complete"
        ;;
    --update)
        log_info "Updating application..."
        check_root
        check_app_dir
        cd ${APP_DIR}
        git pull origin main
        chown -R ${APP_USER}:${APP_USER} ${APP_DIR}
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
    --db-status)
        log_info "Checking database status..."
        check_app_dir
        load_env
        PSQL_BIN=$(which psql 2>/dev/null || find /usr/lib/postgresql -name psql 2>/dev/null | head -1)
        PGPASSWORD="${DB_PASSWORD}" $PSQL_BIN -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "\dt"
        ;;
    --db-reset)
        log_warn "This will DROP all tables and recreate them!"
        read -p "Are you sure? (y/N): " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            check_root
            check_app_dir
            load_env
            PSQL_BIN=$(which psql 2>/dev/null || find /usr/lib/postgresql -name psql 2>/dev/null | head -1)
            log_info "Dropping existing tables..."
            PGPASSWORD="${DB_PASSWORD}" $PSQL_BIN -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "
                DROP TABLE IF EXISTS data_quality_metrics CASCADE;
                DROP TABLE IF EXISTS risk_indicators CASCADE;
                DROP TABLE IF EXISTS alert_history CASCADE;
                DROP TABLE IF EXISTS aggregated_signals CASCADE;
                DROP TABLE IF EXISTS sentiment_results CASCADE;
                DROP TABLE IF EXISTS ingested_messages CASCADE;
                DROP TABLE IF EXISTS reddit_sources CASCADE;
                DROP TABLE IF EXISTS twitter_sources CASCADE;
                DROP TABLE IF EXISTS telegram_sources CASCADE;
            "
            run_database_migration
            log_success "Database reset complete"
        else
            log_info "Cancelled"
        fi
        ;;
    *)
        echo "Usage: $0 {--full|--migrate|--update|--restart|--status|--logs|--db-status|--db-reset}"
        echo ""
        echo "Options:"
        echo "  --full       Full deployment (first time setup)"
        echo "  --migrate    Run database migration only"
        echo "  --update     Pull latest code and restart services"
        echo "  --restart    Restart all services"
        echo "  --status     Show PM2 process status"
        echo "  --logs       Show PM2 logs"
        echo "  --db-status  Show database tables"
        echo "  --db-reset   Drop all tables and recreate (DANGEROUS!)"
        exit 1
        ;;
esac
