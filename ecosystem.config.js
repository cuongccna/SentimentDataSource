module.exports = {
  apps: [
    // ===================================================================
    // PRODUCTION WORKER - Thu thập dữ liệu từ Twitter, Telegram, Reddit
    // PostgreSQL persistence + real collectors
    // ===================================================================
    {
      name: "sentiment-worker",
      script: ".venv/Scripts/python.exe",
      args: "production_worker.py",
      cwd: "D:/projects/SentimentData/SentimentDataSource",
      instances: 1,
      exec_mode: "fork",
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

    // ===================================================================
    // FLASK API - Sentiment Analysis API (Port 5000)
    // Endpoint: POST /api/v1/sentiment/analyze
    // ===================================================================
    {
      name: "sentiment-api",
      script: ".venv/Scripts/python.exe",
      args: "-m gunicorn -w 4 -b 0.0.0.0:5000 api_service:app",
      cwd: "D:/projects/SentimentData/SentimentDataSource",
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

    // ===================================================================
    // FASTAPI - Social Context API (Port 8000)
    // Endpoint: POST /api/v1/social/context
    // ===================================================================
    {
      name: "social-context-api",
      script: ".venv/Scripts/python.exe",
      args: "-m uvicorn fastapi_social_context:app --host 0.0.0.0 --port 8000 --workers 4",
      cwd: "D:/projects/SentimentData/SentimentDataSource",
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
