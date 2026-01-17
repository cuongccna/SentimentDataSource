"""
Social Sentiment Analysis API for Crypto BotTrading
Purpose: RISK CONTROL ONLY - Not for generating trading signals

Endpoint: POST /api/v1/sentiment/analyze
"""

from datetime import datetime, timezone
from typing import Optional
from flask import Flask, request, jsonify, Response
import json

from sentiment_pipeline import process_record, validate_record


app = Flask(__name__)


def get_current_timestamp() -> str:
    """Get current timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_request_structure(data: dict) -> tuple[bool, Optional[str]]:
    """
    Validate request structure.
    Returns (is_valid, error_message)
    """
    if data is None:
        return False, "Request body must be valid JSON"
    
    if not isinstance(data, dict):
        return False, "Request body must be a JSON object"
    
    if "records" not in data:
        return False, "Missing required field: 'records'"
    
    if not isinstance(data["records"], list):
        return False, "'records' must be an array"
    
    if len(data["records"]) == 0:
        return False, "'records' array cannot be empty"
    
    return True, None


def build_meta(asset: str, received: int, processed: int, dropped: int) -> dict:
    """Build meta object for response."""
    return {
        "asset": asset,
        "record_received": received,
        "record_processed": processed,
        "record_dropped": dropped,
        "timestamp": get_current_timestamp()
    }


def build_success_response(results: list, received: int, dropped: int) -> dict:
    """Build success response with processed results."""
    # Determine primary asset from results
    asset = "BTC"  # Default
    if results:
        asset = results[0].get("asset", "BTC")
    
    return {
        "meta": build_meta(asset, received, len(results), dropped),
        "results": results
    }


def build_fallback_response(received: int, dropped: int, asset: str = "BTC") -> dict:
    """
    Build fallback response when all records are dropped.
    MANDATORY structure as per specification.
    """
    return {
        "meta": build_meta(asset, received, 0, dropped),
        "results": [],
        "risk_flag": {
            "sentiment_unavailable": True,
            "action": "BLOCK_TRADING"
        }
    }


def build_error_response(error: str, status_code: int) -> tuple[Response, int]:
    """Build error response."""
    response = {
        "error": {
            "message": error,
            "status_code": status_code,
            "timestamp": get_current_timestamp()
        }
    }
    return jsonify(response), status_code


@app.route("/api/v1/sentiment/analyze", methods=["POST"])
def analyze_sentiment():
    """
    POST /api/v1/sentiment/analyze
    
    Analyze social sentiment for crypto trading risk control.
    
    HTTP Status Codes:
    - 200: At least one valid record processed
    - 400: Invalid request structure
    - 422: Records array empty OR all records dropped
    - 500: Internal error
    """
    try:
        # Validate Content-Type header
        content_type = request.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            return build_error_response(
                "Content-Type must be application/json",
                400
            )
        
        # Parse JSON body
        try:
            data = request.get_json(force=False, silent=False)
        except Exception:
            return build_error_response(
                "Invalid JSON in request body",
                400
            )
        
        # Validate request structure
        is_valid, error_msg = validate_request_structure(data)
        if not is_valid:
            # Empty array is 422, other structure errors are 400
            if error_msg == "'records' array cannot be empty":
                return build_error_response(error_msg, 422)
            return build_error_response(error_msg, 400)
        
        records = data["records"]
        received_count = len(records)
        
        # Process each record using the EXACT Sentiment Pipeline from Step 2
        results = []
        dropped_count = 0
        first_valid_asset = "BTC"  # Default asset for fallback
        
        for record in records:
            # Try to capture asset for fallback response even if record is invalid
            if isinstance(record, dict) and "asset" in record:
                if first_valid_asset == "BTC" and record["asset"]:
                    first_valid_asset = record["asset"]
            
            # Process record using sentiment pipeline
            result = process_record(record)
            
            if result is None:
                # Record was invalid - dropped
                dropped_count += 1
            else:
                results.append(result)
        
        processed_count = len(results)
        
        # Check if all records were dropped
        if processed_count == 0:
            # Return fallback response with 422
            fallback = build_fallback_response(
                received_count,
                dropped_count,
                first_valid_asset
            )
            return jsonify(fallback), 422
        
        # Return success response with 200
        success = build_success_response(results, received_count, dropped_count)
        return jsonify(success), 200
        
    except Exception as e:
        # Internal error - DO NOT return partial results
        return build_error_response(
            "Internal server error",
            500
        )


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "service": "sentiment-analysis-api",
        "version": "1.0.0",
        "timestamp": get_current_timestamp()
    }), 200


@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    return build_error_response("Endpoint not found", 404)


@app.errorhandler(405)
def method_not_allowed(e):
    """Handle 405 errors."""
    return build_error_response("Method not allowed", 405)


@app.errorhandler(500)
def internal_error(e):
    """Handle 500 errors."""
    return build_error_response("Internal server error", 500)


# Ensure JSON responses have correct Content-Type
@app.after_request
def set_response_headers(response):
    """Set response headers for all responses."""
    if response.content_type == "application/json":
        response.headers["Content-Type"] = "application/json"
    return response


if __name__ == "__main__":
    # Run development server
    # For production, use gunicorn or similar WSGI server
    app.run(host="0.0.0.0", port=5000, debug=False)
