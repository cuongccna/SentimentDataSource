-- Migration: Add last_read_positions table for anti-ban protection
-- This table tracks the last message ID fetched from each source
-- to avoid re-fetching old messages (reduces API calls by 90%+)

CREATE TABLE IF NOT EXISTS last_read_positions (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    last_message_id VARCHAR(255),
    last_message_time TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure one record per platform/source combination
    UNIQUE(platform, source_id)
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_last_read_platform_source 
    ON last_read_positions(platform, source_id);

-- Comment on table
COMMENT ON TABLE last_read_positions IS 
    'Tracks last fetched message ID per source to enable incremental fetching (anti-ban protection)';

COMMENT ON COLUMN last_read_positions.platform IS 
    'Platform name: telegram, twitter, reddit';
    
COMMENT ON COLUMN last_read_positions.source_id IS 
    'Source identifier: channel_id for Telegram, account_id for Twitter, subreddit for Reddit';
    
COMMENT ON COLUMN last_read_positions.last_message_id IS 
    'Last message/post ID that was fetched - use as min_id for next fetch';

-- Example usage:
-- SELECT last_message_id FROM last_read_positions 
-- WHERE platform = 'telegram' AND source_id = '1234567890';

-- UPDATE last_read_positions 
-- SET last_message_id = '999', last_message_time = NOW(), updated_at = NOW()
-- WHERE platform = 'telegram' AND source_id = '1234567890';

-- INSERT INTO last_read_positions (platform, source_id, last_message_id, last_message_time)
-- VALUES ('telegram', '1234567890', '999', NOW())
-- ON CONFLICT (platform, source_id) 
-- DO UPDATE SET last_message_id = EXCLUDED.last_message_id, 
--               last_message_time = EXCLUDED.last_message_time,
--               updated_at = NOW();
