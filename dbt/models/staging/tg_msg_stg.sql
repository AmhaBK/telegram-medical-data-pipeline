-- A staging model to select from the raw data and clean it up
SELECT
    id AS message_id,
    channel_name,
    (message_data ->> 'date')::TIMESTAMP AS message_date,
    (message_data ->> 'text')::TEXT AS message_text,
    (message_data ->> 'sender_id')::BIGINT AS sender_id,
    (message_data ->> 'has_image')::BOOLEAN AS has_image_flag,
    (message_data ->> 'image_path')::TEXT AS image_path,
    scraped_at
FROM
    {{ source('raw', 'raw_telegram_messages') }}