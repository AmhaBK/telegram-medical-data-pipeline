-- A fact table for messages
SELECT
    t1.message_id,
    t1.message_date,
    t1.message_text,
    t1.sender_id,
    t1.has_image_flag,
    t1.image_path,
    t2.channel_name
FROM
    {{ ref('tg_msg_stg') }} AS t1
LEFT JOIN
    {{ ref('dim_channels') }} AS t2 ON t1.channel_name = t2.channel_name