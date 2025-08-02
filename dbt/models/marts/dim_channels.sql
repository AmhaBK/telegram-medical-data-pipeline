-- A dimension table for channels
SELECT DISTINCT
    channel_name
FROM
    {{ ref('tg_msg_stg') }}