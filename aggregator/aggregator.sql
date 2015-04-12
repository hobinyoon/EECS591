CREATE TABLE IF NOT EXISTS Log(timestamp integer,
                               uuid text,
                               source_entity text,
                               source_uuid text,
                               destination_entity text,
                               request_type text,
                               status integer,
                               response_size integer);
CREATE INDEX IF NOT EXISTS Log_Timestamp ON Log(timestamp);
CREATE INDEX IF NOT EXISTS Log_UUID ON Log(UUID);
