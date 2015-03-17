CREATE TABLE IF NOT EXISTS Stats(uuid text, connections int);
CREATE TABLE IF NOT EXISTS FileMap(uuid text, server text);
CREATE TABLE IF NOT EXISTS KnownServer(server text, distance real);
CREATE INDEX FileMap_UUID ON FileMap(uuid);
