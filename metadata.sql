CREATE TABLE IF NOT EXISTS Connections(uuid text, requestId text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS FileMap(uuid text, server text);
CREATE TABLE IF NOT EXISTS KnownServer(server text, distance real);
CREATE INDEX FileMap_UUID ON FileMap(uuid);
