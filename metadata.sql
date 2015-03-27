CREATE TABLE IF NOT EXISTS Connections(uuid text, requestId text);
CREATE TABLE IF NOT EXISTS FileMap(uuid text, server text, file_size int, PRIMARY KEY (uuid, server));
CREATE TABLE IF NOT EXISTS KnownServer(server text, distance real);
CREATE INDEX FileMap_UUID ON FileMap(uuid);
