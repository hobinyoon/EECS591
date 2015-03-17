CREATE TABLE Stats(uuid text, connections int);
CREATE TABLE FileMap(uuid text, server text);
CREATE TABLE KnownServer(server text, distance real);
CREATE INDEX FileMap_UUID ON FileMap(uuid);
