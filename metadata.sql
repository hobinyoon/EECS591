CREATE TABLE FileMap(uuid text, server text);
CREATE TABLE Server(server text);
CREATE INDEX FileMap_UUID ON FileMap(uuid);
