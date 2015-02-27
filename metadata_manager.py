# Manages the metadata
# It stores the metadata in a sqlite database
import sqlite3

class MetadataManager:

    def __init__(self):
        self.conn = sqlite3.connect('metadata.db')
        self.cursor = self.conn.cursor()

    # Returns the server that stores the file excluding the local machine
    #
    # params:
    #   file_uuid: the file's uuid
    #   local: the local machine's address
    def lookup_file(self, file_uuid, local):
        self.cursor.execute('SELECT server FROM FileMap WHERE uuid=? AND server<>?', (file_uuid, local))
        return self.cursor.fetchone()

    # Adds the file uuid with the server stored into the database
    #
    # params:
    #   file_uuid: the file's uuid
    #   server_stored: the hostname of the server
    def update_file_stored(self, file_uuid, server_stored):
        self.cursor.execute('INSERT INTO FileMap VALUES (?, ?)', (file_uuid, server_stored))
        self.conn.commit()

    # Delete the file uuid with the server stored into the database
    #
    # params:
    #   file_uuid: the file's uuid
    #   server_stored: the hostname of the server
    def delete_file_stored(self, file_uuid, server_stored):
        self.cursor.execute('DELETE FROM FileMap WHERE uuid=? AND server=?', (file_uuid, server_stored))
        self.conn.commit()

    # Returns a list of the servers that the server knows about excluding itself
    #
    # params:
    #   local: the local address
    def get_all_server(self, local):
        self.cursor.execute('SELECT * FROM Server WHERE server<>?', (local))
        return self.cursor.fetchall()

    # Adds the server into the metadata database
    #
    # params:
    #   server: the server known to this server that it is online
    def update_server(self, servers):
        for server in servers:
            self.cursor.execute('INSERT INTO Server VALUES (?)', (server,))
            self.conn.commit()
