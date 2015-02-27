# Manages the metadata
# It stores the metadata in a sqlite database
import sqlite3

class MetadataManager:

    def __init__(self):
        self.conn = sqlite3.connect('metadata.db')
        self.cursor = self.conn.cursor()

    def test(self):
        print 'hello!'
        self.conn.cursor()

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

    # Adds the server into the metadata database
    #
    # params:
    #   server: the server known to this server that it is online
    def update_server(self, server):
        self.cursor.execute('INSERT INTO Server VALUES (?)', (server))
        self.conn.commit()
