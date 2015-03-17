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

    # Returns the information of the file stored locally on this machine.
    #
    # params:
    #   file_uuid: the file's uuid
    #   local: the local machine's address
    def is_file_exist_locally(self, file_uuid, local):
        self.cursor.execute('SELECT * FROM FileMap WHERE uuid =? AND server=?', (file_uuid, local))
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
        self.cursor.execute('SELECT DISTINCT * FROM Server WHERE server<>?', (local,))
        results = self.cursor.fetchall()
        retval = []
        for result in results:
            retval.append(result[0])
        return retval

    # Clear all metadata from the database.
    def clear_metadata(self):
        self.cursor.execute('DELETE FROM KnownServer WHERE 1=1')
        self.cursor.execute('DELETE FROM FileMap WHERE 1=1')
        self.cursor.execute('DELETE FROM Stats WHERE 1=1')
        self.conn.commit()

    # Adds a concurrent request of a uuid
    def add_concurrent_request(self, uuid):
        self.cursor.execute('SELECT connections FROM Stats WHERE uuid=?', (uuid,))
        result = self.cursor.fetchone()
        if result is not None:
            current_connection = int(result[0]) + 1
            self.cursor.execute('UPDATE Stats SET connections=? WHERE uuid=?', (current_connection, uuid))
        else:
            self.cursor.execute('INSERT INTO Stats VALUES(?,?)', (uuid, 1))
        self.conn.commit()
        return get_concurrent_request(uuid)


    # Returns the number of concurrent requests for the specified uuid.
    def get_concurrent_request(self, uuid):
        self.cursor.execute('SELECT connections FROM Stats WHERE uuid=?', (uuid,))
        result = self.cursor.fetchone()
        if result is not None:
            return result[0]
        else:
            return None

    # Removes a concurrent request of a uuid from the server.
    def remove_concurrent_request(self, uuid):
        self.cursor.execute('SELECT connections FROM Stats WHERE uuid=?', (uuid,))
        result = self.cursor.fetchone()
        if result is not None:
            current_connection = int(result[0]) - 1
            if current_connect == 0:
                self.cursor.execute('DELETE FROM Stats WHERE uuid=?', (uuid,))
            else:
                self.cursor.execute('UPDATE Stats SET connections=? WHERE uuid=?', (current_connection, uuid))
        self.conn.commit()

    # Add a concurrent request
    def add_concurrent_request(self, uuid):
        self.cursor.execute('SELECT connections FROM Stats WHERE uuid=?', (uuid,))
        current_connection = int(self.cursor.fetchone()[0]) + 1
        self.cursor.execute('UPDATE Stats SET connections=? WHERE uuid=?', (current_connection, uuid))
        self.conn.commit()

    # Returns the closest server to our server.
    def find_closest_server(self):
        self.cursor.execute('SELECT ks1.server FROM KnownServer ks1 WHERE ks1.distance=(SELECT MIN(distance) FROM KnownServer ks2)')

    # Adds the server into the metadata database.
    def update_server(self, server, distance):
        self.cursor.execute('INSERT INTO KnownServer VALUES (?, ?)', (server.strip(), distance))
        self.conn.commit()

    # Adds the server into the metadata database
    #
    # params:
    #   server: the server known to this server that it is online
    def update_servers(self, servers):
        for server in servers:
            self.cursor.execute('INSERT INTO KnownServer VALUES (?, ?)', (server.strip(), -1))
            self.conn.commit()

    # Closes the connection to the database
    def close_connection(self):
        self.conn.close()
