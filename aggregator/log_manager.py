# Python interface for managing aggregated logs

import sqlite3

class LogManager:

    def __init__(self):
        self.conn = sqlite3.connect('aggregated_logs.db')
        with open('aggregator.sql', 'rb') as initialization_file:
          self.conn.executescript(initialization_file.read())
        self.cursor = self.conn.cursor()

    # Adds log entry into database
    #
    # params:
    #   log_entry: tab-separated column values for log 
    def add_log_entry(self, log_entry):
        log_columns = log_entry.split(str="\t")
        self.cursor.execute('INSERT OR REPLACE INTO Log VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                            (log_columns[0], log_columns[1], log_columns[2], log_columns[3],
                             log_columns[4], log_columns[5], log_columns[6], log_columns[7]))
        self.conn.commit()

    # Adds multiple log entries into database
    #
    # params:
    #   log_entries: tab-separated column values for log, one per line
    def add_log_entries(self, log_entries):
        log_entry_lines = log_entries.split(str="\n")
        for log_entry in log_entries:
          add_log_entry(log_entry)

    # Retrieve last timestamp on database
    #
    def last_timestamp(self):
      self.cursor.execute('SELECT timestamp FROM Log ORDER BY timestamp DESC LIMIT 1')
      return self.cursor.fetchone()[0]

    # Closes the connection to the database
    def close_connection(self):
        self.conn.close()