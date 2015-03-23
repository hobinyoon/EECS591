# Python interface for managing aggregated logs

import sqlite3
import time
import os

class LogManager:

  def __init__(self):
    db_file = os.path.join(os.path.dirname(__file__), 'aggregated_logs.db')
    self.conn = sqlite3.connect(db_file)
    sql_file = os.path.join(os.path.dirname(__file__), 'aggregator.sql')
    with open(sql_file, 'rb') as initialization_file:
      self.conn.executescript(initialization_file.read())
    self.cursor = self.conn.cursor()

  # Adds log entry into database
  #
  # params:
  #   log_entry: tab-separated column values for log
  def add_log_entry(self, log_entry):
    log_columns = log_entry.split("\t")
    if len(log_columns) == 7:
      self.cursor.execute('INSERT OR REPLACE INTO Log VALUES (?, ?, ?, ?, ?, ?, ?)',
                          (log_columns[0], log_columns[1], log_columns[2], log_columns[3],
                           log_columns[4], log_columns[5], log_columns[6]))
      self.conn.commit()

  # Adds multiple log entries into database
  #
  # params:
  #   log_entries: tab-separated column values for log, one per line
  def add_log_entries(self, log_entries):
    log_entry_lines = log_entries.split("\n")
    for log_entry in log_entry_lines:
      self.add_log_entry(log_entry)

  # Retrieve last timestamp on database
  #
  def last_timestamp(self, destination_entity):
    self.cursor.execute('SELECT timestamp FROM Log WHERE destination_entity = ? ORDER BY timestamp DESC LIMIT 1', (destination_entity,))
    result = self.cursor.fetchone()
    if result is None:
      return None
    return result[0]

  # Retrieve log entries in a specified time period
  #
  # params:
  #   start_timestamp: returned logs start from this integer timestamp
  #   end_timestamp: returned logs end by this integer timestamp
  def get_log_entries(self, start_timestamp = 0, end_timestamp = None):
    if end_timestamp is None:
      end_timestamp = int(time.time())

    self.cursor.execute('SELECT * FROM Log WHERE request_type = \'READ\' AND timestamp >= ? AND timestamp <= ?', (start_timestamp, int(end_timestamp)))
    return self.cursor.fetchall()

  # Closes the connection to the database
  def __del__(self):
      self.conn.close()
