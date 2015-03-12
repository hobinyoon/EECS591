CREATE TABLE IF NOT EXISTS Client(ip text,
                                  lat real, lng real,
                                  city text, region text, country text,
                                  PRIMARY KEY (ip));