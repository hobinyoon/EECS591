# EECS591
The repository contains the source for EECS 591 (Distributed Systems) Class Project

## Aggregator

### Description

The aggregator will retrieve log entries from all known servers and store the results in a sqlite database located at `aggregator/aggregated_logs.db`.

### Usage

1. **Update all logs** 
  ```
  python aggregator/aggregator.py
  ```

2. **Update all logs beginning at Unix timestamp**
  ```
  python aggregator/aggregator.py --time 1425917113
  ```

3. **Update all logs beginning at date**
  ```
  python aggregator/aggregator.py --date 2015-03-01
  ```


## Simulation 

### Description

### Before you run the simulation

1. **Install pyipinfodb (an API for transfering ip address to geolocation) and geopy** 
  ```
  pip install git+git://github.com/markmossberg/pyipinfodb.git
  
  pip install geopy
  ```

### Usage

1. **Run simulation** 
  ```
  python simulation_driver.py
  ```

