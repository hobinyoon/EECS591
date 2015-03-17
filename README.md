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

2. **Update all new records (begin from last-aggregated timestamp)**
  ```
  python aggregator/aggregator.py --update
  ```

3. **Update all logs beginning at Unix timestamp**
  ```
  python aggregator/aggregator.py --time 1425917113
  ```

4. **Update all logs beginning at date**
  ```
  python aggregator/aggregator.py --date 2015-03-01
  ```


## Simulation 

### Description

### Usage

1. **Run simulation** 
  ```
  python simulation.py
  ```

