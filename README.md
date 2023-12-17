![image](docs/confluent-logo-300-2.png)

# Overview

Aggregate data check-in/out and basket update events from a smartshop using ksqlDB and Flink on Confluent Cloud.

# Pre-requisites
- User account on [Confluent Cloud](https://www.confluent.io/confluent-cloud/tryfree)
- Docker
- Python +3.8

# Installation (only need to do that once)
```
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
deactivate
```

## Data Generator
Python script `smartshop.py` help:
```
usage: smartshop.py [-h] [--config CONFIG] [--verbose] [--dry-run]

SmartShop Data Generator

options:
  -h, --help       show this help message and exit
  --config CONFIG  Config file (Default: config/localhost.yaml)
  --verbose        Enable verbose logs
  --dry-run        Generate data, but do not produce to Kafka (it will set logs to be verbose)
```

## Start Demo
Start Producer (For configuration files, follow example on `config/example.yaml`):
```
source .venv/bin/activate
python3 smartshop.py --config config/{your_config_file_here}.yaml --verbose
```

You can also try using Confluent Platform + ksqlDB on Docker (Flink pending):

Start Docker Compose:
```
docker build -t flink-sandbox . -f Dockerfile_flink
docker-compose up --force-recreate --always-recreate-deps -V --scale taskmanager=5 -d
```

Wait for pods to be up and running:
```
source .venv/bin/activate
python3 smartshop.py --config config/localhost.yaml --verbose
```

Example of data produced:
```
2023-12-16 21:52:10.238 [INFO]: Started Kafka producer client
2023-12-16 21:52:10.238 [INFO]: Generating SmartShop events
2023-12-16 21:52:10.239 [DEBUG]: [smartshop-checkin] {'ts': 1702763530239, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'client_id': 'User_50', 'status': 1, 'shop_id': 'SHOP_80'}
2023-12-16 21:52:19.281 [DEBUG]: [smartshop-basket] {'ts': 1702763539281, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_51', 'qty': 3, 'unit_price': 62.05}
2023-12-16 21:52:26.308 [DEBUG]: [smartshop-basket] {'ts': 1702763546308, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_87', 'qty': 1, 'unit_price': 75.73}
2023-12-16 21:52:32.333 [DEBUG]: [smartshop-basket] {'ts': 1702763552333, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_64', 'qty': 1, 'unit_price': 15.21}
2023-12-16 21:52:39.358 [DEBUG]: [smartshop-basket] {'ts': 1702763559357, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_51', 'qty': -2, 'unit_price': 62.05}
2023-12-16 21:52:45.377 [DEBUG]: [smartshop-basket] {'ts': 1702763565374, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_93', 'qty': 1, 'unit_price': 82.48}
2023-12-16 21:52:54.397 [DEBUG]: [smartshop-basket] {'ts': 1702763574397, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'sku': 'SKU_57', 'qty': 2, 'unit_price': 50.12}
2023-12-16 21:52:58.407 [DEBUG]: [smartshop-checkout] {'ts': 1702763578406, 'session_id': '978d646d0d8340b9923733ebe22cb952', 'client_id': 'User_50', 'status': -1, 'shop_id': 'SHOP_80'}
2023-12-16 21:52:58.407 [DEBUG]: [smartshop-checkin] {'ts': 1702763578407, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'client_id': 'User_63', 'status': 1, 'shop_id': 'SHOP_53'}
2023-12-16 21:53:03.421 [DEBUG]: [smartshop-basket] {'ts': 1702763583421, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'sku': 'SKU_49', 'qty': 1, 'unit_price': 4.01}
2023-12-16 21:53:08.441 [DEBUG]: [smartshop-basket] {'ts': 1702763588441, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'sku': 'SKU_0', 'qty': 2, 'unit_price': 8.79}
2023-12-16 21:53:16.470 [DEBUG]: [smartshop-basket] {'ts': 1702763596469, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'sku': 'SKU_60', 'qty': 2, 'unit_price': 30.43}
2023-12-16 21:53:23.494 [DEBUG]: [smartshop-basket] {'ts': 1702763603494, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'sku': 'SKU_90', 'qty': 3, 'unit_price': 55.25}
2023-12-16 21:53:29.515 [DEBUG]: [smartshop-checkout] {'ts': 1702763609515, 'session_id': '759c1c73a8e942c0a0344b28bff01be0', 'client_id': 'User_63', 'status': -1, 'shop_id': 'SHOP_53'}
2023-12-16 21:53:29.516 [DEBUG]: [smartshop-checkin] {'ts': 1702763609515, 'session_id': '646660cc080b4e32a0b391914c885702', 'client_id': 'User_11', 'status': 1, 'shop_id': 'SHOP_47'}
2023-12-16 21:53:38.536 [DEBUG]: [smartshop-basket] {'ts': 1702763618536, 'session_id': '646660cc080b4e32a0b391914c885702', 'sku': 'SKU_89', 'qty': 1, 'unit_price': 92.22}
2023-12-16 21:53:48.569 [DEBUG]: [smartshop-basket] {'ts': 1702763628569, 'session_id': '646660cc080b4e32a0b391914c885702', 'sku': 'SKU_38', 'qty': 3, 'unit_price': 90.66}
2023-12-16 21:53:57.609 [DEBUG]: [smartshop-basket] {'ts': 1702763637609, 'session_id': '646660cc080b4e32a0b391914c885702', 'sku': 'SKU_61', 'qty': 2, 'unit_price': 78.01}
2023-12-16 21:54:05.628 [DEBUG]: [smartshop-basket] {'ts': 1702763645628, 'session_id': '646660cc080b4e32a0b391914c885702', 'sku': 'SKU_49', 'qty': 3, 'unit_price': 4.01}
2023-12-16 21:54:11.651 [DEBUG]: [smartshop-checkout] {'ts': 1702763651651, 'session_id': '646660cc080b4e32a0b391914c885702', 'client_id': 'User_11', 'status': -1, 'shop_id': 'SHOP_47'}
^C2023-12-16 21:54:11.651 [INFO]: Signal received, checking all current sessions out
2023-12-16 21:54:11.652 [INFO]: Checkout completed
2023-12-16 21:54:11.653 [INFO]: Flushing Kafka producer
2023-12-16 21:54:11.653 [INFO]: Kafka producer completed, bye bye
```

Access ksqlDB on Confluent Cloud and create the following SQL statements:
```
--------------------------------
CREATE STREAM `smartshop-basket` (
  session_id STRING KEY,
  sku STRING,
  qty INT,
  unit_price DOUBLE,
  ts BIGINT
)
WITH (
  'kafka_topic' = 'smartshop-basket',
  'key_format' = 'KAFKA',
  'value_format' = 'AVRO'
);

------------------------------------------
CREATE TABLE `smartshop-basket-aggregated`
WITH (
  'kafka_topic' = 'smartshop-basket-aggregated',
  'key_format' = 'KAFKA',
  'value_format' = 'AVRO'
) AS
SELECT
  session_id,
  SUM(qty) AS sum_qty,
  SUM(qty * unit_price) AS total_price
FROM `smartshop-basket`
GROUP BY session_id
EMIT CHANGES;

---------------------------------
CREATE TABLE `smartshop-checkout` (
  session_id STRING PRIMARY KEY,
  shop_id STRING,
  client_id STRING,
  ts BIGINT
) WITH (
  'kafka_topic' = 'smartshop-checkout',
  'key_format' = 'KAFKA',
  'value_format' = 'AVRO'
);

---------------------------------------
CREATE TABLE `smartshop-checkout-qty`
WITH (
  'kafka_topic' = 'smartshop-checkout-qty',
  'key_format' = 'KAFKA',
  'value_format' = 'AVRO'
) AS
SELECT
  s.session_id AS session_id,
  s.shop_id,
  s.client_id,
  FROM_UNIXTIME(s.ts) AS ts,
  COALESCE(b.sum_qty, 0) AS sum_qty,
  CAST(b.total_price AS DECIMAL(16, 2)) AS total_price
FROM `smartshop-checkout` s
LEFT JOIN `smartshop-basket-aggregated` b ON s.session_id=b.session_id
EMIT CHANGES;
```

See the results streamed to the Kafka topic `smartshop-checkout-qty`.

Result Table (upon check-out):
```
+----------------------------------+---------+-----------+---------------+---------+-------------+
| session_id                       | shop_id | client_id | ts            | sum_qty | total_price |
+----------------------------------+---------+-----------+---------------+---------+-------------+
| 978d646d0d8340b9923733ebe22cb952 | SHOP_80 | User_50   | 1702763578406 |       6 |      335.71 |
| 759c1c73a8e942c0a0344b28bff01be0 | SHOP_53 | User_63   | 1702763609515 |       8 |      248.20 |
| 646660cc080b4e32a0b391914c885702 | SHOP_47 | User_11   | 1702763651651 |       9 |      532.25 |
+----------------------------------+---------+-----------+---------------+---------+-------------+
```

# External References
Get started with Confluent, for free: https://www.confluent.io/en-gb/get-started/
 - Confluent makes it easy to connect your apps, systems, and entire organization with real-time data flows and processing
 - We provide a solution for data in motion that is cloud native, complete, and available everywhere you need it.

ksqlDB Documentation: https://docs.ksqldb.io/en/latest/

Apache Flink 101: https://developer.confluent.io/courses/apache-flink/intro/

Check out [Confluent's Developer portal](https://developer.confluent.io), it has free courses, documents, articles, blogs, podcasts and so many more content to get you up and running with a fully managed Apache Kafka service.

Disclaimer: I work for Confluent :wink: