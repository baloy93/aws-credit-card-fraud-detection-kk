CREATE DATABASE IF NOT EXISTS transaction_analysis;

CREATE EXTERNAL TABLE IF NOT EXISTS transaction_analysis.cleaned_transactions (
  user_id STRING,
  timestamp STRING,
  amount DOUBLE,
  city STRING,
  country STRING,
  state STRING,
  zipcode STRING,
  device STRING,
  merchant STRING,
  card_type STRING
)
STORED AS PARQUET
LOCATION 's3://cleaned-transactions-bucket/transactions/';

# Top 5 Countries by Transaction Amount

SELECT country, SUM(amount) AS total_amount
FROM transaction_analysis.cleaned_transactions
GROUP BY country
ORDER BY total_amount DESC
LIMIT 5;

#Transactions over R1000
SELECT *
FROM transaction_analysis.cleaned_transactions
WHERE amount > 1000;