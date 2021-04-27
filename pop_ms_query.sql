SELECT
  ms_poc.rand_date("2021-01-01", "2021-01-07") as response_todaysdatetime,
  CAST (ms_poc.rand_range(90000000000000000, 999999999999999999) as STRING) as TransactionID,
  *
FROM ms_poc.ms_data_parquet