from google.cloud import bigquery
from google.cloud import storage
import datetime

# Initialize BigQuery client
client = bigquery.Client()

# Define your query
query = """
INSERT INTO ga4_demo.landing_page
WITH t1 AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    device.category AS category,
    traffic_source.medium AS medium,
    traffic_source.source AS source,
    device.operating_system AS operating_system,
    (
      SELECT param.value.string_value
      FROM UNNEST(event_params) AS param
      WHERE param.key = 'page_location'
    ) AS landing_page,
    user_pseudo_id AS user,
    CONCAT(
      (
        SELECT param.value.int_value
        FROM UNNEST(event_params) AS param
        WHERE param.key = 'ga_session_id'
      ),
      '_', user_pseudo_id
    ) AS session_user_uid
  FROM
    `floweraura-394511.analytics_276418752.events_*`
  WHERE
    event_name = 'session_start'
    AND _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),
t2 AS (
  SELECT
    CONCAT(
      (
        SELECT param.value.int_value
        FROM UNNEST(event_params) AS param
        WHERE param.key = 'ga_session_id'
      ),
      '_', user_pseudo_id
    ) AS purchase_user_uid,
    COUNT(CASE WHEN event_name = 'purchase' THEN 1 ELSE NULL END) AS purchase
  FROM
    `floweraura-394511.analytics_276418752.events_*`
  WHERE
    event_name = 'purchase'
    AND _table_suffix = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  GROUP BY
    1
),
t3 AS (
  SELECT
    t1.*,
    t2.purchase
  FROM
    t1
  LEFT JOIN
    t2
  ON
    t1.session_user_uid = t2.purchase_user_uid
)
SELECT
  t3.date,
  t3.category,
  t3.medium,
  t3.source,
  t3.operating_system,
  IF(
    ARRAY_LENGTH(SPLIT(t3.landing_page, '-')) >= 1,
    SPLIT(t3.landing_page, '?')[OFFSET(0)],
    t3.landing_page
  ) AS landing_page,
  COUNT(DISTINCT t3.user) AS user,
  COUNT(DISTINCT t3.session_user_uid) AS sessions,
  SUM(t3.purchase) AS purchase
FROM
  t3
GROUP BY
  1, 2, 3, 4, 5, 6;
"""

# Set the job configuration to run the query
job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = False  # Use standard SQL

# Run the query
query_job = client.query(query, job_config=job_config)

# Wait for the job to complete
query_job.result()

print(f"Query completed successfully at {datetime.datetime.now()}")


