WITH ttl AS (
  WITH total AS (
    WITH cntev AS (
      SELECT COUNT(event_type) AS num_of_events,
                                  event_type
      FROM events
      GROUP BY event_type
      HAVING COUNT(event_type) > 1
    )
    
    SELECT *
    FROM cntev
    LEFT JOIN (SELECT event_type as et,
                      value,
                      time,
                      ROW_NUMBER () OVER(PARTITION BY event_type ORDER BY time DESC) as latest_two
               FROM events) as basic
    ON basic.et = cntev.event_type
    ORDER BY event_type
  )

  SELECT event_type,
         (- value + LAG(value) OVER(PARTITION BY event_type)) as value
  FROM total
  WHERE latest_two <= 2
)
SELECT *
FROM ttl
WHERE value is not null
