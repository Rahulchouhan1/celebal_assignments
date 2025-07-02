-- Query 4: Find the median of the LAT_N values (rounded to 4 decimal places)
-- This works for both even and odd number of rows using ROW_NUMBER and COUNT
SELECT ROUND(
    LAT_N, 4
)
FROM (
    SELECT LAT_N,
           ROW_NUMBER() OVER (ORDER BY LAT_N) AS row_num,
           COUNT(*) OVER () AS total_rows
    FROM STATION
) AS ordered_lat
WHERE row_num IN (
    FLOOR((total_rows + 1) / 2),
    CEIL((total_rows + 1) / 2)
);
