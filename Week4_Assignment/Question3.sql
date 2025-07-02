-- Query 3: Calculate the Euclidean distance between the extreme (min/max) latitudes and longitudes
-- and round the result to 4 decimal places
SELECT ROUND(
    SQRT(POWER(MAX(LAT_N) - MIN(LAT_N), 2) + POWER(MAX(LONG_W) - MIN(LONG_W), 2)),
    4
) AS distance
FROM STATION;
