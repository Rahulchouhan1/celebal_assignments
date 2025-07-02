-- Query: Find how many city names are duplicates

-- Explanation: Total count - unique count = duplicates
SELECT COUNT(CITY) - COUNT(DISTINCT CITY) AS duplicates_city_count  FROM STATION;
