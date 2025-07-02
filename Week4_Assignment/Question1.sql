-- Query 1: Select distinct city names that start and end with a vowel (case-insensitive)
SELECT DISTINCT CITY
FROM STATION
WHERE LOWER(SUBSTRING(CITY, 1, 1)) IN ('a', 'e', 'i', 'o', 'u')
  AND LOWER(SUBSTRING(CITY, -1, 1)) IN ('a', 'e', 'i', 'o', 'u');
