-- This query retrieves two rows:
-- 1) city with shortest name
-- 2) city with longest name
-- In case of ties, the lexicographically first city is chosen.


-- city with shortest name
SELECT 
    CITY, 
    LENGTH(CITY)  AS city_length 
FROM STATION
ORDER BY LENGTH(CITY) ASC, CITY
LIMIT 1;

-- 2) city with longest name

SELECT
    CITY, 
    LENGTH(CITY)  AS city_length 
FROM STATION
ORDER BY LENGTH(CITY) DESC, CITY
LIMIT 1;
