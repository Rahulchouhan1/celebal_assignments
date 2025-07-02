-- Query: Get average population across all cities, rounded to nearest integer

SELECT ROUND(AVG(POPULATION),0) AS AvgPopulation
FROM CITY;