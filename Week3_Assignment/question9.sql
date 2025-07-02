-- Query: Calculate the average population of cities in each continent
-- and round down the result using FLOOR

SELECT
    ctry.CONTINENT,                                
    FLOOR(AVG(city.POPULATION)) AS AvgPopulation   
FROM CITY AS city
JOIN COUNTRY AS ctry ON city.COUNTRYCODE = ctry.Code             
GROUP BY ctry.Continent;      