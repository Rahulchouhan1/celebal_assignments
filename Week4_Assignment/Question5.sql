-- Query 5: List the names of cities located in the continent 'Africa'
SELECT city.NAME 
FROM CITy city
JOIN COUNTRY cntry
ON city.COUNTRYCODE = cntry.CODE
WHERE cntry.CONTINENT = "Africa";
