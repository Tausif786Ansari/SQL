CREATE DATABASE IF NOT EXISTS EnergyDB;
USE EnergyDB;

CREATE TABLE IF NOT EXISTS CountryTB(
	Country VARCHAR(100),
    CID VARCHAR(10) UNIQUE,
    PRIMARY KEY (Country)
);

CREATE TABLE IF NOT EXISTS EmissionTB(
	Country VARCHAR(100),
    Energy_Type VARCHAR(50),
    Years INT,
    Emission INT NULL,
    PerCapita_Emission DOUBLE(10,8),
    FOREIGN KEY (Country) REFERENCES CountryTB(Country)
);

CREATE TABLE IF NOT EXISTS PopulationTB (
    Country VARCHAR(100),
    Years INT,
    C_Value DOUBLE NULL,
    FOREIGN KEY (Country) REFERENCES CountryTB(Country)
);

CREATE TABLE IF NOT EXISTS ProductionTB (
    Country VARCHAR(100),
    Energy VARCHAR(50),
    Years INT,
    Production DOUBLE NULL, 
    FOREIGN KEY (Country) REFERENCES CountryTB(Country)
);

CREATE TABLE IF NOT EXISTS GDPTB (
    Country VARCHAR(100),
    Years INT,
    C_Value DOUBLE,
    FOREIGN KEY (Country) REFERENCES CountryTB(Country)
);

CREATE TABLE IF NOT EXISTS ConsumptionTB (
    Country VARCHAR(100),
    Energy VARCHAR(50),
    Years INT,
    Consumption DOUBLE NULL,
    FOREIGN KEY (Country) REFERENCES CountryTB(Country)
);
SHOW VARIABLES LIKE 'secure_file_priv'; -- gives the folder location that the nysql can read;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/emission_3.csv"
INTO TABLE EmissionTB
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Country, Energy_Type, Years, @Emission, PerCapita_Emission)
SET Emission = NULLIF(@Emission, '');

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/population_3.csv"
INTO TABLE PopulationTB
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Country,Years, @C_Value)
SET C_Value = NULLIF(TRIM(@C_Value),'');

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/production_3.csv"
INTO TABLE ProductionTB
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Country,Energy,Years, @Production)
SET Production = NULLIF(TRIM(@Production),'');

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/gdp_3.csv"
INTO TABLE GDPTB
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/consum_3.csv"
INTO TABLE ConsumptionTB
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Country,Energy,Years, @Consumption)
SET Consumption = NULLIF(NULLIF(TRIM(@Consumption),''),'NaN');

-- 1)What is the total emission per country for the most recent year available?
SELECT ET.Country,SUM(Emission) AS TOTALEMISSION FROM EmissionTB AS ET
WHERE Years=(SELECT MAX(Years) FROM EmissionTB)
GROUP BY ET.Country
ORDER BY TOTALEMISSION DESC;

-- 2)What are the top 5 countries by GDP in the most recent year?
SELECT Country,SUM(C_Value) AS GDP FROM GDPTB
WHERE Years=(SELECT MAX(Years) FROM GDPTB)
GROUP BY Country
ORDER BY (GDP) DESC
LIMIT 5;

-- 3)Compare energy production and consumption by country and year.
SELECT P.Country,P.Years,P.Energy,P.Production,C.Consumption,(P.Production - C.Consumption) AS Difference FROM ProductionTB P
INNER JOIN ConsumptionTB C ON P.Country = C.Country
    AND P.Energy = C.Energy
    AND P.Years = C.Years
ORDER BY Difference DESC;

-- 4)Which energy types contribute most to emissions across all countries?
SELECT Energy_Type ,SUM(Emission) AS TOTALEMISSION FROM EmissionTB
GROUP BY Energy_Type
ORDER BY TOTALEMISSION DESC;

-- 5)How have global emissions changed year over year?
SELECT Years,SUM(Emission) AS EMISSION,
	SUM(Emission)-LAG(SUM(Emission)) OVER(ORDER BY Years) AS YEARLYCHANGE,
    ROUND((SUM(Emission) - LAG(SUM(Emission)) OVER (ORDER BY Years)) / LAG(SUM(Emission)) OVER (ORDER BY Years) * 100, 2) 
        AS Percent_Change FROM EmissionTB AS E
GROUP BY Years;

-- 6)What is the trend in GDP for each country over the given years?
SELECT Country,Years,SUM(C_Value) AS GDP,
    LAG(SUM(C_Value)) OVER (PARTITION BY Country ORDER BY Years) AS PREV_GDP,
    ROUND(SUM(C_Value) - LAG(SUM(C_Value)) OVER (PARTITION BY Country ORDER BY Years),4) AS RATE
FROM GDPTB
GROUP BY Country, Years
ORDER BY Country, Years;

-- 7)How has population growth affected total emissions in each country?
WITH YearlyEmission AS (SELECT Country,Years,
        SUM(Emission) AS TotalEmission FROM EmissionTB
		GROUP BY Country, Years
)
SELECT P.Country,P.Years,P.C_Value,YE.TotalEmission,
    LAG(P.C_Value) OVER (PARTITION BY P.Country ORDER BY P.Years) AS PrevPopulation,
    LAG(YE.TotalEmission) OVER (PARTITION BY P.Country ORDER BY P.Years) AS PrevEmission,
    ROUND((P.C_Value - LAG(P.C_Value) OVER (PARTITION BY P.Country ORDER BY P.Years)) 
          / LAG(P.C_Value) OVER (PARTITION BY P.Country ORDER BY P.Years) * 100, 2) AS PopGrowthPercent,
    ROUND((YE.TotalEmission - LAG(YE.TotalEmission) OVER (PARTITION BY P.Country ORDER BY P.Years)) 
          / LAG(YE.TotalEmission) OVER (PARTITION BY P.Country ORDER BY P.Years) * 100, 2) AS EmissionGrowthPercent
FROM PopulationTB P
INNER JOIN YearlyEmission YE
    ON P.Country = YE.Country
    AND P.Years = YE.Years
ORDER BY P.Country, P.Years;

-- 8)Has energy consumption increased or decreased over the years for major economies?
WITH BIGECONOMY AS (SELECT Country,Years,ROUND(SUM(C_Value),4) AS GDP
	FROM GDPTB
    GROUP BY Country,Years
    ORDER BY Country,Years ASC
),
EnergyConsumption AS (SELECT CT.Country,CT.Years,BT.GDP,SUM(Consumption) AS USES FROM ConsumptionTB AS CT
 	INNER JOIN BIGECONOMY AS BT ON CT.Country = BT.Country
		AND CT.Years = BT.Years
	GROUP BY CT.Country,CT.Years
)
SELECT *,LAG(EC.GDP) OVER(PARTITION BY EC.Country ORDER BY EC.Years) AS PRE_GDP,
		 ROUND((EC.GDP - LAG(EC.GDP) OVER(PARTITION BY EC.Country ORDER BY EC.Years))/
         LAG(EC.GDP) OVER(PARTITION BY EC.Country ORDER BY EC.Years) * 100,4) AS RATE_GDP,
         LAG(EC.USES) OVER(PARTITION BY EC.Country ORDER BY EC.Years) AS PRE_USES,
         ROUND((EC.USES - LAG(EC.USES) OVER(PARTITION BY EC.Country ORDER BY EC.Years))/
         LAG(EC.USES) OVER(PARTITION BY EC.Country ORDER BY EC.Years) * 100,4) AS RATE_USES
    FROM EnergyConsumption AS EC;

-- 9)What is the average yearly change in emissions per capita for each country?
WITH EmissionPerCapita AS (SELECT e.Country,e.Years,
        ROUND(SUM(e.Emission) / p.C_Value, 6) AS EmissionPerCapita
		FROM EmissionTB e
		INNER JOIN PopulationTB p 
			ON e.Country = p.Country
			AND e.Years = p.Years
    GROUP BY e.Country, e.Years, p.C_Value
),
YearlyChange AS (SELECT Country,Years,EmissionPerCapita,
        ROUND(EmissionPerCapita - LAG(EmissionPerCapita) OVER(PARTITION BY Country ORDER BY Years),6) 
        AS ChangePerYear
    FROM EmissionPerCapita
)
SELECT Country,ROUND(AVG(ChangePerYear), 6) AS AvgYearlyChangePerCapita
	FROM YearlyChange
	WHERE ChangePerYear IS NOT NULL
	GROUP BY Country
	ORDER BY AvgYearlyChangePerCapita DESC;

-- 10)What is the emission-to-GDP ratio for each country by year?
WITH YearlyEmission AS (SELECT Country,Years,SUM(Emission) AS TOTAL_EMISSION FROM EmissionTB
	GROUP BY Country, Years
),    
EmissionRatio AS (SELECT G.Country,G.Years,G.C_Value AS GDP,YE.TOTAL_EMISSION, 
		IF(C_Value <> 0,ROUND((YE.TOTAL_EMISSION/G.C_Value),4),0) AS RATIO
        FROM GDPTB AS G
INNER JOIN YearlyEmission AS YE ON G.Country = YE.Country
	AND G.Years = YE.Years
    ORDER BY Country,Years
)
SELECT * FROM EmissionRatio;

-- 11)What is the energy consumption per capita for each country over the last decade?
WITH POP_YEAR AS (SELECT Country,Years,SUM(C_Value) AS POPULATION FROM PopulationTB
	GROUP BY Country,Years
),
CONSUMP AS (SELECT Country,Years, ROUND(SUM(Consumption),4) AS ENERGY_COM FROM ConsumptionTB
	GROUP BY Country,Years),
DIFFSUM AS (SELECT PY.Country,PY.Years,PY.POPULATION,C.ENERGY_COM FROM POP_YEAR AS PY
	INNER JOIN CONSUMP AS C ON PY.Country = C.Country
		AND PY.Years = C.Years
	ORDER BY PY.Country,PY.Years
)
SELECT *,ROUND((ENERGY_COM/POPULATION),8) AS PER_CAPITA_CONSUMPTION FROM DIFFSUM;

-- 12)How does energy production per capita vary across countries?
WITH ENERGY_PRO AS (SELECT Country,Years,ROUND(SUM(Production),8) AS TOTAL_PRO FROM ProductionTB
	GROUP BY Country,Years
),
POP_YEAR AS (SELECT Country,Years,SUM(C_Value) AS POPULATION FROM PopulationTB
	GROUP BY Country,Years
),
NEW_TABLE AS (SELECT EP.Country,EP.Years,PY.POPULATION,EP.TOTAL_PRO FROM ENERGY_PRO AS EP
	INNER JOIN POP_YEAR AS PY ON EP.Country = PY.Country
		AND EP.Years = PY.Years
        ORDER BY PY.Country,PY.Years
)
SELECT * ,ROUND((TOTAL_PRO/POPULATION),8) AS RATIO FROM NEW_TABLE;

-- 13)Which countries have the highest energy consumption relative to GDP?
WITH ENERGY_COM AS (SELECT Country,Years,ROUND(SUM(Consumption),8) AS ENERGY_COM FROM ConsumptionTB
	GROUP BY Country,Years
),
CON_GDP AS (SELECT Country,Years,ROUND(SUM(C_Value),8) AS GDP FROM GDPTB
	GROUP BY Country,Years
),
NEW_TABLE AS (SELECT EC.Country,EC.Years,CG.GDP,EC.ENERGY_COM FROM ENERGY_COM AS EC
	INNER JOIN CON_GDP AS CG ON EC.Country = CG.Country
		AND EC.Years = CG.Years
        ORDER BY EC.Country,EC.Years
)
SELECT *,ROUND((ENERGY_COM/GDP),8) AS RATIO FROM NEW_TABLE
ORDER BY RATIO DESC; -- he higher this ratio, the more energy the country consumes for each unit of GDP — meaning it’s less energy-efficient.

-- 14)What is the correlation between GDP growth and energy production growth?
WITH GDP_GROWTH AS (SELECT Country,Years,
		ROUND((SUM(C_Value) - LAG(SUM(C_Value)) OVER(PARTITION BY Country ORDER BY Years))
			/ NULLIF(LAG(SUM(C_Value)) OVER(PARTITION BY Country ORDER BY Years), 0) * 100,4)
		AS GDP_GROWTH_RATE FROM GDPTB
		GROUP BY Country, Years
),
PROD_GROWTH AS (SELECT Country,Years,
        ROUND((SUM(Production) - LAG(SUM(Production)) OVER(PARTITION BY Country ORDER BY Years))
            / NULLIF(LAG(SUM(Production)) OVER(PARTITION BY Country ORDER BY Years), 0) * 100,4)
		AS PROD_GROWTH_RATE FROM ProductionTB
		GROUP BY Country, Years
),
COMBINED AS (SELECT G.Country, G.Years, G.GDP_GROWTH_RATE, P.PROD_GROWTH_RATE
    FROM GDP_GROWTH G
    INNER JOIN PROD_GROWTH P ON G.Country = P.Country 
		AND G.Years = P.Years
)
SELECT Country, CORR(GDP_GROWTH_RATE, PROD_GROWTH_RATE) AS GDP_PROD_CORR FROM COMBINED
GROUP BY Country;

-- 15)What are the top 10 countries by population and how do their emissions compare?
WITH POP_CON AS (SELECT Country,SUM(C_Value) AS POPULATION FROM PopulationTB
	GROUP BY Country
    ORDER BY POPULATION DESC
    LIMIT 10
),
EMI_CON AS (SELECT Country,SUM(Emission) AS EMISSION FROM EmissionTB
	GROUP BY Country
),
NEW_TABLE AS (SELECT PC.Country,PC.POPULATION,EC.EMISSION FROM EMI_CON AS EC
	INNER JOIN POP_CON AS PC ON EC.Country = PC.Country
)
SELECT *,ROUND((POPULATION/EMISSION),8) AS PER_UNIT_EMISSION,
		 ROUND((EMISSION/POPULATION),8) AS EMISSION_PER_CAPITA
         FROM NEW_TABLE;
         
-- 16)Which countries have improved (reduced) their per capita emissions the most over the last decade?
WITH POP_CON AS (SELECT Country,Years,SUM(C_Value) AS POPULATION FROM PopulationTB
	GROUP BY Country,Years
),
EMI_CON AS (SELECT Country,Years,SUM(Emission) AS EMISSION FROM EmissionTB
	GROUP BY Country,Years
),
NEW_TABLE AS (SELECT PC.Country,PC.Years,PC.POPULATION,EC.EMISSION FROM POP_CON AS PC
	INNER JOIN EMI_CON AS EC ON PC.Country = EC.Country
		AND PC.Years = EC.Years
        ORDER BY Country,Years
)
SELECT *,ROUND((EMISSION/POPULATION),8) AS PER_CAPITA_EMISSION FROM NEW_TABLE
	ORDER BY Country,Years,PER_CAPITA_EMISSION DESC;

-- 17)What is the global share (%) of emissions by country?
SELECT Country,SUM(Emission) AS EMISSION,
	ROUND((SUM(Emission)/(SELECT SUM(Emission) FROM EmissionTB))*100,6) AS SHARES
	FROM EmissionTB
	GROUP BY Country
    ORDER BY SHARES DESC
    LIMIT 10;
    
-- 18)What is the global average GDP, emission, and population by year?
SELECT Years,Country,
    ROUND(AVG(GDP), 2) AS AVG_GDP,
    ROUND(AVG(EMISSION), 2) AS AVG_EMISSION,
    ROUND(AVG(POPULATION), 2) AS AVG_POPULATION
	FROM (SELECT G.Years,G.Country,
        SUM(G.C_Value) AS GDP,
        SUM(E.Emission) AS EMISSION,
        SUM(P.C_Value) AS POPULATION
		FROM GDPTB AS G
			INNER JOIN EmissionTB AS E 
				ON G.Country = E.Country AND G.Years = E.Years
			INNER JOIN PopulationTB AS P 
				ON G.Country = P.Country AND G.Years = P.Years
			GROUP BY G.Years, G.Country
) 	AS YearlyCountryData
GROUP BY Years,Country
ORDER BY Years;

SELECT G.Years,G.Country,
        SUM(G.C_Value) AS GDP,
        SUM(E.Emission) AS EMISSION,
        SUM(P.C_Value) AS POPULATION
		FROM GDPTB AS G
			INNER JOIN EmissionTB AS E 
				ON G.Country = E.Country AND G.Years = E.Years
			INNER JOIN PopulationTB AS P 
				ON G.Country = P.Country AND G.Years = P.Years
			GROUP BY G.Years, G.Country;
				


