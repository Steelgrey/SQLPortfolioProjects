
DROP TABLE IF EXISTS CovidDeaths;
DROP TABLE IF EXISTS CovidVaccination;
DROP DATABASE IF EXISTS CovidData;

-- Create CovidData Database

CREATE DATABASE IF NOT EXISTS CovidData;

-- Create CovidDeaths Table

USE CovidData;
CREATE TABLE IF NOT EXISTS CovidDeaths(
iso_code VARCHAR(50),
continent VARCHAR(50),
location VARCHAR(50),
date DATE,
population BIGINT,
total_cases BIGINT,
new_cases DOUBLE,
new_cases_smoothed DOUBLE,
total_deaths DOUBLE,
new_deaths DOUBLE,
new_deaths_smoothed DOUBLE,
total_cases_per_million DOUBLE,
new_cases_per_million DOUBLE,
new_cases_smoothed_per_million DOUBLE,
total_deaths_per_million DOUBLE,
new_deaths_per_million DOUBLE,
new_deaths_smoothed_per_million DOUBLE,
reproduction_rate DOUBLE,
icu_patients DOUBLE,
icu_patients_per_million DOUBLE,
hosp_patients DOUBLE,
hosp_patients_per_million DOUBLE,
weekly_icu_admissions DOUBLE,
weekly_icu_admissions_per_million DOUBLE,
weekly_hosp_admissions DOUBLE,
weekly_hosp_admissions_per_million DOUBLE);

SET SESSION sql_mode = '';
SET GLOBAL sql_mode= 'NO_ENGINE_SUBSTITUTION';

SHOW VARIABLES LIKE "local_infile";

SET GLOBAL local_infile = 1;

-- Make sure to set "OPT_LOCAL_INFILE=1" for access to files at mySQL connections -> local instance 3306 -> edit connections -> advanced -> other -> OPT_LOCAL_INFILE=1

LOAD DATA LOCAL INFILE '/Users/johnha/Documents/Data Science/Western ASCii Convert/CovidDeaths.csv'
INTO TABLE CovidDeaths
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM CovidDeaths;

-- Create CovidVaccination Table

USE CovidData;
CREATE TABLE IF NOT EXISTS CovidVaccination(
iso_code VARCHAR(50),
continent VARCHAR(50),
location VARCHAR(50),
date DATE,
total_tests DOUBLE,
new_tests DOUBLE,
total_tests_per_thousand DOUBLE,
new_tests_per_thousand DOUBLE,
new_tests_smoothed DOUBLE,
new_tests_smoothed_per_thousand DOUBLE,
positive_rate DOUBLE,
tests_per_case DOUBLE,
tests_units VARCHAR(50),
total_vaccinations DOUBLE,
people_vaccinated DOUBLE,
people_fully_vaccinated DOUBLE,
total_boosters DOUBLE,
new_vaccinations DOUBLE,
new_vaccinations_smoothed DOUBLE,
total_vaccinations_per_hundred DOUBLE,
people_vaccinated_per_hundred DOUBLE,
people_fully_vaccinated_per_hundred DOUBLE,
total_boosters_per_hundred DOUBLE,
new_vaccinations_smoothed_per_million DOUBLE,
new_people_vaccinated_smoothed DOUBLE,
new_people_vaccinated_smoothed_per_hundred DOUBLE,
stringency_index DOUBLE,
population_density DOUBLE,
median_age DOUBLE,
aged_65_older DOUBLE,
aged_70_older DOUBLE,
gdp_per_capita DOUBLE,
extreme_poverty DOUBLE,
cardiovasc_death_rate DOUBLE,
diabetes_prevalence DOUBLE,
female_smokers DOUBLE,
male_smokers DOUBLE,
handwashing_facilities DOUBLE,
hospital_beds_per_thousand DOUBLE,
life_expectancy DOUBLE,
human_development_index DOUBLE,
excess_mortality_cumulative_absolute DOUBLE,
excess_mortality_cumulative DOUBLE,
excess_mortality DOUBLE,
excess_mortality_cumulative_per_million DOUBLE);

SET SESSION sql_mode = '';
SET GLOBAL sql_mode= 'NO_ENGINE_SUBSTITUTION';

SHOW VARIABLES LIKE "local_infile";

SET GLOBAL local_infile = 1;

-- Make sure to set "OPT_LOCAL_INFILE=1" for access to files at mySQL connections -> local instance 3306 -> edit connections -> advanced -> other -> OPT_LOCAL_INFILE=1

LOAD DATA LOCAL INFILE '/Users/johnha/Documents/Data Science/Western ASCii Convert/CovidVaccination.csv'
INTO TABLE CovidVaccination
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM CovidVaccination;



-- Looking at Total Cases vs Total Deaths

SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM CovidDeaths
WHERE location LIKE '%tate%'
ORDER BY 1,2;

-- Looking at Total Cases vs Population
-- Shows what percentage of population got Covid

SELECT location, date, population, total_cases, total_deaths, (total_cases/population)*100 as PercentPopulationInfected
FROM CovidDeaths
WHERE location LIKE '%tate%'
ORDER BY 1,2;

-- Looking at Countries with Highest Infection Rate compared to Population

SELECT location, population, MAX(population), MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS PercentPopulationInfected
FROM CovidDeaths
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC;

-- Showing Countries with Highest Death Count per Population

SELECT location, MAX(total_deaths) as TotalDeathCount
FROM CovidDeaths
WHERE continent != ''
GROUP BY location
ORDER BY TotalDeathCount DESC;

-- Showing places by conntinent

SELECT continent, MAX(total_deaths) as TotalDeathCount
FROM CovidDeaths
WHERE continent != ''
GROUP BY continent
ORDER BY TotalDeathCount DESC;

-- Global Numbers of Death Percentage by each date

SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as DeathPercentage
FROM CovidDeaths
WHERE continent != ''
GROUP BY DATE
ORDER BY 1,2;

-- Global Number of Death Percentage

SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as DeathPercentage
FROM CovidDeaths
WHERE continent != ''
ORDER BY 1,2;



-- Looking at total population vs vaccinations

SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rollingPeopleVaccinated
FROM CovidDeaths dea
JOIN CovidVaccination vac
	ON dea.location = vac.location
    AND dea.date = vac.date
WHERE dea.continent != ''
AND dea.location LIKE '%tate%'
ORDER BY 2,3;


-- Use CTE

WITH PopvsVac (continent, location, date, population, new_vaccinations, rollingPeopleVaccinated)
AS (
	SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rollingPeopleVaccinated
	FROM CovidDeaths dea
	JOIN CovidVaccination vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent != ''
	-- AND dea.location LIKE '%tate%'
	-- ORDER BY 2,3
    )
SELECT *, (rollingPeopleVaccinated/population)*100
FROM PopvsVac;


-- TEMP TABLE

DROP TABLE IF EXISTS PercentPopulationVaccinated;

CREATE TABLE PercentPopulationVaccinated
(
continent VARCHAR(50),
location VARCHAR(50),
date DATE,
population BIGINT,
new_vaccinations DOUBLE,
RollingPeopleVaccinated DOUBLE
);

INSERT INTO PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rollingPeopleVaccinated
	FROM CovidDeaths dea
	JOIN CovidVaccination vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent != '';
	-- AND dea.location LIKE '%tate%'
	-- ORDER BY 2,3

SELECT *, (rollingPeopleVaccinated/population)*100
FROM PercentPopulationVaccinated;

-- creating view to store data for later visualizations

CREATE VIEW PercentPopulationVaccinateds AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
	SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rollingPeopleVaccinated
	FROM CovidDeaths dea
	JOIN CovidVaccination vac
		ON dea.location = vac.location
		AND dea.date = vac.date
	WHERE dea.continent != '';
	-- AND dea.location LIKE '%tate%'
	-- ORDER BY 2,3

SELECT * 
FROM PercentPopulationVaccinateds;





