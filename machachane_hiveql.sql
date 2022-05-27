-- Databricks notebook source
-- DBTITLE 1,Variables
/* Files directories */
SET ctFile = '/FileStore/tables/clinicaltrial_2021';
SET mshFile = '/FileStore/tables/mesh.csv';
SET pharFile = '/FileStore/tables/pharma.csv';

/* Files year */
SET ctYear = '2021';

/* Files delimiters */
SET ctDelimiter = '|';
SET condDelimiter = ',';
SET mshDelimiter = ',';
SET pharDelimiter = ',';
SET stats = 'Completed';

/* Collumns from clienttrial file */
SET typeCol = 'Type';
SET condCol = 'Conditions';
SET pcCol = 'Parent_Company';
SET compCol = 'Completion';

/* Collumns from pharma file */
SET sponCol = 'Sponsor';

/* Collumns delimiters */
SET condDelimiter = ",";
SET nullDelimiter = '';
SET rootDelimiter = ".";
SET sponDeliminiter = '","';
SET compDelimiter = " ";
SET stats = "Completed";



-- COMMAND ----------

-- DBTITLE 1,Question 1
/* Deletes the ctDF table if existing */
DROP TABLE IF EXISTS ctDF;

/* Creates a new ctDF table from ctFile with ctDelimiter as delimiter */
CREATE TABLE ctDF
USING csv
OPTIONS (path = ${hiveconf:ctFile}, delimiter = ${hiveconf:ctDelimiter}, header "true")

-- COMMAND ----------

SELECT * FROM ctDF LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Question 1 - Result
SELECT count (*) AS Studies
FROM ctDF

-- COMMAND ----------

-- DBTITLE 1,Question 2 - Result
SELECT Type, count (*) AS Frequency
FROM ctDF
GROUP BY Type
ORDER BY count(Type)DESC

-- COMMAND ----------

-- DBTITLE 1,Question 3 - Result
SELECT Condition, count(*) AS Frequency
FROM (SELECT explode(split(Conditions, ',')) AS Condition
      FROM ctDF)
WHERE Condition != ''
GROUP BY Condition
ORDER BY count(*) DESC
LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Question 4
DROP TABLE IF EXISTS msh;

CREATE TABLE msh
USING csv
OPTIONS (path = ${hiveconf:mshFile}, delimiter = ${hiveconf:mshDelimiter}, header "true")

-- COMMAND ----------

-- DBTITLE 1,Question 4 - Result
SELECT (substring(tree,1,3)) AS Roots, count(Condition) AS Frequency FROM msh 
INNER JOIN (SELECT explode (split(Conditions, ${hiveconf:condDelimiter})) AS Condition
      FROM ctDF)
ON term = Condition
WHERE Condition != ${hiveconf:nullDelimiter}
GROUP BY substring(tree,1,3)
ORDER BY count(Condition)DESC
LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Question 5
DROP TABLE IF EXISTS phar;

CREATE TABLE phar
USING csv
OPTIONS (path = ${hiveconf:pharFile}, delimiter = ${hiveconf:pharDelimiter}, header "true")

-- COMMAND ----------

SELECT * FROM phar LIMIT 10

-- COMMAND ----------

SELECT Parent_Company
FROM phar

-- COMMAND ----------

-- DBTITLE 1,Question 5 - Result
SELECT Sponsor, count(id) AS Frequency
FROM ctDF 
LEFT JOIN phar ON Parent_Company = Sponsor
WHERE Parent_Company IS NULL
GROUP BY Sponsor 
ORDER BY count(id)DESC
LIMIT 10



-- COMMAND ----------

-- DBTITLE 1,Question 6 - Result
SELECT substring(completion,1,3) AS Months,count (substring(completion,1,3)) AS Completed_Studies
FROM ctDF
WHERE status= "Completed" AND substring(completion,5,4)= ${hiveconf:ctYear}
GROUP BY Months
ORDER BY CASE Months WHEN 'Jan' THEN 1
                    WHEN 'Feb' THEN 2
                    WHEN 'Mar' THEN 3
                    WHEN 'Apr' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'Jun' THEN 6
                    WHEN 'Jul' THEN 7
                    WHEN 'Aug' THEN 8
                    WHEN 'Sep' THEN 9
                    WHEN 'Oct' THEN 10
                    WHEN 'Nov' THEN 11
                    WHEN 'Dec' THEN 12
         END

-- COMMAND ----------


