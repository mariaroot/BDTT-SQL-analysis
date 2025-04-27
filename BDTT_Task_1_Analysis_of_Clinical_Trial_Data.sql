-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # BDTT Task 1: Analysis of Clinical Trial Data Using Spark SQL
-- MAGIC
-- MAGIC ## Introduction
-- MAGIC For this task, I performed some analysis of data from clinical trials registered in the USA. First I imported and pre-processed the data, then I used SQL to query the data to answer 4 specific questions.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Import and Preprocessing
-- MAGIC
-- MAGIC In this section, I read the data from a csv file into a dataframe. I then ensured that the data in every column was of the correct datatype (changing the datatype if not). I also counted the number of null values in each column and checked the format of columnns containing lists. All these features will be important for the questions I have to answer subsequently. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check that the Clinicaltrial_16012025.csv file was uploaded successfully to the DBFS 
-- MAGIC
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read Clinicaltrial_16012025.csv file into dataframe (df1) without splitting lines in columns that contain lists (eg Conditions column). 
-- MAGIC
-- MAGIC df1 = spark.read.csv(
-- MAGIC "/FileStore/tables/Clinicaltrial_16012025.csv",
-- MAGIC     header=True, # Use the first row as the header
-- MAGIC     inferSchema=True, # Infer data types
-- MAGIC     multiLine=True # Enable multiline support
-- MAGIC )
-- MAGIC
-- MAGIC # Show first 5 rows and count how many rows in total 
-- MAGIC
-- MAGIC df1.show(5)
-- MAGIC Number_of_rows = df1.count()
-- MAGIC
-- MAGIC print("Number of rows in dataframe:", Number_of_rows)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC From the output of the code above, I can see that all columns are in the default 'string' datatype. However, I can see from `.show(5)` that I need to convert the Enrollment column to the 'integer' datatype and the Start date and Completion date columns to the date datatype. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert Enrollment column from default string datatype to integer datatype 
-- MAGIC
-- MAGIC df1 = df1.withColumn("Enrollment", df1.Enrollment.cast("int"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Inspect contents of columns that contain dates to determine how to cast them from the default string datatype to date datatype
-- MAGIC
-- MAGIC date_columns = df1[['Start Date', 'Completion Date']]
-- MAGIC date_columns.show(10, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC From the brief inspection above, I can see that the Start and Completion Dates are in different formats in different rows. I will check what percentage of the start and completion dates are in the format YYYY-MM or YYYY-MM-DD using regular expressions. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC # Define regular expressions
-- MAGIC
-- MAGIC regex_yyyy_mm_dd = r'^\d{4}-\d{2}-\d{2}$'  # Matches YYYY-MM-DD
-- MAGIC regex_yyyy_mm = r'^\d{4}-\d{2}$'  # Matches YYYY-MM
-- MAGIC
-- MAGIC # Count Start Date rows that match each pattern
-- MAGIC
-- MAGIC SD_count_yyyy_mm_dd = df1.filter(col("Start Date").rlike(regex_yyyy_mm_dd)).count()
-- MAGIC SD_count_yyyy_mm = df1.filter(col("Start Date").rlike(regex_yyyy_mm)).count()
-- MAGIC
-- MAGIC # Total count of Start Date rows matching either pattern 
-- MAGIC
-- MAGIC SD_total_count = SD_count_yyyy_mm_dd + SD_count_yyyy_mm
-- MAGIC
-- MAGIC # Calculate percentage and round to the nearest whole number
-- MAGIC
-- MAGIC SD_percentage = round((SD_total_count / Number_of_rows) * 100)
-- MAGIC
-- MAGIC print("Number of Start Date entries conforming to YYYY-MM-DD or YYYY-MM format:", SD_total_count)
-- MAGIC print("Percentage of Start Date entries conforming to YYYY-MM-DD or YYYY-MM format:", SD_percentage, "%")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Count Completion Date rows that match each pattern
-- MAGIC
-- MAGIC CD_count_yyyy_mm_dd = df1.filter(col("Completion Date").rlike(regex_yyyy_mm_dd)).count()
-- MAGIC CD_count_yyyy_mm = df1.filter(col("Completion Date").rlike(regex_yyyy_mm)).count()
-- MAGIC
-- MAGIC # Total count of Completion Date rows matching either pattern 
-- MAGIC
-- MAGIC CD_total_count = CD_count_yyyy_mm_dd + CD_count_yyyy_mm
-- MAGIC
-- MAGIC # Calculate percentage and round to the nearest whole number
-- MAGIC
-- MAGIC CD_percentage = round((CD_total_count / Number_of_rows) * 100)
-- MAGIC
-- MAGIC print("Number of Completion Date entries conforming to YYYY-MM-DD or YYYY-MM format:", CD_total_count)
-- MAGIC print("Percentage of Completion Date entries conforming to YYYY-MM-DD or YYYY-MM format:", CD_percentage, "%")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC From the inspection above, I can see that almost all Start Date and Completion Date entries are in the format YYYY-MM-DD or YYYY-MM. I will ignore the ones that are not in one of these formats, as there are so few of them that they shouldn't have much impact on my analysis. 
-- MAGIC
-- MAGIC Next I need to convert the Start Date and Completion Date columns from string datatype into date datatype. By default, PySpark requires full dates for this, so I will convert YYYY-MM values to YYYY-MM-01, making an assumption that dates in the YYYY-MM format refer to the first day of that month. This will be important for calculating mean trial lengths in question 3 below.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import to_date, when, length, col, concat, lit
-- MAGIC
-- MAGIC # Convert Start date and Completion date columns from string format to date format
-- MAGIC
-- MAGIC df1 = df1.withColumn(
-- MAGIC     "Start Date",
-- MAGIC     when(length(col("Start Date")) == 7, to_date(concat(col("Start Date"), lit("-01")), "yyyy-MM-dd"))  # Handle YYYY-MM
-- MAGIC     .otherwise(to_date(col("Start Date"), "yyyy-MM-dd"))  # Handle YYYY-MM-DD
-- MAGIC )
-- MAGIC
-- MAGIC df1 = df1.withColumn(
-- MAGIC     "Completion Date",
-- MAGIC     when(length(col("Completion Date")) == 7, to_date(concat(col("Completion Date"), lit("-01")), "yyyy-MM-dd")) 
-- MAGIC     .otherwise(to_date(col("Completion Date"), "yyyy-MM-dd")) 
-- MAGIC )
-- MAGIC
-- MAGIC # Check datatypes and contents of start date and completion date columns to check that the datatype conversion above worked 
-- MAGIC
-- MAGIC date_columns = df1[['Start Date', 'Completion Date']]
-- MAGIC date_columns.show(10, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next I will check how many null values there are in each column. This will be useful for questions 3 and 4 below (where I will likely need to filter out trials which don't have a start date or end date)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Count how many null values in each column
-- MAGIC
-- MAGIC from pyspark.sql.functions import col, sum
-- MAGIC df1.select([sum(col(c).isNull().cast("int")).alias(c) for c in df1.columns]).show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now I'll inspect the format of the lists in two of the columns of df1. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Examine two columns that contain lists to see how the lists are formatted
-- MAGIC
-- MAGIC selected_columns = df1[['Conditions', 'Interventions']]
-- MAGIC selected_columns.show(5, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the cell above, I can see that for the columns that contain lists, the delimiter between list items is |. This will be important for questions 2 and 4. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1 
-- MAGIC
-- MAGIC **List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent**
-- MAGIC
-- MAGIC To answer this question, I used SQL Views. This allows me to show only certain parts of the data without making any lasting changes to the underlying dataframe 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create temporary view of df1
-- MAGIC
-- MAGIC df1.createOrReplaceTempView("ct1")

-- COMMAND ----------

-- Check temporary view creation worked 

SELECT * FROM ct1 LIMIT 5

-- COMMAND ----------

-- Create smaller view of just the 'Study Type' column 

CREATE OR REPLACE TEMP VIEW types AS SELECT `Study Type` FROM ct1

-- COMMAND ----------

-- Count frequencies of different study types and show in descending order (from most to least frequent study type)

SELECT `Study Type`, COUNT(*) as frequency
FROM types
GROUP BY `Study Type`
ORDER BY COUNT(*) DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By far the most common clinical trial types are Interventional and Observational, which account for 76% and 23% of the trials, respectively. The entries from the 8th ranking onwards are numbers or phrases that don't make much sense in the context of clinical trial type (for example, 2 trials have the trial type listed as 'France'). This suggests these rows had missing values in other columns which resulted in the row contents getting shifted into neighbouring columns. However, as the frequencies of these erroneous entries are very low, I think they can be ignored. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2
-- MAGIC
-- MAGIC **List the top 10 conditions along with their frequency**
-- MAGIC
-- MAGIC The Condition column contains multiple conditions in each row, so I need to separate these out and count each occurrence separately.

-- COMMAND ----------

-- Create temporary view of the conditions column only. Create an array in each row of the conditions column, splitting on the delimiter |

CREATE OR REPLACE TEMP VIEW split_conditions AS SELECT split(Conditions , '\\|') as conditions FROM ct1

-- COMMAND ----------

-- Check the code above worked 

SELECT * FROM split_conditions LIMIT 10

-- COMMAND ----------

-- Create another temporary view, exploding out the arrays in each row of the consitions column above so that each item in the array (that is, each condition) is on a separate row

CREATE OR REPLACE TEMP VIEW exploded_conditions AS SELECT explode(conditions) FROM split_conditions

-- COMMAND ----------

-- Check the code above worked 

SELECT * FROM exploded_conditions LIMIT 10

-- COMMAND ----------

-- Count the frequenct of each condition, order the conditions from most to least frequent, show the top 10 most frequent conditions 

SELECT `col`, COUNT(*) as frequency
FROM exploded_conditions
GROUP BY `col`
ORDER BY COUNT(*) DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By far the most common entry in the 'Conditions' column is 'Healthy'. I assume this is because healthy individuals were used in control groups for many of these trials. Cancer also appears 3 times in this list of top 10 most common conditions, as Breast Cancer (2nd place), Prostate Cancer (7th place) and Cancer (10th place). This is somewhat misleading as breast cancer and prostate cancers are both subsets of the broader category of "Cancer", not separate conditions. Furthermore, in the view called 'split_conditions' above, I can see that row 8 contains a list of conditions including "Stage I Prostate Cancer", "Stage II Prostate Cancer" and "Stage III Prostate Cancer". Terms like these should be aggregated with the term "Prostate Cancer", which is currently the 7th most common condition in the count above. There are probably other instances of this inaccurate separation in the dataset (for example, it might be appropirate to aggregate "severe depression" with "depression" and "chronic pain" with "pain" if those terms occur in the "Conditions" column). However, this is beyond the scope of this assignment. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 3
-- MAGIC
-- MAGIC **For studies with an end date, calculate the mean clinical trial length in months.**
-- MAGIC
-- MAGIC As for question 1, I will use SQL views to answer this question without altering the underlying dataframe 

-- COMMAND ----------

-- Create temporary view of just the start date and completion date columns, and only the rows where neither of those fields are blank

CREATE OR REPLACE TEMP VIEW ct2 AS SELECT `Start Date`, `Completion Date` FROM ct1 WHERE `Completion Date` IS NOT NULL AND `Start Date` IS NOT NULL

-- COMMAND ----------

-- Inspect top 20 rows of new temporary view to check it worked 

SELECT * FROM ct2 LIMIT 20

-- COMMAND ----------

-- Add column that calculates Duration of each trial in days by subtracting the start date from the end date. Use ABS to return only positive integers (that is, exclude any rows where the start date is after the end date)

CREATE OR REPLACE TEMP VIEW ct3 AS
SELECT *, 
       ABS(DATEDIFF(MONTH, `Completion Date`, `Start Date`)) AS Duration
FROM ct2;


-- COMMAND ----------

-- Check above code worked 

SELECT * FROM ct3 LIMIT 10

-- COMMAND ----------

-- Calculate mean duration (in months) of all trials 

SELECT ROUND(AVG(Duration), 0) AS Average_Duration FROM ct3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The mean duration of the clinical trials is 35 months. However, I want to check if mean is a good reflection of the average trial length for this dataset. To do this, I will count the frequency of each trial duration and sort these frequencies by trial duration

-- COMMAND ----------

-- Count frequencies of trial durations and sort from longest duration to shortest duration 

SELECT `Duration`, COUNT(*) as frequency
FROM ct3
GROUP BY `Duration`
ORDER BY `Duration` DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By visualising the above table as a scatter graph (with Duration along the x axis), I can see that there is a very long tail of long durations (up to 1,320 months) that have very low frequencies. This tail is likely to be skewing the mean up. If I were to calculate the median trial duration I think it would be a more accurate reflection of the average trial duration, and would likely be lower than the mean. However, this is beyong the scope of this assignment. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 4
-- MAGIC
-- MAGIC **From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year. Display the trend over time in an appropriate visualisation.** 
-- MAGIC
-- MAGIC (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ in the Conditions column.)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- Create temporary view of trials with non-null completion date and Sudy Status of 'COMPLETED'

CREATE OR REPLACE TEMP VIEW ct4 AS SELECT `Conditions`, `Study Status`, `Start Date`, `Completion Date` FROM ct1 WHERE `Completion Date` IS NOT NULL AND `Study Status`="COMPLETED"

-- COMMAND ----------

-- Check the code above worked 

SELECT * FROM ct4 LIMIT 10

-- COMMAND ----------


-- From the above temoprary view, show only the rows that contain the word 'diabetes' (case-insenstive) in the 'Conditions' column 

CREATE OR REPLACE TEMP VIEW ct5 AS SELECT * FROM ct4 WHERE Conditions ILIKE '%diabetes%';


-- COMMAND ----------

-- Check the above code worked

SELECT * FROM ct5  LIMIT 10;

-- COMMAND ----------

-- Add column called 'Year' which shows only the completion year (not the month or date)

CREATE OR REPLACE TEMP VIEW ct6 AS
SELECT *, 
       YEAR(TO_DATE(`Completion Date`, 'dd-MM-yyyy')) AS Year
FROM ct5;

SELECT * FROM ct6  LIMIT 10;

-- COMMAND ----------

-- Count how many diabetes studies completed in EACH YEAR 
-- Visualise with scatter plot 

SELECT COUNT(Conditions), Year
FROM ct6
GROUP BY Year
ORDER BY Year DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As the scatter plot above shows, there were very few studies on diabetes completed between 1989 and 2000. From 2000 onwards, the number of completed diabetes studies increased steadily up to a peak of 755 in 2019. Between 2019 and 2024, the number of studies seems to be decreasing. This could be because there are fewer diabetes studies being conducted, or because diabetes studies started since 2019 have not yet completed, or a combination of these two factors. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC In conclusion, I used python to import and pre-process a large dataset on clinical trials. I then used SQL to query this dataset to answer 4 specific questions to gain insight into this dataset. 