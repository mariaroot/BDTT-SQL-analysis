-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # BDTT Task 1: Analysis of Clinical Trial Data Using Spark SQL
-- MAGIC
-- MAGIC ## Introduction
-- MAGIC
-- MAGIC _In this section you should briefly introduce the task and briefly summarise your understanding of what you need to do. It can be relatively brief and is just intended to frame and introduce the rest of the notebook. You should consider that you are writing this for a technical audience._
-- MAGIC
-- MAGIC ## Data Import and Preprocessing
-- MAGIC
-- MAGIC _In this section you should briefly explain the data import and preprocessing steps you have gone through to read the data into a DataFrame and clean it prior to creating the necessary tables for your SQL analysis in the following code cells. The aim is to provide explanation / justification for the techniques you have used. If there are any notable features of the data which have required handling in the pre-processing stage you should mention them here._
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Checking that the Clinicaltrial_16012025.csv file was uploaded successfully to the DBFS 
-- MAGIC
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read Clinicaltrial_16012025.csv file into dataframe (df1) without splitting lines in columns that contain lists (eg Conditions column). Show first 5 rows and count how many rows in total 
-- MAGIC
-- MAGIC df1 = spark.read.csv(
-- MAGIC "/FileStore/tables/Clinicaltrial_16012025.csv",
-- MAGIC     header=True, # Use the first row as the header
-- MAGIC     inferSchema=True, # Infer data types
-- MAGIC     multiLine=True # Enable multiline support
-- MAGIC )
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
-- MAGIC # Converting Enrollment column from default string datatype to integer datatype 
-- MAGIC
-- MAGIC df1 = df1.withColumn("Enrollment", df1.Enrollment.cast("int"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Inspecting contents of columns that contain dates to determine how to cast them from the default string datatype to date datatype
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
-- MAGIC regex_yyyy_mm_dd = r'^\d{4}-\d{2}-\d{2}$'  # Matches YYYY-MM-DD
-- MAGIC regex_yyyy_mm = r'^\d{4}-\d{2}$'  # Matches YYYY-MM
-- MAGIC
-- MAGIC # Count Start Date rows that match each pattern
-- MAGIC SD_count_yyyy_mm_dd = df1.filter(col("Start Date").rlike(regex_yyyy_mm_dd)).count()
-- MAGIC SD_count_yyyy_mm = df1.filter(col("Start Date").rlike(regex_yyyy_mm)).count()
-- MAGIC
-- MAGIC # Total count of Start Date rows matching either pattern 
-- MAGIC SD_total_count = SD_count_yyyy_mm_dd + SD_count_yyyy_mm
-- MAGIC
-- MAGIC # Calculate percentage and round to the nearest whole number
-- MAGIC SD_percentage = round((SD_total_count / Number_of_rows) * 100)
-- MAGIC
-- MAGIC print("Number of Start Date entries conforming to YYYY-MM-DD or YYYY-MM format:", SD_total_count)
-- MAGIC print("Percentage of Start Date entries conforming to YYYY-MM-DD or YYYY-MM format:", SD_percentage, "%")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Count Completion Date rows that match each pattern
-- MAGIC CD_count_yyyy_mm_dd = df1.filter(col("Completion Date").rlike(regex_yyyy_mm_dd)).count()
-- MAGIC CD_count_yyyy_mm = df1.filter(col("Completion Date").rlike(regex_yyyy_mm)).count()
-- MAGIC
-- MAGIC # Total count of Completion Date rows matching either pattern 
-- MAGIC CD_total_count = CD_count_yyyy_mm_dd + CD_count_yyyy_mm
-- MAGIC
-- MAGIC # Calculate percentage and round to the nearest whole number
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
-- MAGIC date_columns = df1[['Start Date', 'Completion Date']]
-- MAGIC date_columns.show(10, truncate=False)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next I will check how many null values there are in each column. This will be useful for questions 3 and 4 below (where I will likely need to filter out trials which don't have a start date or end date)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Counting how many null values in each column
-- MAGIC
-- MAGIC from pyspark.sql.functions import col, sum
-- MAGIC df1.select([sum(col(c).isNull().cast("int")).alias(c) for c in df1.columns]).show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now I'll inspect the format of the lists in 2 of the columns of df1. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Examining 2 columns that contain lists to see how the lists are formatted
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
-- MAGIC You should start each section with a markdown cell providing a brief explanation of the code. It should be aimed at providing enough information on the functions and methods you have used that a person with a similar tehcnical background can understand **why** you have used them.
-- MAGIC
-- MAGIC You can also include formatted code snippets like this ```print("Hello World")``` in a markdown cell. To do this, simply wrap in these characters at the beginning: "\```" and these characters at the end: "\```"_

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create temporary view of df1
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
-- MAGIC # Question 2
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

-- MAGIC %md
-- MAGIC By far the most common entry in the 'Conditions' column is 'Healthy'. I assume this is because healthy individuals were used in control groups for many of these trials. Cancer also appears 3 times in this list of top 10 most common conditions, as Breast Cancer (2nd place), Prostate Cancer (7th place) and Cancer (10th place). This is somewhat misleading as breast cancer and prostate cancers are both subsets of the broader category of "Cancer", not separate conditions. Furthermore, in the view called 'split_conditions' above, I can see that row 8 contains a list of conditions including "Stage I Prostate Cancer", "Stage II Prostate Cancer" and "Stage III Prostate Cancer". Terms like these should be aggregated with the term "Prostate Cancer", which is currently the 7th most common condition in the count above. There are probably other instances of this inaccurate separation in the dataset (for example, it might be appropirate to aggregate "severe depression" with "depression" and "chronic pain" with "pain" if those terms occur in the "Conditions" column). However, this is beyond the scope of this assignment. 

-- COMMAND ----------

-- Count the frequenct of each condition, order the conditions from most to least frequent, show the top 10 most frequent conditions 
SELECT `col`, COUNT(*) as frequency
FROM exploded_conditions
GROUP BY `col`
ORDER BY COUNT(*) DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 3
-- MAGIC
-- MAGIC **For studies with an end date, calculate the mean clinical trial length in months.**
-- MAGIC
-- MAGIC NB! Watch out for outliers skewing the mean (eg v v short or vv long trials)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Question 4
-- MAGIC
-- MAGIC **From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year. Display the trend over time in an appropriate visualisation.** 
-- MAGIC
-- MAGIC (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ in the Conditions column.)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use a markdown cell at the end of the question to briefly explain the output / relate this back to the question and to comment or explain anything significant about this finding. Follow this structure (or a similar structure) for all questions / sections of your submission.
-- MAGIC
-- MAGIC If you want to add additional markdown cells to provide further explanation throughout your notebook, add these as required.
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC Finish with a brief conclusion to wrap up your submission and any key findings.