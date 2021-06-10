# SQL_Test
Code and documentation for the SQL Test from WCD. 

Runs sql queries to pull data from the given sql dump, formats it into an excel file and can send out an email with the excel file attached.  Python is used to combine everything together.

### Documentation of any questions/uncertainties and decisions you had to make along the way

The test required us to calculate gross revenue as [=cost + datacost + revenue] and net revenue as [=revenue].  I'm not sure that is the correct way to calculate those numbers, but I have calculated the numbers based on those formulas for this report.

Due to 2017 starting on a different day than 2018, I had to add in some python code to give a week 0 to the 2017 data.  This week is basically empty and exists to properly align the 2018 and 2017 data together on the report for ease of reading.

### Documentation of any steps you did not have time to complete or features you would add given more time

1. I would write some of the code as functions.  The repeated lines to generate the results from the sql query [cursor.fetchall()] and to store it in a dataframe could have been written up as a function to make the code a bit neater and easier to read.
2. Potentially write a DAG to have airflow automatically run the report daily.  Additional automation of the report.
3. If not a DAG, then at least a BAT file and have the task scheduler run the report automatically.
4. Write the SQL queries to a seperate file to keep the main report code neater.

### Provide an analysis of the report results. What is going on with the business? 

Based on the report, the given company is doing worse this year than last year.  
