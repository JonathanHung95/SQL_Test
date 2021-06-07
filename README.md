# SQL_Test
Code and documentation for the SQL Test from WCD.

### Documentation of any questions/uncertainties and decisions you had to make along the way

The test required us to calculate gross revenue as [=cost + datacost + revenue] and net revenue as [=revenue].  I'm not sure that is the correct way to calculate those numbers, but I have calculated the numbers based on those formulas for this report.

### Documentation of any steps you did not have time to complete or features you would add given more time

1. I would write some of the code as a function.  The repeated lines to generate the results from the sql query [cursor.fetchall()] and to store it in a dataframe could have been written up as a function to make the code a bit neater.
2. Potentially write a DAG to have airflow automatically run the report daily.

### Provide an analysis of the report results. What is going on with the business? 

Based on the report, the given company is doing worse this year than last year.  
