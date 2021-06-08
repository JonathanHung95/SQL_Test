import mysql.connector
import pandas as pd
import datetime as dt
import openpyxl

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

# normally, we would use the datetime library to pull the date for the report
# but for this exercise, we have been told to assume that today is sept 24th 2018
# so we will be using yesterday's date as that's where our data would be most accurate up to

today = dt.datetime(2018, 9, 24)
last_year_today = dt.datetime(2017, 9, 24)

yesterday = dt.datetime(2018, 9, 23)
last_year_yesterday = dt.datetime(2017, 9, 23)

print("Generating report...")

# set up the connections and all that

db = mysql.connector.connect(host = "localhost",
                                user = "root",
                                password = "12345",
                                database = "test")

cursor = db.cursor()

# FIRST PART
# we can neatly pull the gross revenue, net revenue and margin percentage in one query for 2018

cursor.execute("""
    SELECT WEEK(date) AS raw_week, 
	    RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week, 
        ROUND((SUM(revenue) + SUM(cost) + SUM(datacost)), 0) AS gross_revenue,
        ROUND(SUM(revenue), 0) AS net_revenue,
        ROUND((SUM(revenue) / (SUM(revenue) + SUM(cost) + SUM(datacost)) * 100), 0) AS margin_percentage
    FROM fact_date_customer_campaign
    JOIN dim_customer 
	    ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
    WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
    GROUP BY WEEK(date)
    ORDER BY week;
    """, (yesterday.year, yesterday.strftime("%Y-%m-%d")))

# save to a dataframe, this is pretty much the base for our 2018 report details

gross_net_margin_this_year = cursor.fetchall()
gross_net_margin_this_year_df = pd.DataFrame(gross_net_margin_this_year, 
                                            columns = ["raw_week", "week", "gross_revenue", "net_revenue", "margin_percentage"])

# fix the issue where due to the week layout in sql, some years have a week 0 and some don't

if gross_net_margin_this_year_df.iloc[0]["raw_week"] != 0:
    gross_net_margin_this_year_df.loc[-1] = ["0", "00_" + str(yesterday.year), "", "", ""]
    gross_net_margin_this_year_df.index = gross_net_margin_this_year_df.index + 1
    gross_net_margin_this_year_df.sort_index(inplace = True)

# we also have to pull the same data for last year (2017 in this case)

cursor.execute("""
    SELECT WEEK(date) AS raw_week, 
	    RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week, 
        ROUND((SUM(revenue) + SUM(cost) + SUM(datacost)), 0) AS gross_revenue,
        ROUND(SUM(revenue), 0) AS net_revenue,
        ROUND((SUM(revenue) / (SUM(revenue) + SUM(cost) + SUM(datacost)) * 100), 0) AS margin_percentage
    FROM fact_date_customer_campaign
    JOIN dim_customer 
	    ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
    WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
    GROUP BY WEEK(date)
    ORDER BY week;
    """, (last_year_yesterday.year, last_year_yesterday.strftime("%Y-%m-%d")))

# save to a new dataframe for 2017 data

gross_net_margin_last_year = cursor.fetchall()
gross_net_margin_last_year_df = pd.DataFrame(gross_net_margin_last_year, 
                                            columns = ["raw_week", "week", "gross_revenue", "net_revenue", "margin_percentage"])

if gross_net_margin_last_year_df.iloc[0]["raw_week"] != 0:
    gross_net_margin_last_year_df.loc[-1] = ["0", "00_" + str(last_year_yesterday.year), "", "", ""]
    gross_net_margin_last_year_df.index = gross_net_margin_last_year_df.index + 1
    gross_net_margin_last_year_df.sort_index(inplace = True)

# SECOND PART
# get the daily average gross and net revenue for the weeks generated (2018)

cursor.execute("""
    WITH CTE AS (
        SELECT WEEK(date) AS raw_week, 
            date,
            CASE
                WHEN DATE_ADD(date, INTERVAL 7 DAY) <= %s THEN DATE_ADD(date, INTERVAL 7 DAY) 
                ELSE %s
            END AS end_date,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week, 
            (SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue,
            SUM(revenue) AS net_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer 
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    )
    SELECT raw_week,
        week,
        ROUND((gross_revenue / DATEDIFF(end_date, date)), 0) AS daily_avg_gross_revenue,
        ROUND((net_revenue / DATEDIFF(end_date, date)), 0) AS daily_avg_net_revenue
    FROM CTE;
""", (yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), yesterday.year, yesterday.strftime("%Y-%m-%d")))

# add to a dataframe

daily_avg_net_gross_this_year = cursor.fetchall()
daily_avg_net_gross_this_year_df = pd.DataFrame(daily_avg_net_gross_this_year, 
                                                columns = ["raw_week", "week", "daily_avg_gross_revenue", "daily_avg_net_revenue"])

if daily_avg_net_gross_this_year_df.iloc[0]["raw_week"] != 0:
    daily_avg_net_gross_this_year_df.loc[-1] = ["0", "00_" + str(yesterday.year), "", ""]
    daily_avg_net_gross_this_year_df.index = daily_avg_net_gross_this_year_df.index + 1
    daily_avg_net_gross_this_year_df.sort_index(inplace = True)

# get the same thing but for last year

cursor.execute("""
    WITH CTE AS (
        SELECT WEEK(date) AS raw_week, 
            date,
            CASE
                WHEN DATE_ADD(date, INTERVAL 7 DAY) <= %s THEN DATE_ADD(date, INTERVAL 7 DAY) 
                ELSE %s
            END AS end_date,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week, 
            (SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue,
            SUM(revenue) AS net_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer 
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    )
    SELECT raw_week,
        week,
        ROUND((gross_revenue / DATEDIFF(end_date, date)), 0) AS daily_avg_gross_revenue,
        ROUND((net_revenue / DATEDIFF(end_date, date)), 0) AS daily_avg_net_revenue
    FROM CTE;
""", (last_year_yesterday.strftime("%Y-%m-%d"), last_year_today.strftime("%Y-%m-%d"), last_year_yesterday.year, last_year_yesterday.strftime("%Y-%m-%d")))

# add to a dataframe

daily_avg_net_gross_last_year = cursor.fetchall()
daily_avg_net_gross_last_year_df = pd.DataFrame(daily_avg_net_gross_last_year, 
                                                columns = ["raw_week", "week", "daily_avg_gross_revenue", "daily_avg_net_revenue"])

if daily_avg_net_gross_last_year_df.iloc[0]["raw_week"] != 0:
    daily_avg_net_gross_last_year_df.loc[-1] = ["0", "00_" + str(last_year_yesterday.year), "", ""]
    daily_avg_net_gross_last_year_df.index = daily_avg_net_gross_last_year_df.index + 1
    daily_avg_net_gross_last_year_df.sort_index(inplace = True)

# THIRD PART
# get the period-over-period growth percentage for gross and net 2018
# this would be week after week

cursor.execute("""
    WITH CTE AS (
	SELECT WEEK(date) AS raw_week,
		RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
		(SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue,
        SUM(revenue) AS net_revenue
    FROM fact_date_customer_campaign
    JOIN dim_customer
		ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
	WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
    GROUP BY WEEK(date)
)
SELECT raw_week, 
	week, 
    gross_revenue, 
    net_revenue,
	ROUND(((gross_revenue - LAG(gross_revenue) OVER (ORDER BY raw_week)) / LAG(gross_revenue) OVER (ORDER BY raw_week) * 100), 0) AS gross_percentage_period_over_period_growth,
    ROUND(((net_revenue - LAG(net_revenue) OVER (ORDER BY raw_week)) / LAG(net_revenue) OVER (ORDER BY raw_week) * 100), 0) AS net_percentage_period_over_period_growth
FROM CTE;
""", (yesterday.year, yesterday.strftime("%Y-%m-%d")))

# put it into another dataframe

period_over_period_this_year = cursor.fetchall()
period_over_period_this_year_df = pd.DataFrame(period_over_period_this_year, 
                                            columns = ["raw_week", "week", "gross_revenue", "net_revenue", "gross_percentage_period_over_period_growth", "net_percentage_period_over_period_growth"])

if period_over_period_this_year_df.iloc[0]["raw_week"] != 0:
    period_over_period_this_year_df.loc[-1] = ["0", "00_" + str(yesterday.year), "", "", "", ""]
    period_over_period_this_year_df.index = period_over_period_this_year_df.index + 1
    period_over_period_this_year_df.sort_index(inplace = True)

# grab period-over-period data for 2017 as well

cursor.execute("""
    WITH CTE AS (
	SELECT WEEK(date) AS raw_week,
		RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
		(SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue,
        SUM(revenue) AS net_revenue
    FROM fact_date_customer_campaign
    JOIN dim_customer
		ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
	WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
    GROUP BY WEEK(date)
)
SELECT raw_week, 
	week, 
    gross_revenue, 
    net_revenue,
	ROUND(((gross_revenue - LAG(gross_revenue) OVER (ORDER BY raw_week))/ LAG(gross_revenue) OVER (ORDER BY raw_week) * 100), 0) AS gross_percentage_period_over_period_growth,
    ROUND(((net_revenue - LAG(net_revenue) OVER (ORDER BY raw_week)) / LAG(net_revenue) OVER (ORDER BY raw_week) * 100), 0) AS net_percentage_period_over_period_growth
FROM CTE;
""", (last_year_yesterday.year, last_year_yesterday.strftime("%Y-%m-%d")))

# put into dataframe

period_over_period_last_year = cursor.fetchall()
period_over_period_last_year_df = pd.DataFrame(period_over_period_last_year, 
                                            columns = ["raw_week", "week", "gross_revenue", "net_revenue", "gross_percentage_period_over_period_growth", "net_percentage_period_over_period_growth"])

if period_over_period_last_year_df.iloc[0]["raw_week"] != 0:
    period_over_period_last_year_df.loc[-1] = ["0", "00_" + str(last_year_yesterday.year), "", "", "", ""]
    period_over_period_last_year_df.index = period_over_period_last_year_df.index + 1
    period_over_period_last_year_df.sort_index(inplace = True)

# FOURTH PART
# year over year gross revenue growth percentage as requested

cursor.execute("""
    WITH CTE1 AS (
        SELECT WEEK(date) AS raw_week,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
            (SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    ),
    CTE2 AS (
        SELECT WEEK(date) AS raw_week,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
            (SUM(revenue) + SUM(cost) + SUM(datacost)) AS gross_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    )
    SELECT CTE1.raw_week,
        CTE1.week, 
        CTE2.week,
        ROUND(((CTE1.gross_revenue - CTE2.gross_revenue) / CTE2.gross_revenue) * 100, 0) AS gross_percentage_year_over_year_growth
    FROM CTE1
    JOIN CTE2 ON CTE1.raw_week = CTE2.raw_week;
""", (yesterday.year, yesterday.strftime("%Y-%m-%d"), last_year_yesterday.year, last_year_yesterday.strftime("%Y-%m-%d")))

# place data in a dataframe

year_over_year_gross_growth = cursor.fetchall()
year_over_year_gross_growth_df = pd.DataFrame(year_over_year_gross_growth,
                                                columns = ["raw_week", "CTE1_week", "CTE2_week", "gross_percentage_year_over_year_growth"])

if year_over_year_gross_growth_df.iloc[0]["raw_week"] != 0:
    year_over_year_gross_growth_df.loc[-1] = ["0", "00_" + str(yesterday.year), "00_" + str(last_year_yesterday.year), ""]
    year_over_year_gross_growth_df.index = period_over_period_this_year_df.index + 1
    year_over_year_gross_growth_df.sort_index(inplace = True)

# year over year net revenue growth percentage as requested

cursor.execute("""
    WITH CTE1 AS (
        SELECT WEEK(date) AS raw_week,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
            SUM(revenue) AS net_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    ),
    CTE2 AS (
        SELECT WEEK(date) AS raw_week,
            RIGHT(CONCAT('0', WEEK(date), '_', YEAR(date)), 7) AS week,
            SUM(revenue) AS net_revenue
        FROM fact_date_customer_campaign
        JOIN dim_customer
            ON dim_customer.customer_id = fact_date_customer_campaign.customer_id
        WHERE segment = 'Segment A' AND YEAR(date) = %s AND date <= %s
        GROUP BY WEEK(date)
    )
    SELECT CTE1.raw_week,
        CTE1.week, 
        CTE2.week,
        ROUND(((CTE1.net_revenue - CTE2.net_revenue) / CTE2.net_revenue), 0) AS net_percentage_year_over_year_growth
    FROM CTE1
    JOIN CTE2 ON CTE1.raw_week = CTE2.raw_week;
""", (yesterday.year, yesterday.strftime("%Y-%m-%d"), last_year_yesterday.year, last_year_yesterday.strftime("%Y-%m-%d")))

# place data in a dataframe

year_over_year_net_growth = cursor.fetchall()
year_over_year_net_growth_df = pd.DataFrame(year_over_year_net_growth,
                                                columns = ["raw_week", "CTE1_week", "CTE2_week", "net_percentage_year_over_year_growth"])

if year_over_year_net_growth_df.iloc[0]["raw_week"] != 0:
    year_over_year_net_growth_df.loc[-1] = ["0", "00_" + str(yesterday.year), "00_" + str(last_year_yesterday.year), ""]
    year_over_year_net_growth_df.index = period_over_period_this_year_df.index + 1
    year_over_year_net_growth_df.sort_index(inplace = True)


# now we can combine all our data
# shared aspects are the raw_week and the week
# but we can combine on raw_week to align everything nicely for the report

# create the 2018 section first

df_2018 = gross_net_margin_this_year_df
df_2018 = df_2018.join(daily_avg_net_gross_this_year_df, on = "raw_week", rsuffix = "_a")
df_2018 = df_2018.join(period_over_period_this_year_df, on = "raw_week", rsuffix = "_b")
df_2018 = df_2018.join(year_over_year_gross_growth_df, on = "raw_week", rsuffix = "_c")
df_2018 = df_2018.join(year_over_year_net_growth_df, on = "raw_week", rsuffix = "_d")

# clean up

df_2018 = df_2018.drop(["raw_week_a", "week_a", "raw_week_b", "week_b", "gross_revenue_b", "net_revenue_b", "raw_week_c", "raw_week_d", "CTE1_week_d", "CTE2_week_d"], 
                        axis = 1)

# create 2017 section

df_2017 = gross_net_margin_last_year_df
df_2017 = df_2017.join(daily_avg_net_gross_last_year_df, on = "raw_week", rsuffix = "_a")
df_2017 = df_2017.join(period_over_period_last_year_df, on = "raw_week", rsuffix = "_b")
df_2017 = df_2017.drop(["raw_week_a", "week_a", "raw_week_b", "week_b", "gross_revenue_b", "net_revenue_b"], axis = 1)

# combine the 2 parts and then reorganize into the final report

suffix = "_" + str(last_year_yesterday.year)
report_df = df_2018.join(df_2017, on = "raw_week", rsuffix = suffix)
report_df = report_df.drop(["CTE1_week", "CTE2_week", "raw_week" + suffix], axis = 1)

organized_columns = ["raw_week", "week", "gross_revenue", "daily_avg_gross_revenue", "net_revenue", "daily_avg_net_revenue", "margin_percentage", 
                        "gross_percentage_period_over_period_growth", "net_percentage_period_over_period_growth", "gross_percentage_year_over_year_growth", 
                        "net_percentage_year_over_year_growth", "week" + suffix, "gross_revenue" + suffix, "daily_avg_gross_revenue" + suffix, "net_revenue" + suffix, 
                        "daily_avg_net_revenue" + suffix, "margin_percentage" + suffix, "gross_percentage_period_over_period_growth" + suffix, 
                        "net_percentage_period_over_period_growth" + suffix]

report_df = report_df[organized_columns]

# write to excel file as neat as possible

report_df.to_excel("report_" + yesterday.strftime("%Y-%m-%d") + ".xlsx", index = False)
print("...Report generated!")

# send out the email report
# we use dummy emails here

msg = MIMEMultipart()

msg["To"] = "insert_recepeints_here@email.com"
msg["From"] = "insert_my_email_here@email.com"
msg["Subject"] = "Report for " + yesterday.strftime("%Y-%m-%d")

html = """\
        <html>
            Hi, <br>
            <br>
            Attached is the report for {}.
            <br>
            <br>
            Thank you, <br>
            Jonathan Hung
        </html>
        """.format(yesterday.strftime("%Y-%m-%d"))

html_part = MIMEText(html, "html")
msg.attach(html_part)

fp = open("report_" + yesterday.strftime("%Y-%m-%d") + ".xlsx", "rb")
xlsx_part = MIMEBase("application", "vnd.ms-excel")
xlsx_part.set_payload(fp.read())

fp.close()

encoders.encode_base64(xlsx_part)
xlsx_part.add_header("Content-Disposition", "attachment", filename = "report_" + yesterday.strftime("%Y-%m-%d") + ".xlsx")
msg.attach(xlsx_part)

s = smtplib.SMTP("insert_smtp_server_here")

s.sendmail("insert_my_email_here@email.com", ["list_of_receipients_here"], msg.as_string())
s.quit()
