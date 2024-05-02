# Welcome to your new notebook
# Type here in the cell editor to add code!
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.functions import dayofmonth, dayofweek, date_format, month, quarter, year, lit, concat
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window
    
startyear = 2013
endyear = 2026

lakehousePath = "abfss://c89241ee-7519-4e67-8cf6-fd90c1b75eb5@onelake.dfs.fabric.microsoft.com/00cf5ef5-2096-4a32-8030-a854c2533b66"
deltaTablePath = f"{lakehousePath}/Tables/Calendar" 
    
spark = SparkSession.builder.appName("Calendar").getOrCreate()

# Set the time parser policy to LEGACY
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Create a DataFrame with a range of dates
dates = spark.range(
    (datetime(endyear, 12, 31) - datetime(startyear, 1, 1)).days + 1
).select(
    (date_add(lit(f"{startyear}-01-01"), col("id").cast("int"))).alias("date")
)


# Select the desired columns
calendardf = dates.select(
    monotonically_increasing_id().alias("Id"),
    "Date",
    dayofmonth("date").alias("DayNumber"),
    dayofweek("date").alias("DayOfWeekNumber"),
    date_format("date", "EEEE").alias("DayOfWeekName"),
    date_format("date", "EEE").alias("DayOfWeekNameShort"),
    weekofyear("date").alias("WeekOfYear"),
    month("date").alias("MonthNumber"),
    date_format("date", "MM").alias("MonthSort"),
    date_format("date", "MMMM").alias("MonthName"),
    date_format("date", "MMM").alias("MonthNameShort"),
    expr("trunc(date, 'MM')").alias("FirstDayOfMonth"),
    last_day("date").alias("LastDayOfMonth"),
    concat(year("date"), lit("-"), date_format("date", "MM")).alias("YearMonth"),
    quarter("date").alias("QuarterNumber"),
    concat(lit("Q"), quarter("date")).alias("QuarterName"),
    concat(year("date"), lit("-"), lit("Q"), quarter("date")).alias("YearQuarter"),
    year("date").alias("Year"),
    expr("CASE WHEN dayofweek(date) IN (2,3,4,5,6) THEN true ELSE false END").alias("IsWeekDay"),
    expr("CASE WHEN dayofweek(date) IN (1, 7) THEN true ELSE false END").alias("IsHoliday")
   

)

# Show the resulting DataFrame
#calendardf.show()
calendardf.write.format("delta").mode("overwrite").save(deltaTablePath)
