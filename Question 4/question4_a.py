## from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf
import time
spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()

start=time.time()

#reading the datasets as we did before
basic_dataset_2010to2019_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2010to2019b.csv", header=True)
basic_dataset_2010to2019_df_casted = basic_dataset_2010to2019_df \
.withColumn("Date Rptd", to_timestamp("Date Rptd", "MM/dd/yyyy hh:mm:ss a").cast(DateType())) \
.withColumn("DATE OCC", to_timestamp("DATE OCC", "MM/dd/yyyy hh:mm:ss a").cast(DateType())) \
.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) \
.withColumn("LAT", col("LAT").cast(DoubleType())) \
.withColumn("LON", col("LON").cast(DoubleType()))

basic_dataset_2020topresent_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2020topresent.csv", header=True)
basic_dataset_2020topresent_df_casted = basic_dataset_2020topresent_df \
.withColumn("Date Rptd", to_timestamp("Date Rptd", "MM/dd/yyyy hh:mm:ss a").cast(DateType())) \
.withColumn("DATE OCC", to_timestamp("DATE OCC", "MM/dd/yyyy hh:mm:ss a").cast(DateType())) \
.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) \
.withColumn("LAT", col("LAT").cast(DoubleType())) \
.withColumn("LON", col("LON").cast(DoubleType()))

full_dataset_df = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted)

#defining the User Defined Function that matches a specific time to its corresponding period of day
def get_period_of_day(time_occ):
    if int(time_occ)>=500 and int(time_occ)<=1159:
        return "05:00-11:59"
    elif int(time_occ)>=1200 and int(time_occ)<=1659:
        return "12:00-16:59"
    elif int(time_occ)>=1700 and int(time_occ)<=2059:
        return "17:00-20:59"
    elif (int(time_occ)>=0 and int(time_occ)<=459) or (int(time_occ)>=2100 and int(time_occ)<=2359) :
        return "21:00-03:59"
    else :
        return "Null"

#register the UDF
get_period_of_day_udf = udf(get_period_of_day, StringType())

#add a new column with the corresponding period of day (about when the crime was commited)
full_dataset_df_withDayPeriod = full_dataset_df \
    .withColumn("period_of_day", get_period_of_day_udf(col("TIME OCC")))

#main body of the query
full_dataset_df_withDayPeriod.filter(col("Premis Desc")=="STREET").groupBy("period_of_day").agg(count("*") \
.alias("crime_total")).orderBy(col("crime_total").desc()).select("period_of_day", "crime_total").show()

end=time.time()

print("Execution time: ",end-start)

spark.stop()
