from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, year, to_timestamp, count, month, rank
from pyspark.sql.window import Window
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

#add year and month columns for each dataset record
full_dataset_withYearandMonth_df = full_dataset_df \
.withColumn("year", year("DATE OCC")).withColumn("month", month("DATE OCC"))

#creating an SQL table from the dataframe of the main dataset
full_dataset_withYearandMonth_df.createOrReplaceTempView("full_dataset")

#the main query
id_query = "SELECT * FROM \
    (SELECT year, month, count(*) as crime_total, rank() OVER w as rank FROM full_dataset \
    GROUP BY year, month \
    WINDOW w AS (PARTITION BY year ORDER BY count(*) DESC)) \
    WHERE rank<4"

results = spark.sql(id_query)
results.show(42)

end=time.time()

print("Execution Time: ",end-start)

spark.stop()
