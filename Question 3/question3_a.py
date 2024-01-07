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

full_dataset = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted)

#add year and month columns for each dataset record
full_dataset_withYearandMonth = full_dataset \
.withColumn("year", year("DATE OCC")).withColumn("month", month("DATE OCC"))

#defining the window function specifications
windowSpec = Window.partitionBy("year").orderBy(col("crime_total").desc())

#main body of the query
full_dataset_withYearandMonth_Window = full_dataset_withYearandMonth \
.groupBy(col("year"),col("month")).agg(count("*").alias("crime_total")).withColumn("#",rank().over(windowSpec)).where(col("#")<4).show(42)

end=time.time()

print("Execution Time: ",end-start)

spark.stop()
