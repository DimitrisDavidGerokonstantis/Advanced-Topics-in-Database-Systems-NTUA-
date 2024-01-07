from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()

#reading the first part of the dataset and casting
basic_dataset_2010to2019_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2010to2019b.csv", header=True)
basic_dataset_2010to2019_df_casted = basic_dataset_2010to2019_df \
.withColumn("Date Rptd", col("Date Rptd").cast(DateType())) \
.withColumn("DATE OCC", col("DATE OCC").cast(DateType())) \
.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) \
.withColumn("LAT", col("LAT").cast(DoubleType())) \
.withColumn("LON", col("LON").cast(DoubleType()))

#reading the second part of the dataset and casting
basic_dataset_2020topresent_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2020topresent.csv", header=True)
basic_dataset_2020topresent_df_casted = basic_dataset_2020topresent_df \
.withColumn("Date Rptd", col("Date Rptd").cast(DateType())) \
.withColumn("DATE OCC", col("DATE OCC").cast(DateType())) \
.withColumn("Vict Age", col("Vict Age").cast(IntegerType())) \
.withColumn("LAT", col("LAT").cast(DoubleType())) \
.withColumn("LON", col("LON").cast(DoubleType()))

#unify the 2 parts
full_dataset_df = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted)

print('Schema :')
full_dataset_df.printSchema()

print('Number of Rows :')
print(full_dataset_df.count())

spark.stop()
