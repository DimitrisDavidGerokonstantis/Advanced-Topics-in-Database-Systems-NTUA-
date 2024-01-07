from geopy import distance
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf, split,monotonically_increasing_id,regexp_replace,min,substring,year,avg,row_number
spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()


# calculate the distance between two points [lat1,long1], [lat2,long2] in km
def get_distance(lat1, long1, lat2, long2):
    return distance.geodesic((lat1,long1),(lat2,long2)).km

get_distance_udf=udf(get_distance,DoubleType())



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

full_dataset_df = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted).filter((col("LAT")!=0)&(col("LON")!=0)).withColumn("year", year("DATE Rptd"))

LA_Police_Stations_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/LA_Police_Stations.csv", header=True)


full_dataset_df.createOrReplaceTempView("full_dataset")
LA_Police_Stations_df.createOrReplaceTempView("LA_Police_Stations")

JoinedData_query="""
SELECT *
FROM full_dataset as FD
INNER JOIN LA_Police_Stations as PS
WHERE FD.`Weapon Used Cd` IS NOT NULL
"""

JoinedDataquery_df = spark.sql(JoinedData_query)
JoinedDataquery_withDist_df=JoinedDataquery_df.withColumn("distance",get_distance_udf(col("LAT"),col("LON"),col("Y"),col("X")))
JoinedDataquery_withDist_df.createOrReplaceTempView("JoinedData")


FinalResults_query="""
SELECT DIVISION, avg(distance) as average_distance ,count(*) as `#` FROM
(
SELECT ROW_NUMBER OVER w as rank
FROM JoinedData
WINDOW w AS (PARTITION BY DR_NO ORDER BY distance)
)
WHERE rank=1
GROUP BY DIVISION
ORDER BY `#` DESC
"""
FinalResults_df=spark.sql(FinalResults_query).show()
