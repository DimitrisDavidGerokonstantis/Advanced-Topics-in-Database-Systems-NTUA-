from geopy import distance
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf, split,monotonically_increasing_id,regexp_replace,min,substring,year,avg
import time
spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()


# calculate the distance between two points [lat1,long1], [lat2,long2] in km
def get_distance(lat1, long1, lat2, long2):
    return distance.geodesic((lat1,long1),(lat2,long2)).km

get_distance_udf=udf(get_distance,DoubleType())

start=time.time()

#reading datasets
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

#keeping the crimes relevant to the desired weapon type
full_dataset_WeaponFiltered_df=full_dataset_df.filter(substring(col("Weapon Used Cd"),1,1)=="1")
#main body of the query - explained in the report
full_dataset_WeaponFiltered_with_minDistance_df=full_dataset_WeaponFiltered_df.join(LA_Police_Stations_df)\
.withColumn("distance",get_distance_udf(col("LAT"),col("LON"),col("Y"),col("X")))\
.groupBy(col("DR_NO")).agg(min("distance").alias("minimum_distance"),min("year").alias("year"))


Results_df=full_dataset_WeaponFiltered_with_minDistance_df\
.groupBy("year").agg(count("*").alias("#"),avg("minimum_distance").alias("average_distance")).orderBy("year").select("year","average_distance","#").show()

end=time.time()
print("Execution time:",end-start)
spark.stop()
