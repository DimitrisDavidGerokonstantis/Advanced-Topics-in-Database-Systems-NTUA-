from geopy import distance
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf, split,monotonically_increasing_id,regexp_replace,min,substring,year,avg,row_number
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

#reading the datasets
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

#removing Null island location
full_dataset_df = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted).filter((col("LAT")!=0)&(col("LON")!=0)).withColumn("year", year("DATE Rptd"))

LA_Police_Stations_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/LA_Police_Stations.csv", header=True)

#keep crimes with not null weapon type
full_dataset_WeaponFiltered_df=full_dataset_df.filter(col("Weapon Used Cd").isNotNull())

full_dataset_WeaponFiltered_with_distances_df=full_dataset_WeaponFiltered_df.join(LA_Police_Stations_df)\
.withColumn("distance",get_distance_udf(col("LAT"),col("LON"),col("Y"),col("X")))

#defining the window specification
window_spec = Window.partitionBy("DR_NO").orderBy("distance")

full_dataset_WeaponFiltered_with_distances_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank")==1)\
.groupBy("DIVISION").agg(count("*").alias("#"),avg("distance").alias("average_distance"))\
.orderBy(col("#").desc()).select("DIVISION","average_distance","#").show()

end=time.time()
print("Execution Time:",end-start)

spark.stop()


'''
#OTHER IMPLEMENTATION : USING ONE MORE JOIN IN ORDER TO KEEP THE DIVISION FIELD
full_dataset_WeaponFiltered_with_minDistance_df=full_dataset_WeaponFiltered_with_distances_df\
.groupBy(col("DR_NO")).agg(min("distance").alias("minimum_distance")).alias("first")\
.join(full_dataset_WeaponFiltered_with_distances_df.alias("second"),(col("first.DR_NO")==col("second.DR_NO")) &\
     (col("first.minimum_distance")==col("second.distance")))\
.select(col("first.DR_NO"), col("second.DIVISION"),col("first.minimum_distance"))


Results_df=full_dataset_WeaponFiltered_with_minDistance_df\
.groupBy("DIVISION").agg(count("*").alias("#"),avg("minimum_distance").alias("average_distance")).orderBy(col("#").desc()).select("DIVISION","average_distance","#").show()
'''
