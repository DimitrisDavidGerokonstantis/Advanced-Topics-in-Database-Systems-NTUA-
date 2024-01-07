from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf, split,monotonically_increasing_id,regexp_replace,min,sum,year
import time

spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()

#Function that matches descent abbreviations to the full descent names
def get_full_victim_descent(victim_descent):
    if victim_descent=="A":
        return "Other Asian"
    elif victim_descent=="B":
        return "Black"
    elif victim_descent=="C":
        return "Chinese"
    elif victim_descent=="D":
        return "Cambodian"
    elif victim_descent=="F":
        return "Filipino"
    elif victim_descent=="G":
        return "Guamanian"
    elif victim_descent=="H":
        return "Hispanic/Latin/Mexican"
    elif victim_descent=="I":
        return "American Indian/Alaskan Native"
    elif victim_descent=="J":
        return "Japanese"
    elif victim_descent=="K":
        return "Korean"
    elif victim_descent=="L":
        return "Laotian"
    elif victim_descent=="O":
        return "Other"
    elif victim_descent=="P":
        return "Pacific Islander"
    elif victim_descent=="S":
        return "Samoan"
    elif victim_descent=="U":
        return "Hawaiian"
    elif victim_descent=="V":
        return "Vietnamese"
    elif victim_descent=="W":
        return "White"
    elif victim_descent=="X":
        return "Unknown"
    elif victim_descent=="Z":
        return "Asian Indian"

get_full_victim_descent = udf(get_full_victim_descent, StringType())


start = time.time()

#reading the main datasets
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


full_dataset_df = basic_dataset_2010to2019_df_casted.union(basic_dataset_2020topresent_df_casted).withColumn("year",year("DATE OCC"))


#keep records with not null descent and only relevant to year 2015
full_dataset_noNullDescent_df=full_dataset_df.filter(col("Vict Descent").isNotNull()).filter(col("year")==2015)

#reading the other datasets
median_household_income_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/LA_income_2015.csv", header=True)\
.withColumn("Estimated median income",regexp_replace(col("Estimated Median Income"), "[^\d]", "").cast(IntegerType()))

reverse_geocoding_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/revgecoding_b.csv", header=True)

#keep only the Zip code value for the records that contain postal code suffix too (i.e. if we have 90731-7232 we only want to keep the 90731)
reverse_geocoding_keepFirstZip_df=reverse_geocoding_df.withColumn("ZIP_code",split(col("ZIPcode"),"-")[0]).filter(col("ZIP_code").isNotNull())


# purpose : keep only ZIP codes relevant to the basic dataset (in which a crime has been commited)
full_dataset_noNullDescent_withZip_df = full_dataset_noNullDescent_df.join(reverse_geocoding_keepFirstZip_df, \
                                                                          (full_dataset_noNullDescent_df["LAT"]==reverse_geocoding_keepFirstZip_df["LAT"]) & \
                                                                          (full_dataset_noNullDescent_df["LON"]==reverse_geocoding_keepFirstZip_df["LON"]),"leftouter") \
                                        .select("Vict Descent","ZIPcode")
full_dataset_noNullDescent_withZip_df.persist()

#After this, for each crime we have the Victim Descent, the Zip Code and the median income for that zip code
full_dataset_noNullDescent_withZip_andIncome_df = full_dataset_noNullDescent_withZip_df.join(median_household_income_df,\
                                                                                             full_dataset_noNullDescent_withZip_df['ZIPcode']==median_household_income_df['Zip Code']) \
.select("Vict Descent", "ZIPcode","Estimated median income")

#keep Zip codes with the corresponding median incomes
income_perRelevant_zip_code_df = full_dataset_noNullDescent_withZip_andIncome_df.select("ZIPcode","Estimated median income")\
.groupBy("ZIPcode").agg(min("Estimated median income").alias("Estimated median income"))
income_perRelevant_zip_code_df.persist()

#keep Zip codes of areas with the lowest and highest median income
lowest_income_zip_codes3_df = income_perRelevant_zip_code_df.orderBy("Estimated median income").select("ZIPcode").limit(3)
highest_income_zip_codes3_df = income_perRelevant_zip_code_df.orderBy(col("Estimated median income").desc()).select("ZIPcode").limit(3)

#main body of the query
print("For Highest Income")
Victim_Descent_Highest_Income = full_dataset_noNullDescent_withZip_df.join(highest_income_zip_codes3_df,"ZIPcode").groupBy("Vict Descent").agg(count("*").alias("#"))\
.orderBy(col("#").desc()).withColumn("Victim Descent", get_full_victim_descent(col("Vict Descent"))).select("Victim Descent","#")
Victim_Descent_Highest_Income.show()

print("For Lowest Income")
Victim_Descent_Lowest_Income = full_dataset_noNullDescent_withZip_df.join(lowest_income_zip_codes3_df,"ZIPcode").groupBy("Vict Descent").agg(count("*").alias("#"))\
.orderBy(col("#").desc()).withColumn("Victim Descent", get_full_victim_descent(col("Vict Descent"))).select("Victim Descent","#")
Victim_Descent_Lowest_Income.show()

print("Total Results")
Victim_Descent_Highest_Income.union(Victim_Descent_Lowest_Income).groupBy("Victim Descent").agg(sum("#").alias("#")).orderBy(col("#").desc()).show()

end=time.time()

print("Execution Time:",end-start)

spark.stop()
