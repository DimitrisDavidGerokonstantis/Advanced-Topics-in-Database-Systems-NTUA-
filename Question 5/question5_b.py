from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, DateType, StringType
from pyspark.sql.functions import col, to_timestamp, count, udf, split,monotonically_increasing_id,regexp_replace,min
spark = SparkSession \
    .builder \
    .appName("Defining schema (basic dataset) and printing") \
    .getOrCreate()

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

full_dataset_noNullDescent_df=full_dataset_df.filter(col("Vict Descent").isNotNull()).filter(col("year")==2015)

median_household_income_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/LA_income_2015.csv", header=True)\
.withColumn("Estimated median income",regexp_replace(col("Estimated Median Income"), "[^\d]", "").cast(IntegerType()))


reverse_geocoding_df = spark.read.csv("hdfs://okeanos-master:54310/spark_project_datasets/revgecoding_b.csv", header=True)

reverse_geocoding_keepFirstZip_df=reverse_geocoding_df.withColumn("ZIP_code",split(col("ZIPcode"),"-")[0]).filter(col("ZIP_code").isNotNull())



full_dataset_noNullDescent_df.createOrReplaceTempView("full_dataset_noNullDescent")
median_household_income_df.createOrReplaceTempView("median_household_income")
reverse_geocoding_keepFirstZip_df.createOrReplaceTempView("reverse_geocoding_keepFirstZip")


subquery_zipCode_per_Crime = """
SELECT `Vict Descent`, `ZIPcode`
FROM full_dataset_noNullDescent as FD
INNER JOIN reverse_geocoding_keepFirstZip as RG
ON FD.LAT=RG.LAT AND FD.LON=RG.LON
"""

full_dataset_noNullDescent_withZip_df=spark.sql(subquery_zipCode_per_Crime).createOrReplaceTempView("full_dataset_noNullDescent_withZip")

subquery_zipCode_and_Income_per_Crime="""
SELECT `Vict Descent`, `ZIPcode`,`Estimated median income`
FROM full_dataset_noNullDescent_withZip as FDZ
INNER JOIN median_household_income as MHI
ON FDZ.ZIPcode=MHI.`Zip Code`
"""

full_dataset_noNullDescent_withZip_andIncome_df=spark.sql(subquery_zipCode_and_Income_per_Crime).createOrReplaceTempView("full_dataset_noNullDescent_withZip_andIncome")

subquery_income_perRelevant_zip_code="""
SELECT `ZIPcode`, MIN(`Estimated median income`) as `Estimated median income`
FROM full_dataset_noNullDescent_withZip_andIncome
GROUP BY `ZIPcode`
"""

income_perRelevant_zip_code_df=spark.sql(subquery_income_perRelevant_zip_code).createOrReplaceTempView("income_perRelevant_zip_code")

subquery_lowest_income_zip_codes3="""
SELECT ZIPcode
FROM income_perRelevant_zip_code
ORDER BY `Estimated median income`
LIMIT 3
"""
subquery_lowest_income_zip_codes3_df=spark.sql(subquery_lowest_income_zip_codes3).createOrReplaceTempView("subquery_lowest_income_zip_codes3")

subquery_highest_income_zip_codes3="""
SELECT ZIPcode
FROM income_perRelevant_zip_code
ORDER BY `Estimated median income` DESC
LIMIT 3
"""
subquery_highest_income_zip_codes3_df=spark.sql(subquery_highest_income_zip_codes3).createOrReplaceTempView("subquery_highest_income_zip_codes3")


subquery_Victim_Descent_Highest_Income="""
SELECT `Vict Descent`, count(*) as `#`
FROM full_dataset_noNullDescent_withZip as FDZ
INNER JOIN subquery_highest_income_zip_codes3 as LIZ
ON FDZ.ZIPcode=LIZ.ZIPcode
GROUP BY `Vict Descent`
ORDER BY `#` DESC
"""

Victim_Descent_Highest_Income_df=spark.sql(subquery_Victim_Descent_Highest_Income)\
.withColumn("Victim Descent", get_full_victim_descent("Vict Descent")).select("Victim Descent","#")
Victim_Descent_Highest_Income_df.show()
Victim_Descent_Highest_Income_df.createOrReplaceTempView("Victim_Descent_Highest_Income")


subquery_Victim_Descent_Lowest_Income="""
SELECT `Vict Descent`, count(*) as `#`
FROM full_dataset_noNullDescent_withZip as FDZ
INNER JOIN subquery_lowest_income_zip_codes3 as LIZ
ON FDZ.ZIPcode=LIZ.ZIPcode
GROUP BY `Vict Descent`
ORDER BY `#` DESC
"""

Victim_Descent_Lowest_Income_df=spark.sql(subquery_Victim_Descent_Lowest_Income)\
.withColumn("Victim Descent", get_full_victim_descent("Vict Descent")).select("Victim Descent","#")
Victim_Descent_Lowest_Income_df.show()
Victim_Descent_Lowest_Income_df.createOrReplaceTempView("Victim_Descent_Lowest_Income")

unified_results="""
SELECT `Victim Descent`, sum(`#`) as `#`
FROM
(
(SELECT * FROM Victim_Descent_Highest_Income)
UNION
(SELECT * FROM Victim_Descent_Lowest_Income)
)
GROUP BY `Victim Descent`
ORDER BY `#` DESC
"""

unified_results_df=spark.sql(unified_results).show()
