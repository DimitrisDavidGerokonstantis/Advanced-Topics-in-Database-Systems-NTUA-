import csv
from io import StringIO
from pyspark.sql import SparkSession
import time

#function to parse the csv file (because of a problem explained in the report)
def parse_csv(row):
    csv_reader = csv.reader(StringIO(row))
    return next(csv_reader)

sc = SparkSession \
    .builder \
    .appName("RDD query 2 execution") \
    .getOrCreate() \
    .sparkContext

start=time.time()

# reading the datasets
basic_dataset_2010to2019 = sc.textFile("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2010to2019b.csv") \
                .map(parse_csv)
basic_dataset_2020topresent= sc.textFile("hdfs://okeanos-master:54310/spark_project_datasets/basic_dataset_2020topresent.csv") \
                .map(parse_csv)

full_datasets = basic_dataset_2010to2019.union(basic_dataset_2020topresent)

#defining the different periods of day
morning_list=['05','06','07','08','09','10','11']
afternoon_list = ['12','13','14','15','16']
evening_list = ['17','18','19','20']
night_list = ['21','22','23','00','01','02','03','04']

#the main body of the query
Morning_RDD_counter = full_datasets.map(lambda x: ['morning', 1] if((x[3][:2] in morning_list) and (x[15]=='STREET')) \
                                        else ['afternoon', 1] if((x[3][:2] in afternoon_list) and (x[15]=='STREET'))  \
                                        else ['evening', 1] if((x[3][:2] in evening_list) and (x[15]=='STREET'))  \
                                        else ['night', 1] if((x[3][:2] in night_list) and (x[15]=='STREET')) \
                                        else None) \
                                    .filter(lambda x: x != None)\
                                    .reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False).collect()

print(Morning_RDD_counter)

end=time.time()

print("Execution time: ",end-start)
sc.stop()
