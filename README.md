# AdvancedDatabasesTopics2023-2024  

## Description
In this project, we implemented some queries on the Los Angeles Crime Data 2010-2023 datasets. We made use of  
a two-node cluster to load our data and run our queries in a distributed environment using Apache Spark and  
HDFS (Hadoop Distributed File System). In this repository, one can find our queries implemented using the  
DataFrame API, the SQL API and RDDs, as well as our report with an analysis of the performance of each query  
(making comparisons between different implementation methods and different numbers of Spark executors). This  
project was created as part of the "Advanced Topics in Database Systems" course of the National Technical  
University of Athens, during the 9th semester.    


## Collaborators  
- [Dimitrios-David Gerokonstantis](https://github.com/DimitrisDavidGerokonstantis)  (el19209)
- [Athanasios Tsoukleidis-Karydakis](https://github.com/ThanosTsoukleidis-Karydakis)  (el19009)

## Datasets  
We run our queries using the following datasets that are stored in the HDFS:  
- `basic_dataset_2010to2019b.csv`: Basic LA Crime Data from 2010 to 2019.  
- `basic_dataset_2020topresent.csv`: Basic LA Crime Data from 2020 to 2023. 
- `LA_income_2015.csv`: Estimated median incomes for different LA Zip Codes for year 2015.
- `revgecoding_b.csv` : Reverse Geocoding (correlation between Latitude, Longitude and Zip Codes).
- `LA_Police_Stations.csv`: A list of the police stations of Los Angeles and their locations.  

## Project Layout
We implemented the queries using Python and specifically pyspark. Our project consists  
of the following folders, each one containing code for each question of the assignment:  

- `Question 2`: Creation of a DataFrame for our basic dataset (from 2010 to 2023), appropriate type casting
for some rows and printing of the schema and total number of rows.  
- `Question 3/Query 1`: Implementation of the first query.
- `Question 4/Query 2`, : Implementation of the second query.
- `Question 5/Query 3` : Implementation of the third query.
- `Question 6/Query 4` :  Implementation of the fourth query.
- `Question 7` : Some of the query plans extracted while executing the joins using the
hint&explain methods.
- `report.pdf` : Contains our report in pdf format with our analysis of the results, performances and
explanations of our codes.
- `Latex Project` : Contains a .zip file of our Latex Project (Latex code and images used).
- `advanced_db_project_en.pdf` : Contains the description of the project (e.g. description of the queries that we implemented).    

## Cluster Description
As mentioned before, we used a two-node cluster, consisting of two virtual machines, running Ubuntu 22.04 and    
hosted on the website https://okeanos.grnet.gr/. The two machines (master and worker) were connected to each  
other through a private network and a public IP was attached to one of the two nodes to enable remote  
communication with the cluster. Each machine had the following technical characteristics:   
- RAM : 8GB
- Number of CPUs: 4
- Disk Capacity:  30GB
- Private IP addresses : 192.168.0.2 for the master and 192.168.0.3 for the worker.  

## Usage
After we have set up our cluster, we need to install in both our nodes Hadoop (HDFS, Yarn resource manager) and  
Apache Spark. For the installation and the necessary configurations of our system we followed the lab guide given  
to us during the course. Some further details regarding installation and system setup are analyzed in the  
beginning of our report (e.g. Loading data to HDFS). Our report contains the answers to all the questions of the assignment.  
