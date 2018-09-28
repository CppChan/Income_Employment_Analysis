Resident income and employment analysis based on Hadoop MapReduce and Spark Scala
====

## Employment analysis in China

#### china_employment/src/Top_K
#### Used MapReduce on Hadoop to analyze the employment of residents in different countries with raw data from HDFS:
* data source: china_employment/china_employ.txt(generate from original csv in income_analysis/data/survey_results_schema.csv
* First use word count to count the number of every type of employment in China
* Then use sorting function in MapReduce to compute the Top K heat employment type in China.

#### other code in china_employment/src is some sample in MapReduce


## Resident income analysis
#### income_analysis/src/main/scala/
* Task 1: Compute the total number of survey responses per country that have provided a salary value – i.e., 
response entries containing ‘ NA ’ or ‘ 0 ’ salary values are considered non-useful responses and should be discarded

* Task 2: Since processing large volumes of data requires performance decisions, properly partitioning the
data for processing is imperative. In this task, I show the number of partitions for the RDD built in Task 1 and 
show the number of items per partition. Then, I use the partition function (using the country value as driver) to 
improve the performanceof map and reduce tasks.

* Task3: compute annual salary averages per country and show min and max salaries.

