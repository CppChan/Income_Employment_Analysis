import java.io.{File, PrintWriter}
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner

object Task2 {

  def main(args: Array[String]):Unit = {

    //configuration
    val conf = new SparkConf().setAppName("response_count").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._



//    var rdd = sc.textFile("./data/survey_results_public.csv").repartition(2)
//    print("dddddddddddd"+rdd.getNumPartitions)
//    val schema = StructType(Seq(
//      StructField("Country", StringType, false),
//      StructField("Salary", StringType, false),
//      StructField("SalaryType", StringType, false)
//    ))
//    val partitioner = new HashPartitioner(2)
//    val rdd_ = rdd.map(r => (r(0), r)).partitionBy(partitioner)
//    val rdd__ = rdd_.map(r=>(r(1)(0),r(1)(2),r(1)(3)))
//    val df = spark.createDataFrame(rdd_, schema)

    //build dataframe
    val df = spark.read.csv("file:////Users/chenxijia/personal/code/usccode/inf553/hw/hw1/data/survey_results_public.csv").select("_c3", "_c52", "_c53")
    val _df = df.withColumnRenamed("_c3", "Country").withColumnRenamed("_c52", "Salary").withColumnRenamed("_c53", "SalaryType")
    val header = _df.first()
    val main_df = _df.filter(x => x != header)
    var Salary = main_df.filter(x => x(1) != "NA").filter(x => x(1) != "0")
//    Salary.repartition(2, $"Country").write.csv("task_2")


    //before partition

    var partition_size1 = Salary
      .rdd
      .repartition(2)
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .rdd.collect()
    Salary
      .rdd
      .repartition(2)
    var start_time_ =new Date().getTime
    val sum_ = Salary.rdd.map(x=>("Total",1)).reduceByKey((a, b) => a + b).collect()
    var end_time_ =new Date().getTime


    //after repartition
    var partition_size2 = Salary
      .repartition(2, $"Country")
      .rdd
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .rdd.collect()
    Salary
      .repartition(2, $"Country")
    var start_time =new Date().getTime
    val sum = Salary.rdd.map(x=>("Total",1)).reduceByKey((a, b) => a + b).collect()
    var end_time =new Date().getTime


    val writer = new PrintWriter(new File("Xijia_Chen_task2.csv"))
    writer.write("standard, "+partition_size1(0).toString().drop(3).dropRight(1)+partition_size1(1).toString().drop(2).dropRight(1)+","+(end_time_ - start_time_).toString()+"\n")
    writer.write("partition, "+partition_size2(0).toString().drop(3).dropRight(1)+partition_size2(1).toString().drop(2).dropRight(1)+","+(end_time - start_time).toString()+"\n")
    writer.close()

    //compute num of response per country
    sum.foreach(println)
    val country_salary = Salary.rdd.map(x=>(x(0),1)).reduceByKey((a, b) => a + b).map(x=>(x._1.toString(),x._2)).sortBy(x=>x._1,true).collect()
    country_salary.foreach(println)

//    main_df.rdd.partitions.size

  }
}
