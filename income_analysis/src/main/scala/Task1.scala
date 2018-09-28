import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.util.Date
import java.io._


object Task1 {
  case class Salary(country: String, salary: String, salarytype: String)

  def main(args: Array[String]):Unit = {

    //configuration
//    val conf = new SparkConf().setAppName("response_count").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder.getOrCreate
    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("553_hw1").getOrCreate
    import spark.implicits._

    //build dataframe
    val df = spark.read.csv("file:////Users/chenxijia/personal/code/usccode/inf553/hw/hw1/data/survey_results_public.csv").select("_c3","_c52", "_c53" )
    val _df = df.withColumnRenamed("_c3", "Country").withColumnRenamed("_c52", "Salary").withColumnRenamed("_c53","SalaryType")
    val header = _df.first()
    val main_df = _df.filter(x=>x!=header)
    val Salary = main_df.filter(x=>x(1) != "NA" ).filter(x=>x(1) != "0")
    main_df.show()

    //partition size
    print ("partition size for Salary is:"+main_df.rdd.partitions.size)
    Salary.rdd.repartition(2)
      .mapPartitionsWithIndex{case (i,rows) => Iterator((i,rows.size))}
      .toDF("partition_number","number_of_records")
      .show


    //compute num of response per country
    var start_time =new Date().getTime
    val sum = Salary.rdd.map(x=>("Total",1)).reduceByKey((a, b) => a + b).collect()
    var end_time =new Date().getTime
    println("running time is"+(end_time-start_time))
    sum.foreach(println)


//    for (i <- 0 until sum.length){
//      writer.write(sum(i)(1).toString().drop(1).dropRight(1) + '\n')
//    }

    var country_salary = Salary.rdd.map(x=>(x(0),1)).reduceByKey((a, b) => a + b).map(x=>(x._1.toString(),x._2)).sortBy(x=>x._1,true).collect()
    country_salary.foreach(println)

    //print into csv\
    val writer = new PrintWriter(new File("Xijia_Chen_task1.csv" ))
    writer.write("Total," + sum(0).toString().drop(7).dropRight(1) + "\n")
    for (i <- 0 until country_salary.length){
      writer.write( country_salary(i).toString().drop(1).dropRight(1) + '\n')
    }

    writer.close()

//    for (i <- 0 until country_salary.length){
//        val temp = country_salary(i).toString().drop(1).dropRight(1)
//        var flag = true
//        var j = temp.length-1
//        while ( j>0 && flag == true){
//          if (isIntByRegex(temp(j)) == false){
//            flag = false
//          }else{
//            j-=1
//          }
//        }
//        val num = temp.substring(j+1)
//        var country_split = temp.substring(0,j+1).split(",")
//        var country = ""
//        if (country_split.length>1) {
//          country = country_split(0) + ", " + country_split(1)
//        }else{
//          country = country_split(0)
//        }
//        writer.write(country.toString()+'\n')
//    }


//    val conf = new SparkConf().setAppName("response_count").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    //print basic info
//    val survey_result = sc.textFile("./data/survey_results_public.csv")
////    survey_result.foreach(println)
//
//    //filter
//    val survey_array = survey_result.map(x => x.split(","))
//    val salary = survey_array.filter(x => x(52) != "NA" ).filter(x=>x(52)!=0)
//
//    //get result
//    val sum = salary.map(x=>("Total",1)).reduceByKey((a, b) => a + b)
//    val country_salary = salary.map(x=>(x(3),1)).reduceByKey((a, b) => a + b).sortBy(_._1, true).collect()
//    sum.foreach(println)
//    country_salary.foreach(println)
  }

//  def isIntByRegex(s : Char) = {
//    val pattern = """^(\d+)$""".r
//    s match {
//      case pattern(_*) => true
//      case _ => false
//    }
//  }
}
