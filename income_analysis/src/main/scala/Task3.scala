import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


object Task3 {

  case class Salary(country: String, salary: String, salarytype: String)

  def main(args: Array[String]):Unit = {

    val conf = new SparkConf().setAppName("response_count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._

    //build dataframe
    var df = spark.read.csv("file:////Users/chenxijia/personal/code/usccode/inf553/hw/hw1/data/survey_results_public.csv").select("_c3","_c52", "_c53" )
    var _df = df.withColumnRenamed("_c3", "Country").withColumnRenamed("_c52", "Salary").withColumnRenamed("_c53","SalaryType")
    var header = _df.first()
    var main_df = _df.filter(x=>x!=header)
    var Salary = main_df.filter(x=>x(1) != "NA" ).filter(x=>x(1) != "0").filter(x=>x(0) != "NA")
    Salary.show()


    // compute average
    var Salary_sql = Salary.createOrReplaceTempView("country_salary")
    var Salary_year = spark.sql("SELECT Country, CASE WHEN SalaryType = 'Monthly' THEN cast(Salary as float)*12 " +
      "WHEN SalaryType = 'Weekly' THEN cast(Salary as float)*52 "+
      "ELSE cast(Salary as float) END AS Year_Salary " +
      "FROM country_salary")
    var Salary_year_sql = Salary_year.createOrReplaceTempView("year_sql")
    var Salary_res = spark.sql("SELECT Country, COUNT(*) AS num, cast(MIN(Year_Salary) as double) AS min, cast(MAX(Year_Salary) as double) AS max, ROUND(AVG(Year_Salary),2) AS avg " +
      "FROM year_sql " +
      "GROUP BY Country " +
      "ORDER BY Country ASC")
    Salary_year.show()
    Salary_res.show()
    var res = Salary_res.toDF().rdd.collect()
    val writer = new PrintWriter(new File("Xijia_Chen_task3.csv"))
    writer.write("Country, "+"Num, "+"Min, "+"Max, "+"Avg_Salary"+"\n")
    for (i <- 0 until res.length){
      writer.write(res(i).toString().drop(1).dropRight(1) + '\n')
    }
    writer.close()


      //first read rdd, then transfer to dataframe
//    val survey_result = sc.textFile("./data/survey_results_public.csv").map(_.split(",")).filter(x => x(52) != "NA" ).filter(x=>x(52)!='0')
//    val survey_df =survey_result.map(attri => Salary(attri(3), attri(52), attri(53))).toDF()
//    survey_df.show()
  }
}
