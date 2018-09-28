import java.io.{File, PrintWriter}

object getCountry {

  def main(args: Array[String]):Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder.master("local").appName("553_hw1").getOrCreate
    import spark.implicits._

    val df = spark.read.csv("file:////Users/chenxijia/personal/code/usccode/inf553/hw/hw1/data/survey_results_public.csv").select("_c3","_c9")
    val _df = df.withColumnRenamed("_c3", "Country").withColumnRenamed("_c9", "Employment")
    val header = _df.first()
    val main_df = _df.filter(x=>x!=header)
    var employ = main_df.filter(x=>x(1) != "NA" ).filter(x=>x(1) != "0").filter(x=>x(0) =="China").rdd.collect()

    val writer = new PrintWriter(new File("China_employ.csv"))
    writer.write("Country, "+"jobtype"+"\n")
    for (i <- 0 until employ.length){
      writer.write(employ(i).toString().drop(1).dropRight(1) + '\n')
    }
    writer.close()
  }

}
