package RDD_DF
import java.util.Calendar
import scala.io.Source._
import org.apache.spark._
import org.apache.spark.sql._
import java.util.Date
import java.text.SimpleDateFormat

class Transform_Df (val spark: SparkSession) {
  // Read Configuration from file
  def parseProperties(filename: String): Map[String,String] = {
    val lines = fromFile(filename).getLines.toSeq
    val cleanLines = lines.map(_.trim).filter(!_.startsWith("#")).filter(_.contains("="))
    cleanLines.map(line => { val Array(a,b) = line.split("=",2); (a.trim, b.trim)}).toMap
  }

  val prop_map = parseProperties("/home/suresh/Spark_DF_Operations/config/config.properties")

  val Customer_File = prop_map.getOrElse("Customer_File", "Not Found!")
  val Sales_File = prop_map.getOrElse("Sales_File","Not Found!")
  val Output_DIR = prop_map.getOrElse("Output_DIR","Not Found!")
  val Output_File = prop_map.getOrElse("Output_File","Not Found!")

  def transform_df(spark: SparkSession): Unit = {

    // Load Customer file into a dataframe
    val df_customer =   spark.read
      .format("csv")
      .option("header","true")
      .option("delimiter", "#")
      .option("inferSchema","true")
      .option("nullValue","NA")
      .option("mode","failfast")
      .option("path",Customer_File)
      .load()

    // Load Customer file into a dataframe
    val df_sales = spark.read
      .format("csv")
      .option("header","true")
      .option("delimiter", "#")
      .option("inferSchema","true")
      .option("nullValue","NA")
      .option("mode","failfast")
      .option("path",Sales_File)
      .load()

    // Create Temp View
    df_customer.createOrReplaceTempView("customer_table")
    df_sales.createOrReplaceTempView("sales_table")

    // Show created tables
    spark.catalog.listTables.show

    // Register Epoch time convert UDF
    spark.udf.register("time_convert",(epochMillis: Long) => {val df: SimpleDateFormat = new SimpleDateFormat("yyyy#MM#dd#hh");df.format(epochMillis*1000)})

    // Aggregate sales price according to state, timestamp
    val df_join = spark.sql("""select c.state,time_convert(timestamp) time,sum(s.sales_price) sum_sales from customer_table c, sales_table s where c.customer_id = s.customer_id group by state,time order by c.state, sum_sales desc""")

    // Write output to a file
    df_join.write
      .format("csv")
      .option("delimiter", "#")
      .option("mode","overwrite")
      .save(Output_DIR)

    println("End of Spark Program : " + Calendar.getInstance().getTime)
  }
}
