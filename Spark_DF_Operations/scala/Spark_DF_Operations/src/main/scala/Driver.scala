package RDD_DF

import java.util.Calendar
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql._

object Driver {
  def main(args: Array[String]):Unit = {
    println("Start of Program : " + Calendar.getInstance().getTime())
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Get SPARK Session
    val spark = SparkSession
                    .builder()
                    .appName("Spark_DF")
                    .getOrCreate()

    // Instantiate Transform RDD class
    val trans = new Transform_Df(spark)
    trans.transform_df(spark)
  }
}
