package com.spark2.driver

import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.Level
import org.apache.log4j.Logger
import com.spark2.util.dataframe.Fire_Department_Calls

object DataFrameDriver {

  var isLocal: Boolean = _
  var master: String = _
  var appName: String = _
  var inputFile: String = _
  @transient private var sparkSession: SparkSession = _

  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss")
    println("Spark2.3.1 DataFrame Driver Program Started..... : " + sdf.format(new Date()))

    System.setProperty("hadoop.home.dir", "D:/Study Softwares/IDE'S Softwares/winutils-master/hadoop-2.7.1");
    //setLogLevel(true)

    if (args.length > 0) {
      isLocal = false
      master = args(0)
      appName = args(1)
      sparkSession = SparkSession
        .builder()
        .master(master)
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()

      inputFile = args(2)

      //sparkSession.conf.set("spark.executor.memory", "2g")

    } else {
      inputFile = "D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/retail_db_data/Fire_Department_Calls_for_Service.csv"
      isLocal = true
      master = "local[*]"
      appName = "DataFrameDriverApplication"
      sparkSession = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config("spark.sql.warehouse.dir", "D:/Hive/warehouse")
        .config("spark.sql.shuffle.partition",14)
        //.enableHiveSupport()
        .getOrCreate()

      //sparkSession.conf.set("spark.executor.memory", "2g")
    }

    val fdc = new Fire_Department_Calls(sparkSession)
    fdc.fire_Department_Calls_for_Service(inputFile);

    println("")
    println("Spark2.3.1 DataFrame Driver Program ended..... : " + sdf.format(new Date()))

  }
  /**
   * sets log Level
   * @param boolean
   */
  private def setLogLevel(boolean: Boolean) = {
    if (boolean) {
      Logger.getLogger("org").setLevel(Level.OFF);
      Logger.getLogger("akka").setLevel(Level.OFF);
      Logger.getLogger("com").setLevel(Level.OFF);
    }
  }
}