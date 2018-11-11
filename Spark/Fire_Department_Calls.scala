package com.spark2.util.dataframe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class Fire_Department_Calls(sparkSession: SparkSession) {
  import sparkSession.implicits._

  def fire_Department_Calls_for_Service(inputFile: String) {

    val FD_Calls = sparkSession.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(inputFile)
    FD_Calls.printSchema()

    val FD_Calls_DF = FD_Calls.toDF("Call_Number", "Unit_ID", "Incident_Number", "Call_Type", "Call_Date",
      "Watch_Date", "Received_DtTm", "Entry_DtTm", "Dispatch_DtTm", "Response_DtTm",
      "On_Scene_DtTm", "Transport_DtTm", "Hospital_DtTm", "Call_Final_Disposition", "Available_DtTm",
      "Address", "City", "Zipcode_of_Incident", "Battalion", "Station_Area",
      "Box", "Original_Priority", "Priority", "Final_Priority", "ALS_Unit",
      "Call_Type_Group", "Number_of_Alarms", "Unit_Type", "Unit_sequence_in_call_dispatch", "Fire_Prevention_District",
      "Supervisor_District", "Neighborhooods_Analysis_Boundaries", "Location", "RowID")
    FD_Calls_DF.printSchema()

    //writeDataUsingFileFormats(FD_Calls_DF)
    DistinctCalls(FD_Calls_DF)
  }

  def writeDataUsingFileFormats(FD_Calls_DF: DataFrame) {
    sparkSession.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    sparkSession.sqlContext.setConf("spark.sql.orc.compression.codec", "snappy")
    sparkSession.sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")

    FD_Calls_DF.show(100)

    FD_Calls_DF.write.format("parquet").mode("overwrite")
      .parquet("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/Fire_Department_Calls_for_Service/fdcs_parquet")

    FD_Calls_DF.write.format("orc").mode("overwrite")
      .orc("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/Fire_Department_Calls_for_Service/fdcs_orc")

    FD_Calls_DF.write.format("com.databricks.spark.avro").mode("overwrite").
      save("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/Fire_Department_Calls_for_Service/fdcs_avro")

    val df = sparkSession.read.format("com.databricks.spark.avro")
      .load("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/Fire_Department_Calls_for_Service/fdcs_avro/*.avro")
    df.printSchema()
    df.show()
  }

  def DistinctCalls(FD_Calls_DF: DataFrame) {
    println("Record count : " + FD_Calls_DF.count())
    FD_Calls_DF.select("*").orderBy("City").groupBy("City", "Call_Date", "Call_Type").count().coalesce(1)
      .write.option("header", "true").option("inferSchema", "true").mode("overwrite")
      .csv("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/Fire_Department_Calls_for_Service/DistinctCallscount.csv")
  }

}
