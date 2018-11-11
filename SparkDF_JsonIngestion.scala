package com.spark2.util.df

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import java.io.File

case class SparkDF_JsonIngestion(sc: SparkContext,sparkSession: SparkSession) {
    import sparkSession.implicits
  def jsonIngestionDF(): Unit = {

   /* val ccDF = sparkSession.read.csv("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/inputdata/emp1.txt")
    ccDF.show()
    val ccDF1 = sparkSession.read.format("com.databricks.spark.csv").option("header", "true")
    .load("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/inputdata/emp.csv")
    ccDF1.show()
    val ccDF2 = sparkSession.read.format("com.databricks.spark.csv").option("header", "true")
    .load("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/inputdata/cc_report_sample_data.csv")
    ccDF2.show()*/

    //val ew = new PrintWriter(new File("D:/NVE/Data/inputFiles/test.csv"))
    
    val jsonDF = sparkSession.read.json("D:/Hadoop/Hadoop_Myself_Spark_Practice/Data/inputdata/Json/ITEM*.json")
    jsonDF.printSchema()
    
    jsonDF.createOrReplaceTempView("ITEM")
    val sqlDF = jsonDF.sqlContext.sql(""" 
          select 
          userId, 
          startDate,
          endDate,
          payloadtype,
          c1.id as id,
          c1.category as category,
          c1.usage as usage,
          c1.cost as cost
          from(
          select request.userId,
          request.startDate,
          request.endDate,
          case when payload.electric is not null AND size(payload.electric) > 0 THEN 'electric' end as payloadtype,
          c2
          from ITEM
          LATERAL VIEW explode(payload) exploded_table as c2
          )a 
          LATERAL VIEW explode(c2.electric) exploded_table1 as c1
            """)
    sqlDF.createOrReplaceTempView("item")
    sqlDF.sparkSession.sql("select *from item where startDate='2017-03-18'").show(500000)
    println("count="+sqlDF.sparkSession.sql("select *from item where startDate='2017-03-18'").count())

    /*val df1 = sparkSession.read.
    json("D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/retail_db_data/retail_db_json/customers.json")
    df1.createOrReplaceTempView("customer")
    val sqlDF1 = sparkSession.sql(""" 
          SELECT
          *FROM customer
            """)
    sqlDF1.show()*/

    /*for (out <- sqlDF) {
      ew.write(out.toString().replace('[', ' ').replace(']', ' ') + "\n")
    }
    ew.close()*/

  }
}
