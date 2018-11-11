package com.spark2.util.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.DataFrame

case class Sales(id: Int, name: String, loc: String, sales: Long)

class SalesGroupByDistinctCount(spark: SparkSession) {
  val rdd = spark.sparkContext.textFile("D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/sales.csv") //CSV File

  val sales_rdd = rdd.map(row => {
    val r = row.split(",")
    Sales(r(0).toInt, r(1), r(2), r(3).toLong)
  })
  /* ----   (or) --------
  val sales_rdd = rdd.map(rec => rec.split(",")).map(row => Sales(row(0).toInt, row(0), row(0), row(0).toLong))
    */
  import spark.implicits._
  sales_rdd.toDF().select($"id", $"name", collect_set($"loc").over(Window.partitionBy("id", "name")).as("loc"),
    sum($"sales").over(Window.partitionBy("id", "name")).as("sum_sales")).distinct().show()
  // (or)
  sales_rdd.toDF().select($"id", $"name", collect_set($"loc").over(Window.partitionBy("id", "name")).as("loc"),
    sum($"sales").over(Window.partitionBy("id", "name")).as("sum_sales")).dropDuplicates().show()

   spark.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema","true").option("delimiter",",")
   .load("D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/emp.txt").show()
   
   // Only parquet for files
   //val sqlDF = spark.sql("SELECT * FROM parquet.`D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/emp.txt`") 
    
  /*
     Input.csv

  	101,Ram,hyderabad,7000
		102,raja,mumbai,8900
		101,Ram,bangalore,7000
		103,ravi,chennai,9000
		102,raja,delhi,6000
		103,ravi,bangalore,6700
		101,Ram,pune,6300

  	OUTPUT:
  	+---+-----+--------------------+---------+
		| id|name1|                 loc|sum_sales|
		+---+-----+--------------------+---------+
		|101|  Ram|[hyderabad, banga...|    20300|
		|102| raja|     [mumbai, delhi]|    14900|
		|103| ravi|[chennai, bangalore]|    15700|
		+---+-----+--------------------+---------+*/

  /*  val df1 = sales_rdd.toDF().groupBy("id", "name").agg(collect_set("loc").as("loc")).toDF("id", "name1", "loc") // its work in cluster because hiveUDF..
  val df2 = sales_rdd.toDF().groupBy("id", "name").agg(sum($"sales")).as("sum_sales").toDF("id", "name2", "sum_sales")
  val df3 = df1.join(df2, "id")
  df3.select("id", "name1", "loc", "sum_sales").show()
  df3.show()

   //(or)

  val sales_df=sales_rdd.toDF().registerTempTable("Sales")
  val df_sales=sqlContext.sql("select id,name,collect_set(loc) as loc,sum(sales) as sum_sales from Sales group by id,name");

  df_sales.show()*/

  /* sales_rdd.toDF().groupBy("id", "name").agg(sum($"sales")).as("sum_sales").show()
  sales_rdd.toDF().groupBy("id", "name").agg(avg($"sales")).as("avg_sales").show()
  sales_rdd.toDF().groupBy("id", "name").agg(max($"sales")).as("max_sales").show()
  sales_rdd.toDF().groupBy("id", "name").agg(min($"sales")).as("min_sales").show()
  sales_rdd.toDF().groupBy("id", "name").agg(count($"sales")).as("count_sales").show()

Apple,10,Hyderabad
SweetLemon,15,Bnaglore
orange,5,chennai
banana,20,Hyderabad
Mango,35,Bnaglore
Grapes,50,chennai
//----------------------------------------

var fr=sc.textFile("D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/inputdata/fruits.csv")
var names=fr.map(line=>line.split(","))
println("Assending Order")
names.map(x=>(x(1).toInt,x(0))).sortByKey().foreach(println)//asending order
println("Dessending Order")
names.map(x=>(x(1).toInt,x(0))).sortByKey(false).foreach(println)//desending order
println("Dessending Order TOP 3")
names.map(x=>(x(1).toInt,x(0))).sortByKey(false). take(3).foreach(println)//desending

*/

}