package com.spark2.util.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.DataFrame

class ElectionsResultsDF(spark: SparkSession) {
  import spark.implicits._

  val empDF = spark.createDataFrame(Seq(
    (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
    (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
    (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
    (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
    (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
    (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
    (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
    (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
    (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
    (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
    (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20))).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

  def mainCallingFuncrion(inputFile: String) {
    //electionResultReport(inputFile)
    //windowAnalyticFunctions()
    LagPreviousDifference()
  }
  def electionResultReport(inputFile: String): Unit = {

    /* val df=spark.read.format("com.databricks.spark.csv").option("header","true")
  .option("inferSchema", "true").option("delimiter", "\t")
  .load("D:/Hadoop/Hadoop_Meself_Spark_Practice/Data/data-master/electionresults/Is2014.tsv")
    */
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", "\t").csv(inputFile)
    df.printSchema()
    val filterDF = df //.filter($"state"==="Andhra Pradesh")
    val rowDF = filterDF.select($"state", $"constituency", $"candidate_name", $"sex", $"age", $"partyname", $"partysymbol", $"total", $"totalvoters",
      row_number.over(Window.partitionBy($"state", $"constituency").orderBy($"total".desc)).as("rank"))
    rowDF.show()

    val finalResult = rowDF.filter($"rank" === 1).
      select($"state", $"constituency", $"candidate_name", $"sex", $"age", $"partyname", $"partysymbol", $"total", $"totalvoters",
        count($"partyname").over(Window.partitionBy($"partyname")).as("nationalwise_party_mp_count"),
        count($"constituency").over(Window.partitionBy($"state")).as("state_mp_count"),
        count($"partyname").over(Window.partitionBy($"state", $"partyname")).as("statewisepartywon_mp_count"))
      .sort($"partyname")

    //finalResult.show(700)
    finalResult.write.partitionBy("state").mode("overwrite").saveAsTable("Eresults")

    spark.sql("select *from Eresults where state='Andhra Pradesh'").show(45)
  }

  def windowAnalyticFunctions() {
    // Create Sample Dataframe
    /*====================================================================================================
     * Rank salary within each department
     * Rank():-
     * SELECT empno,deptno,sal,RANK() OVER (partition by deptno ORDER BY sal desc) as rank FROM emp;
     * ======================================================================================================
    */

    /* val partitionWindow = Window.partitionBy($"deptno").orderBy($"sal".desc)
    val rankTest = rank().over(partitionWindow)
    empDF.select($"*", rankTest as "rank").show() */

    val rankDF = empDF.select($"*", rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("Rank"))
    rankDF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/rank")

    /*====================================================================================================
     * Dense Rank salary within each department
     * DENSE_RANK():-
     * SELECT empno,deptno,sal,DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal desc) as dense_rank FROM emp;
     * ======================================================================================================
    */

    val dense_rankDF = empDF.select($"*", dense_rank().over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("dense_Rank"))
    dense_rankDF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/dense_rank")

    /*====================================================================================================
     * ROW_NUMBER() within each department
     * ROW_NUMBER():-
     * SELECT empno,deptno,sal,ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal desc) as rno FROM emp;
     * ======================================================================================================
    */

    val row_NumberDF = empDF.select($"*", row_number().over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("row_number"))
    row_NumberDF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/row_number")

    /*====================================================================================================
     * Running Total (Salary) within each department
     * SUM():-
     * SELECT empno,deptno,sal,SUM(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as running_total FROM emp;
     * ======================================================================================================
    */

    val running_totalrDF = empDF.select($"*", sum($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("running_total"))
    running_totalrDF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/running_total_sum")

    /*====================================================================================================
     * Avg Total (Salary) within each department
     * AVG():-
     * SELECT empno,deptno,sal,AVG(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as avg_total FROM emp;
     * ======================================================================================================
    */

    val avg_totalrDF = empDF.select($"*", avg($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("avg_total"))
    avg_totalrDF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/avg_total")

    /*====================================================================================================
     * Max (Salary) within each department
     * Max():-
     * SELECT empno,deptno,sal,MAX(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as max_sal FROM emp;
     * ======================================================================================================
    */

    val max_sal_DF = empDF.select($"*", max($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("max_sal"))
    max_sal_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/max_sal")

    /*====================================================================================================
     * Min (Salary) within each department
     * Min():-
     * SELECT empno,deptno,sal,MIN(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as min_sal FROM emp;
     * ======================================================================================================
    */

    val min_sal_DF = empDF.select($"*", min($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("min_sal"))
    min_sal_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/min_sal")

    /*====================================================================================================
     * count within each department
     * COUNT():-
     * SELECT empno,deptno,sal,Count(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as count FROM emp;
     * ======================================================================================================
    */

    val count_DF = empDF.select($"*", count($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("count"))
    count_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/count")

    /*====================================================================================================
     * Lead(Next value) within each department
     * Lead():-
     * SELECT empno,deptno,sal,lead(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as lead_value FROM emp;
     * ======================================================================================================
    */

    val lead_DF = empDF.select($"*", lead($"sal", 1, 0).over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("lead_value"))
    lead_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/lead_value")

    /*====================================================================================================
     * Lag(previous value) within each department
     * Lag():-
     * SELECT empno,deptno,sal,lag(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as lag_value FROM emp;
     * ======================================================================================================
    */

    val lag_DF = empDF.select($"*", lag($"sal", 1, 0).over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("lag_value"))
    lag_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/lag_value")

    /*====================================================================================================
     * first_value within each department
     * FIRST_VALUE():-
     * SELECT empno,deptno,sal,first_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as first_value FROM emp;
     * ======================================================================================================
    */

    val first_value_DF = empDF.select($"*", first($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("first_value"))
    first_value_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/first_value")

    /*====================================================================================================
     * last_value within each department
     * LAST_VALUE():-
     * SELECT empno,deptno,sal,last_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as last_value FROM emp;
     * ======================================================================================================
    */

    val last_value_DF = empDF.select($"*", last($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)).as("last_value"))
    last_value_DF.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/last_value")

  }

  def LagPreviousDifference() {

    val lag_DF_diff = empDF.select($"*",(lag($"sal", 1, 0).over(Window.partitionBy($"deptno").orderBy($"sal".desc))- $"sal").as("lag_value"))
    lag_DF_diff.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
      csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/lag_value_difference")

    val lead_DF_diff= empDF.select($"*", (lead($"sal", 1, 0).over(Window.partitionBy($"deptno").orderBy($"sal".desc))-$"sal").as("lead_value"))
    lead_DF_diff.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
    csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/lead_value_difference")

    /*====================================================================================================
     * Define new window partition to operate on row frame
     * SELECT empno,deptno,sal,last_value(sal) OVER (PARTITION BY deptno ORDER BY sal desc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as last_val FROM emp;
     * ===================================================================================================
     */

    val pwindowedWithUnbounded = empDF.select($"*", last($"sal").over(Window.partitionBy($"deptno").orderBy($"sal".desc)
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)).alias("windowedWithUnbounded"))

    pwindowedWithUnbounded.coalesce(1).write.mode("overwrite").option("header","true").option("inferSchema","true").
      csv("D:/Spark_Applications/SparkPractice/ApacheSpark2_Applications/OutPut/pwindowedWithUnbounded")

  }

}