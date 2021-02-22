/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import java.io.File
import java.util.Collections

import scala.collection.JavaConverters._
import scala.util.Random
import com.alibaba.sparkcube.ZIndexUtil
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.util.Utils

class ZIndexEndToEndSuite extends QueryTest with SQLMetricsTestUtils {

  // 二维int例子
  test("int basic case") {

    val format = "json"

    withTempDir {
      dir =>
        val dataLength = 8
        spark.udf.register("vec", (i: Int) => (0 until i).toArray)
        import org.apache.spark.sql.functions._
        spark.range(0, dataLength)
          .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1", "col_2"),
          fileNum = 16, format = format
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == 2 or col_2 == 2", 7, 15),
            FilterTestInfo("col_1 == 2 and col_2 == 2", 1, 1),
            FilterTestInfo("col_1 == 2", 4, 8),
            FilterTestInfo("col_2 == 2", 4, 8),
            FilterTestInfo("2 == col_1", 4, 8),
            FilterTestInfo("1 == 1", 16, 64),
            FilterTestInfo("1 == 2", 16, 0),
            FilterTestInfo("1 == 1 and col_1 == 2", 4, 8),
            FilterTestInfo("1 == 2 and col_1 == 2", 4, 0),
            FilterTestInfo("1 == 1 or col_1 == 2", 16, 64),
            FilterTestInfo("1 == 2 or col_1 == 2", 4, 8),
            FilterTestInfo("1 == 1 and 2 == 2", 16, 64),
            FilterTestInfo("col_1 < 2", 4, 16),
            FilterTestInfo("col_1 <= 2", 8, 24),
            FilterTestInfo("col_1 > 5", 4, 16),
            FilterTestInfo("col_1 >= 5", 8, 24),
            FilterTestInfo("col_1 < 3", 8, 24),
            FilterTestInfo("col_1 <= 3", 8, 32)
          )
        )
    }
  }

  test("int basic case 2") {

    val format = "parquet"

    withTempDir {
      dir =>
        import org.apache.spark.sql.functions._
        spark.range(0, 1000)
          .selectExpr("id as col_1", "'x' as col_2")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1"),
          fileNum = 16, format = format
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
//          .filter("col_1 == 2")
//        assert(df.inputFiles.size == 1)
//        assert(df.collect().size == 1)
        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == 2", 1, 1, Some(Row(2, "x") :: Nil))
          )
        )
    }
    Thread.sleep(Int.MaxValue)
  }

  // 三维int例子
  test("int basic case 3") {

    val format = "parquet"

    withTempDir {
      dir =>
        val dataLength = 8
        spark.udf.register("vec", (i: Int) => (0 until i).toArray)
        import org.apache.spark.sql.functions._
        spark.range(0, dataLength)
          .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
          .selectExpr("*", s"explode(vec(${dataLength})) as col_3")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1", "col_2", "col_3"),
          fileNum = 64, format = format
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == 2 or col_2 == 2 or col_3 == 2", 48 - 9 - 2, 169)
          )
        )
    }
  }

  test("int basic case 4") {

    val format = "parquet"

    withTempDir {
      dir =>
        val dataLength = 8
        spark.udf.register("vec", (i: Int) => (0 until i).toArray)
        import org.apache.spark.sql.functions._
        spark.range(0, dataLength)
          .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
          .selectExpr("0 - col_1 as col_1", "0 - col_2 as col_2")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1", "col_2"),
          fileNum = 16, format = format
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
          .filter("col_1 == -2 or col_2 == -2")

        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == -2 or col_2 == -2", 7, 15)
//            FilterTestInfo("col_1 == 2 and col_2 == 2", 1, 1),
//            FilterTestInfo("col_1 == 2", 4, 8),
//            FilterTestInfo("col_2 == 2", 4, 8),
//            FilterTestInfo("2 == col_1", 4, 8),
//            FilterTestInfo("1 == 1", 16, 64),
//            FilterTestInfo("1 == 2", 16, 0),
//            FilterTestInfo("1 == 1 and col_1 == 2", 4, 8),
//            FilterTestInfo("1 == 2 and col_1 == 2", 4, 0),
//            FilterTestInfo("1 == 1 or col_1 == 2", 16, 64),
//            FilterTestInfo("1 == 2 or col_1 == 2", 4, 8),
//            FilterTestInfo("1 == 1 and 2 == 2", 16, 64)
          )
        )
    }
  }

  test("string basic case") {

    val format = "parquet"

    withTempDir {
      dir =>
        val sparkSession = spark
        import org.apache.spark.sql.functions._
        import sparkSession.implicits._
        (Seq("abcdefG") ++
          (1 to 127).map(i => {
            val size = Random.nextInt(50)
            (1 to size).map(j => {
              ('a'.toInt + Random.nextInt(25)).toChar.toString
            }).mkString("")
          })).toDF("id")
          .selectExpr("id as col_1", "'x' as col_2")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1"),
          fileNum = 16, format = format
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == 'abcdefG'", 1, 1)
          )
        )

        ('a'.toInt until ('a'.toInt + 26)).map(_.toChar.toString)
          .toDF("col_char")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_char"),
          fileNum = 6, format = format
        )
        testFilters(
          spark.read
            .format(format)
            .load(dir.getCanonicalPath),
          Seq(
            FilterTestInfo("col_char < 'e'", 1, 4,
              Some(Row("a") :: Row("b") :: Row("c") :: Row("d") :: Nil))
          )
        )
    }

  }

  // 分区表
  test("partitioned table") {

    val format = "parquet"

    withTempDir {
      dir =>
        val sparkSession = spark
        import org.apache.spark.sql.functions._
        import sparkSession.implicits._
        val df2 = Seq((2, "y")).toDF("col_1", "col_2")
        spark.range(0, 1000)
          .selectExpr("id as col_1", "'x' as pid")
          .union(df2)
          .write
          .format(format)
          .partitionBy("pid")
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_1"),
          fileNum = 16, format = format, partitionCols = Some(Seq("pid"))
        )

        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
        testFilters(df,
          Seq(
            FilterTestInfo("col_1 == 2", 2, 2),
            FilterTestInfo("col_1 == 2 and pid == 'x'", 1, 1)
          )
        )
    }
  }

  test("map column ") {
    val format = "json"
    withTempDir {
      dir =>
        val sparkSession = spark
        import sparkSession.implicits._
        (1 to 100).map(i => {
          val info = if (i < 50) "a" else "b"
          (i, Map("info" -> info, "name" -> s"name_${i}"))
        }).toDF("col_1", "col_2")
          .write
          .format(format)
          .mode("overwrite")
          .save(dir.getCanonicalPath)

        ZIndexUtil.createZIndex(
          spark, format, dir.getCanonicalPath, Array("col_2['info']"),
          fileNum = 5, format = format
        )
        val df = spark.read
          .format(format)
          .load(dir.getCanonicalPath)
//          .filter("col_2['info'] == 'a'")
//          .show()
        testFilters(df,
          Seq(
            FilterTestInfo("col_2['info'] == 'a'", 2, 49)
          )
        )
        Thread.sleep(Int.MaxValue)
    }
  }

  private def testFilters(input: DataFrame, info: Seq[FilterTestInfo]): Unit = {
    info.foreach(i => {
      val df = input.filter(i.condition)
      testNumFilesAndRowCount(df, i.numFiles, i.numRowCount, i.expectedRow)
      val exchangeCondition = exchangeLeftRight(i.condition)
      testNumFilesAndRowCount(
        input.filter(exchangeCondition),
        i.numFiles,
        i.numRowCount,
        i.expectedRow)
    })
  }

  private def testNumFilesAndRowCount(df: DataFrame,
                                      numFiles: Long,
                                      numRowCount: Long,
                                      expectedRow: Option[Seq[Row]] = None): Unit = {
    val res = df.collect()
    assert(res.length== numRowCount)
    df.queryExecution.executedPlan.collectLeaves().foreach(l => {
      l.metrics.foreach {
        case (key, metric) if (key == "numFiles") =>
          assert(metric.value == numFiles)
        case _ =>
      }
    })
    expectedRow.foreach(expected => {
      checkAnswer(df, expected)
    })
  }

  override def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      Utils.deleteRecursively(dir)
      Utils.deleteRecursively(
        new File(ZIndexUtil.tableIndexPath(dir.getName))
      )
    }
  }

  override protected def spark: SparkSession = {
    SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()
  }

  case class FilterTestInfo(condition: String,
                            numFiles: Long,
                            numRowCount: Long,
                            expectedRow: Option[Seq[Row]] = None)

  //TODO: (fchen) 不带空格处理
  private def exchangeLeftRight(condition: String): String = {
    val opMap = Map(
      ">" -> "<",
      "<" -> ">",
      "=" -> "=",
      "==" -> "==",
      ">=" -> "<=",
      "<=" -> ">="
    )
    val arr = condition.split(" ")
      .filter(_ != "")

    val opIndex = arr.zipWithIndex.filter {
      case (op, index) => opMap.keySet.contains(op)
    }

    val res = arr.toBuffer.asJava
    opIndex.foreach {
      case (op, index) =>
        Collections.swap(res, index - 1, index + 1)
        res.set(index, opMap(op))
    }
    res.asScala.toArray.mkString(" ")
  }

  test("aa") {
//    spark.range(1, 1000)
////      .sort("id")
//      .write
//      .format("json")
//      .mode("overwrite")
//      .save("/tmp/aaa")
    val f = spark.read
      .format("json")
      .load("/tmp/aaa")
//    f.queryExecution.analyzed.foreach {
//      case logicalRelation @ LogicalRelation(
//        relation @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _) =>
//        println(location.rootPaths.mkString(""))
//      case _ =>
//    }
//      .sort("id")
//      .write
//      .format("json")
//      .mode("overwrite")
//      .save("/tmp/bbb")
//    spark.range(0 , 1000)
      .selectExpr("id", "cast(rand() * 1000 as int ) % 3 as col_1")
      .createOrReplaceTempView("aaa")

    spark.sql("select col_1, id, DENSE_RANK() over(partition by col_1 order by id) as nid from aaa")
        .show(200)
    Thread.sleep(Int.MaxValue)

//    val sparkSession = spark
//    import sparkSession.implicits._
//    val df = (0 to 10000).toDF("id")
//    val df2 = Seq("a", "b", "b", "c", "f", "c").toDF("id")
//    generateGlobalRankId(df2, "id", "id_rank")
  }

  def generateGlobalRankId(df: DataFrame, colName: String, rankColName: String): DataFrame = {

    val inputSchema = df.schema.map(_.name)
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val partDF = df.repartition(1000)
      .orderBy(colName)
      .withColumn("partitionId", spark_partition_id())

    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy("partitionId").orderBy(colName)

    val rankDF = partDF.withColumn("local_rank", dense_rank().over(w))

    val tempDf =
      rankDF.groupBy("partitionId").agg(max("local_rank").alias("max_rank"))

    val w2 = Window.orderBy("partitionId").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val statsDf = tempDf.withColumn("cum_rank", sum("max_rank").over(w2))

    val joinDF = statsDf.alias("l")
      .join(
        statsDf.alias("r"), $"l.partitionId" === $"r.partitionId" +1, "left")
      .select(
        col("l.partitionId"),
        coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor"))


    val finalDF = rankDF.join(
      broadcast(joinDF), Seq("partitionId"),"inner")
      .withColumn(rankColName, $"local_rank" + $"sum_factor" - 1)

    finalDF.selectExpr((rankColName :: Nil ++ inputSchema): _*)
      .printSchema()
    finalDF.selectExpr((rankColName :: Nil ++ inputSchema): _*)
//    finalDF

//        .filter("sum_factor")
//      .show


  }
}
