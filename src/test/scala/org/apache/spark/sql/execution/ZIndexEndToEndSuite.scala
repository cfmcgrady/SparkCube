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

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.util.Utils

import com.alibaba.sparkcube.{SchemaUtils, ZIndexUtil}

class ZIndexEndToEndSuite extends QueryTest with SQLMetricsTestUtils {

  val testFileFormatSet = Set("parquet", "json")

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

  test("map column.") {
    testFileFormatSet.foreach {
      format =>
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
                FilterTestInfo("col_2['info'] == 'a'", 3, 49)
              )
            )
        }
    }
  }

  test("map column test case.") {
    val sparkSession = spark
    import sparkSession.implicits._
    val input = (1 to 100).map(i => {
      val info = if (i < 50) "a" else "b"
      (i, Map("info" -> info, "name" -> s"name_${i}"))
    }).toDF("col_1", "col_2")

    runZIndexTest(
      input, Array("col_2['info']"),
      Seq(
        FilterTestInfo("col_2['info'] == 'a'", 3, 49)
      ),
      fileNum = 5
    )

    val input2 = (1 to 100).map(i => {
      val info = if (i < 50) "a" else "b"
      val map = if (i % 3 == 0) {
        Map("name" -> s"name_$i")
      } else {
        Map("info" -> info, "name" -> s"name_${i}")
      }
      (i, map)
    }).toDF("col_1", "col_2")

    runZIndexTest(
      input2, Array("col_2['info']"),
      Seq(
        FilterTestInfo("col_2['info'] = 'a'", 3, 33)
//        FilterTestInfo("col_2['info'] >= 'a'", 5, 67)
      ),
      fileNum = 5
    )
  }

  test("generate right zindex info") {
    testFileFormatSet.foreach {
      format =>
        withTempDir {
          dir =>
            val sparkSession = spark
            import sparkSession.implicits._
            val df = (1 to 100).map {
              case i if i == 3 => Map("k1" -> "a", "k2" -> "b")
              case _ => Map("k1" -> "a")
            }.toDF("col_1")

            df.write
              .format(format)
              .mode("overwrite")
              .save(dir.getCanonicalPath)

            ZIndexUtil.createZIndex(
              spark, format, dir.getCanonicalPath, Array("col_1['k2']"),
              fileNum = 20, format = format
            )
//            ReplaceHadoopFsRelation.relationMetadata.head
//                ._2
//                .fileMetadata
//                .foreach(println)
//            Thread.sleep(Int.MaxValue)
            assert(ReplaceHadoopFsRelation.relationMetadata.headOption.isDefined)
            assert(ReplaceHadoopFsRelation.relationMetadata.head._2.fileMetadata.size == 20)
            val Array(tail, rest @ _*) =
              ReplaceHadoopFsRelation.relationMetadata.head._2.fileMetadata.sortBy(_.file).reverse
            assert(tail.minMax == Map("col_1.k2" -> ColumnMinMax(null, "b")))
            rest.foreach(f => {
              assert(f.minMax == Map("col_1.k2" -> ColumnMinMax(null, null)))
            })
        }
    }

  }

  private def runZIndexTest(input: DataFrame,
                            indexCols: Array[String],
                            filterInfo: Seq[FilterTestInfo],
                            fileNum: Int = 16): Unit = {
    testFileFormatSet.foreach {
      format =>
        withTempDir {
          dir =>
            input.write
              .format(format)
              .mode("overwrite")
              .save(dir.getCanonicalPath)

            ZIndexUtil.createZIndex(
              spark, format, dir.getCanonicalPath, indexCols,
              fileNum = fileNum, format = format
            )
            val df = spark.read
              .format(format)
              .load(dir.getCanonicalPath)
            testFilters(df, filterInfo)
        }
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
    val df = spark.read.load("file:///Users/fchen/Project/bigdata/SparkCube/tmp/zindex/spark-a11c875b-50c3-47c1-99d3-276981741135/part-00019-9eb05fc4-3297-4270-b746-9b949db84c06-c000.snappy.parquet")
    df.show(truncate = false)
    df.createOrReplaceTempView("aa")
    spark.sql("select min(col_1['k2']) as min, max(col_1['k2']) as max from aa")
      .show



    val sparkSession = spark
    import sparkSession.implicits._
    val df2 = Seq(
      "a", "b", "c", null
    ).toDF("ca")
    df2.createOrReplaceTempView("bb")

//    df.createOrReplaceTempView("aa")
    spark.sql("select min(ca) as min, max(ca) as max from bb")
      .show

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
