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

import com.alibaba.sparkcube.ZIndexUtil
import org.scalatest.BeforeAndAfterEach

class ZIndexEndToEndSuite extends QueryTest with SQLMetricsTestUtils with BeforeAndAfterEach {

  val testFileFormatSet = Set("parquet")
  val sparkSession: SparkSession = spark
  import sparkSession.implicits._

  override def afterEach(): Unit = {
    ReplaceHadoopFsRelation.relationMetadata = Map.empty
  }

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
        FilterTestInfo("col_2['info'] = 'a'", 3, 33),
        FilterTestInfo("col_2['info'] >= 'a'", 4, 67)
      ),
      fileNum = 5
    )
  }

  test("column contains null value case.") {
    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")

    runZIndexTest(
      input, Array("col_1"),
      Seq(
        FilterTestInfo("col_1 = '1'", 1, 1),
        FilterTestInfo("col_1 = null", 0, 0),
        FilterTestInfo("col_1 < '3'", 3, 11),
        FilterTestInfo("col_1 > '8'", 3, 11),
        FilterTestInfo("col_1 is null", 10, 50),
        FilterTestInfo("col_1 is not null", 10, 50),
        FilterTestInfo("col_1 in ('1', '9')", 2, 2),
        FilterTestInfo("col_1 in (null)", 0, 0),
        FilterTestInfo("col_1 not in ('1', '9')", 10, 48),
        FilterTestInfo("col_1 not in ('1', '9', null)", 0, 0),
//        FilterTestInfo("col_1 in (select null)", 0, 0)
//        FilterTestInfo("col_1 not in (select null)", 0, 0)
        FilterTestInfo("col_1 in ('1', null)", 1, 1),
        FilterTestInfo("col_1 like '1%'", 2, 6),
        FilterTestInfo("col_1 <=> null", 10, 50),
        FilterTestInfo("col_1 <=> '1'", 1, 1)
      ),
      fileNum = 20
    )
  }

//  test("filter with StingStartWith should work fine.") {
//    withTempDir {
//      dir =>
//        (1 to 200).foreach(fileNum => {
//          val format = "parquet"
//          val input = (1 to 10000).flatMap(_ => {
//            val size = Random.nextInt(50) + 20
//
//            var res: List[String] = List()
//
//            (0 to size).foreach(j => {
//              val last = res.lift(j - 1)
//              res ++= (last.getOrElse("") + ('a'.toInt + Random.nextInt(25)).toChar.toString) :: Nil
//            })
//            res
//          })
//
//          input.toDF("id")
//            .write
//            .format(format)
//            .mode("overwrite")
//            .save(dir.getCanonicalPath)
//          ZIndexUtil.createZIndex(
//            spark, format, dir.getCanonicalPath, Array("id"),
//            fileNum = fileNum, format = format
//          )
//          val df = spark.read
//            .format(format)
//            .load(dir.getCanonicalPath)
//
////          val maxString = input.sortBy(s => s).last
//          val maxString = input.lift(50).get
//          (1 to maxString.length).foreach(i => {
//            val likeString = maxString.slice(0, i)
//            checkAnswer(
//              df.filter(s"id like '$likeString%'"),
//              input.toDF("id").filter(s"id like '$likeString%'").collect())
//          })
//        })
//    }
//
//  }

  test("TODO:(fchen) handle subquery case?") {
    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")

    runZIndexTest(
      input, Array("col_1"),
      Seq(
//        FilterTestInfo("col_1 in (select null)", 20, 0),
//        FilterTestInfo("col_1 not in (select null)", 20, 0)
//        FilterTestInfo("col_1 exists (select '1')", 20, 0)
      ),
      fileNum = 20
    )

  }

  test("TODO:(fchen) handle implicit conversion case.") {
    val input = (1 to 100).map {
      case i if i % 2 == 0 => null
      case i => i.toString
    }.toDF("col_1")

    runZIndexTest(
      input, Array("col_1"),
      Seq(
        FilterTestInfo("col_1 = 1", 20, 1)
      ),
      fileNum = 20
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

            assert(ReplaceHadoopFsRelation.relationMetadata.headOption.isDefined)
            assert(ReplaceHadoopFsRelation.relationMetadata.head._2.fileMetadata.size == 20)
            val Array(tail, rest @ _*) =
              ReplaceHadoopFsRelation.relationMetadata.head._2.fileMetadata.sortBy(_.file).reverse
            assert(
              tail.columnStatistics == Map(
                "col_1.k2" -> ColumnStatistics("col_1.k2", Some("b"), Some("b"), true)
              )
            )
            rest.foreach(f => {
              assert(
                f.columnStatistics == Map(
                  "col_1.k2" -> ColumnStatistics("col_1.k2", None, None, true)
                )
              )
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

            val filterTestInfoWithResult = filterInfo.map {
              case filterTestInfo: FilterTestInfo if filterTestInfo.expectedRow.isDefined =>
                filterTestInfo
              case filterTestInfo: FilterTestInfo =>
                filterTestInfo.copy(
                  expectedRow = Option(input.filter(filterTestInfo.condition).collect())
                )
            }
            testFilters(df, filterTestInfoWithResult)
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
    df.explain(true)
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
      "<=" -> ">=",
      "<=>" -> "<=>"
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
//    val df = Seq("1", "2", "3")
//      .toDF("col")
//    df.show()
//    df.printSchema()
//    df.filter("col > 1")
//      .show
//    spark.sql("select 1 > null and 1 == 1")
//      .show

//    Seq("a", "b", "c").toDF("ca").write.mode("overwrite").save("/tmp/aaa")
//    Seq("a").toDF("ca").write.mode("overwrite").save("/tmp/bbb")
//
//    spark.read.load("/tmp/bbb").createOrReplaceTempView("bbb")
//    spark.read.load("/tmp/aaa")
//      .filter("ca in (select ca from bbb)")
//      .filter("ca in (select 'a' as ca)")
//      .filter("ca exists (select ca = 'a')")
//      .filter("ca inset('a', 'b')")
//      .show
//      .explain(true)
    spark.read.json

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
