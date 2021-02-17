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

import com.alibaba.sparkcube.ZIndexUtil
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.util.Utils

import scala.util.Random

class ZIndexEndToEndSuite extends SparkFunSuite with SQLMetricsTestUtils {

//  protected def withTempDir(f: File => Unit): Unit = {
//    val dir = Utils.createTempDir().getCanonicalFile
//    try f(dir) finally {
//      Utils.deleteRecursively(dir)
//    }
//  }

  // 二维int例子
  test("int basic case") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

    val format = "parquet"

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
          .filter("col_1 == 2 or col_2 == 2")
        df.explain(true)
        df.show
//        assert(df.inputFiles.size == 7)
        assert(df.collect().size == 15)
        Thread.sleep(Int.MaxValue)
    }
  }

  test("int basic case 2") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

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
          .filter("col_1 == 2")
        assert(df.inputFiles.size == 1)
        assert(df.collect().size == 1)
    }
  }

  // 三维int例子
  test("int basic case 3") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

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
          .filter("col_1 == 2 or col_2 == 2 or col_3 == 2")
        df.explain(true)
        df.show
        assert(df.inputFiles.size == 48 - 9 - 2)
        assert(df.collect().size == 169)
    }
  }

  test("int basic case 4") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

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
        df.explain(true)
        df.show
        assert(df.inputFiles.size == 8)
        assert(df.collect().size == 15)
    }
  }

  test("string basic case 2") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

    val format = "parquet"

    withTempDir {
      dir =>
        import org.apache.spark.sql.functions._
        import spark.implicits._
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
          .filter("col_1 == 'abcdefG'")
        df.show
//        assert(df.inputFiles.size == 1)
        assert(df.collect().size == 1)
    }

  }

  // 分区表
  test("partitioned table") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

    val format = "parquet"

    withTempDir {
      dir =>
        import org.apache.spark.sql.functions._
        import spark.implicits._
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
          .filter("col_1 == 2")
//        assert(df.inputFiles.size == 1)
        df.show
//        assert(df.collect().size == 1)
    }
  }

  test("aa") {
    val spark = SparkSession
      .builder()
      .appName("test-spark")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

    val format = "json"

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df2 = Seq((2, "y")).toDF("col_1", "col_2")
    spark.range(0, 1000)
      .selectExpr("id as col_1", "'x' as pid")
      .union(df2)
      .write
      .format(format)
      .partitionBy("pid")
      .mode("overwrite")
      .save("/tmp/aaa.json")

    ZIndexUtil.createZIndex(
      spark, format, "/tmp/aaa.json", Array("col_1"),
      fileNum = 16, format = format, partitionCols = Some(Seq("pid"))
    )

    val df = spark.read
      .format(format)
      .load("/tmp/aaa.json")
//      .filter("col_1 == 2 or (col_1 == 3 and col_1 == 4)")
    df.collect()
    df.queryExecution.executedPlan.collectLeaves().foreach(l => {
      println(l.getClass.getCanonicalName)
      l.metrics.foreach(println)
      println("----nunfiles----")
      l.metrics.get("numFiles").foreach(x => println(x.value))
      println("----nunfiles----")
    })

//    val df2 = Seq(1, 2, 3).toDF("id").limit(2)
//    df2.collect()
//    val metrics2 = df2.queryExecution.executedPlan.collectLeaves().head.metrics
//    df2.queryExecution.executedPlan
//    assert(metrics2.contains("numOutputRows"))
//    assert(metrics2("numOutputRows").value === 2)
    this.getSparkPlanMetrics(df, 1, Set(0)).foreach(println)
//    testSparkPlanMetrics()

        Thread.sleep(Int.MaxValue)
  }

  override protected def spark: SparkSession = SparkSession.active
}
