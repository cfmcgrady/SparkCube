// scalastyle:off
package com.alibaba.sparkcube

import org.apache.spark.sql.catalyst.expressions.HiveHash
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.{ArrayZIndexV2, ReplaceHadoopFsRelation, ZIndexFileInfoV2, ZIndexMetadata}

import scala.collection.mutable

object Test {
//  val length = 10
  val dataLength = 8
  def main(args: Array[String]): Unit = {
//    zIndex(length, Seq(0, 2))
//    System.exit(0)
//    println(Integer.toBinaryString(20))
//    println(Integer.toBinaryString(9))
    val spark = SparkSession
      .builder()
      .appName("Spark z order Example")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()

    spark.udf.register("vec", (i: Int) => (0 until i).toArray)

    spark.udf.register("zIndex", (vec: Seq[Int]) => zIndex(dataLength, vec))

    spark.udf.register("arrayZIndex", (vec: Seq[Int]) => ArrayZIndexV2.create(vec).indices)

//    ZIndexUtil.createZIndex(spark, "tb_input", Array("col_1", "col_2"), fileNum = 16)
    arrayZindexTest(spark)
//    testCase2(spark)
    System.exit(0)

    import org.apache.spark.sql.functions._

    val df = spark.range(0, dataLength)
      .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
      .selectExpr("*", "zIndex(array(col_1, col_2)) as zIndex")
      // 按照4条记录一个文件来划分
//      .repartitionByRange()
//      .repartitionByRange(expr("zIndex / 4"))
      .repartitionByRange(16, col("zIndex"))

    df.explain(true)

//    df.show()
    df.rdd.mapPartitionsWithIndex {
      case (index, row) =>
        println(index + "-----" + row.mkString(","))
        Iterator.empty
    }.foreach(println)
    df.write.format("json").mode("overwrite").save("/tmp/dd.json")



    val ndf = spark.read.format("json").load("/tmp/dd.json")
    ndf.createOrReplaceTempView("tb_input")

    val stat = spark.sql(
      """
        |  SELECT row_number() OVER (ORDER BY file) AS file_id, file,
        |       count(*) as numRecords, min(zIndex), max(zIndex)
        |  FROM (
        |  SELECT input_file_name() AS file, * FROM tb_input)
        |  GROUP BY file
        |""".stripMargin)
//    stat.show()

    val q = ndf.filter("col_1 == 2 or col_2 == 2")
    q.explain(true)
    ReplaceHadoopFsRelation.tt(q.queryExecution.analyzed)
    q.show()

  }

  // TODO: (fchen)返回值需要替换成Array[Long]，否则可能溢出，如果z order处理的列比较多的话
  def zIndex(maxValue: Int, points: Seq[Int]): Int = {

    // 十进制返回值
    var res = 0

    val binaryLen = Integer.toBinaryString(maxValue).size
    for (i <- (0 until binaryLen).reverse) {

      val mask = 1 << i

      points.zipWithIndex.foreach
      {
        case (p, index) => {
          if ((p & mask) != 0) {
            print(1)
//            println(i)
//            println(i * points.size - index)
            val idx = (i + 1) * points.size - index - 1
            println(idx)
            res += 1 << idx
          } else {
            print(0)
          }
        }
      }
    }
    println()
    println(res)
    res
  }

  def computeStatistic(spark: SparkSession): Unit = {
    val stat = spark.sql(
      """
        |  SELECT row_number() OVER (ORDER BY file) AS file_id, file,
        |       count(*) as numRecords, min(zIndex) as minZIndex, max(zIndex) as maxZIndex
        |  FROM (
        |  SELECT input_file_name() AS file, * FROM tb_input)
        |  GROUP BY file
        |""".stripMargin)
    stat.show()
    val metadata = stat.collect()
      .map(r => {
        ZIndexFileInfoV2(
          r.getAs[String]("file"),
          r.getAs[Long]("numRecords"),
          ArrayZIndexV2(r.getAs[mutable.WrappedArray[Long]]("minZIndex").map(_.toInt).toArray),
          ArrayZIndexV2(r.getAs[mutable.WrappedArray[Long]]("maxZIndex").map(_.toInt).toArray)
        )
      })

//    println("dxdx")
//    metadata.foreach(i => {
//      println(i.file)
//      println(i.minIndex.toBinaryString)
//      println(i.maxIndex.toBinaryString)
//    })

    ReplaceHadoopFsRelation.metadata = ZIndexMetadata(
      Integer.toBinaryString(dataLength - 1).length,
      Map("col_1" -> 0, "col_2" -> 1),
      metadata
    )
    ReplaceHadoopFsRelation.relationMetadata += ("json.`file:/tmp/ee.json`" -> ReplaceHadoopFsRelation.metadata)
  }


  def arrayZindexTest(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    val df = spark.range(0, dataLength)
      .selectExpr("id as col_1", s"explode(vec(${dataLength})) as col_2")
      .selectExpr("*", "arrayZIndex(array(col_1, col_2)) as zIndex")
      .repartitionByRange(16, col("zIndex"))
    df.printSchema()

    df.explain(true)

    df.write.format("json").mode("overwrite").save("/tmp/ee.json")



    val ndf = spark.read.format("json").load("/tmp/ee.json")
    ndf.createOrReplaceTempView("tb_input")

    val stat = spark.sql(
      """
        |  SELECT row_number() OVER (ORDER BY file) AS file_id, file,
        |       count(*) as numRecords, min(zIndex), max(zIndex)
        |  FROM (
        |  SELECT input_file_name() AS file, * FROM tb_input)
        |  GROUP BY file
        |""".stripMargin)
    //    stat.show()
    computeStatistic(spark)

    val q = ndf.filter("col_1 == 2 or col_2 == 2")
    q.explain(true)
//    ReplaceHadoopFsRelation.tt(q.queryExecution.analyzed)
    q.show()

  }

  def testCase2(spark: SparkSession): Unit = {
      import org.apache.spark.sql.functions._
      val df = spark.range(0, 1000)
        .union(spark.range(0, 10))
        .union(spark.range(0, 100))
        .selectExpr("id as col_1", s"'x' as col_2")
        .selectExpr("*", "arrayZIndex(array(col_1)) as zIndex")
        .repartitionByRange(16, col("zIndex"))

      df.write.format("json").mode("overwrite").save("/tmp/gg.json")
      val ndf = spark.read.format("json").load("/tmp/gg.json")
      ndf.createOrReplaceTempView("tb_input")

    val stat = spark.sql(
      """
        |  SELECT row_number() OVER (ORDER BY file) AS file_id, file,
        |       count(*) as numRecords, min(zIndex) as minZIndex, max(zIndex) as maxZIndex
        |  FROM (
        |  SELECT input_file_name() AS file, * FROM tb_input)
        |  GROUP BY file
        |""".stripMargin)
    stat.show()
    val metadata = stat.collect()
      .map(r => {
        ZIndexFileInfoV2(
          r.getAs[String]("file"),
          r.getAs[Long]("numRecords"),
          ArrayZIndexV2(r.getAs[mutable.WrappedArray[Long]]("minZIndex").map(_.toInt).toArray),
          ArrayZIndexV2(r.getAs[mutable.WrappedArray[Long]]("maxZIndex").map(_.toInt).toArray)
        )
      })

    ReplaceHadoopFsRelation.metadata = ZIndexMetadata(
      Integer.toBinaryString(1000 - 1).length,
      Map("col_1" -> 0),
      metadata
    )
    ReplaceHadoopFsRelation.relationMetadata += ("JSON.`file:/tmp/gg.json`" -> ReplaceHadoopFsRelation.metadata)

    val q = ndf.filter("col_1 == 2")
    q.explain(true)
    //    ReplaceHadoopFsRelation.tt(q.queryExecution.analyzed)
    q.show()

  }

  def testRepartitionByRange(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark z order Example")
//      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df = Seq(
      Array(0),
      Array(5),
      Array(1, 5),
      Array(1, 3),
      Array(2, 3),
      Array(1, 3)
    ).toDF("id")
      .repartitionByRange(2, col("id"))

    df.rdd.mapPartitionsWithIndex {
      case (index, row) =>
        println(index + "-----" + row.mkString(","))
        Iterator.empty
    }.foreach(println)
  }
}

case class ZIndexFileInfo(
    file: String,
    numRecords: Int,
    minIndex: Int,
    maxIndex: Int)
