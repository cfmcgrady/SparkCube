// scalastyle:off
package com.alibaba.sparkcube

import org.apache.spark.sql.catalyst.expressions.HiveHash
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.{ArrayZIndexV2, ReplaceHadoopFsRelation, ZIndexFileInfoV2, TableMetadata}

import scala.collection.mutable
import scala.util.Random

object Test {
//  val length = 10
  val dataLength = 8
  def main(args: Array[String]): Unit = {
//    zIndex(length, Seq(0, 2))
//    test()
    testRepartitionByRange
    System.exit(0)
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

    ReplaceHadoopFsRelation.metadata = TableMetadata(
      "",
      null
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

    ReplaceHadoopFsRelation.metadata = TableMetadata(
      "",
      null
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

    df.explain(true)

    df.rdd.mapPartitionsWithIndex {
      case (index, row) =>
        println(index + "-----" + row.mkString(","))
        Iterator.empty
    }.foreach(println)
  }

  case class ConnRecord(src_ip: String, src_port: Int, dst_ip: String, dst_port: Int)

  def randomIPv4(r: Random) = Seq.fill(4)(r.nextInt(256)).mkString(".")
  def randomPort(r: Random) = r.nextInt(65536)

  def randomConnRecord(r: Random) = ConnRecord(
    src_ip = randomIPv4(r), src_port = randomPort(r),
    dst_ip = randomIPv4(r), dst_port = randomPort(r))

  case class TestResult(numFilesScanned: Long, numRowsScanned: Long, numRowsReturned: Long)

//  def testFilter(table: String, filter: String): TestResult = {
//    val query = s"SELECT COUNT(*) FROM $table WHERE $filter"
//
//    val(result, metrics) = collectWithScanMetrics(sql(query).as[Long])
//    TestResult(
//      numFilesScanned = metrics("filesNum"),
//      numRowsScanned = metrics.get("numOutputRows").getOrElse(0L),
//      numRowsReturned = result.head)
//  }

  def test(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark z order Example")
      .config("spark.sql.extensions", "org.apache.spark.sql.execution.PartitionExtensions")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    def mockData(): Unit = {
      (1 to 100 * 1000).map(i => {
        randomConnRecord(Random)
      }).toDF
        .repartition(100)
        .write
        .format("parquet")
        .save("/tmp/tb_log")
    }
//    mockData()

//    ZIndexUtil.createZIndex(
//      spark, "parquet", "/tmp/tb_log", Array("src_ip", "src_port", "dst_ip", "dst_port"),
//      fileNum = 100, format = "parquet"
//    )
//    ZIndexUtil.createZIndex(
//      spark, "parquet", "/tmp/tb_log", Array("src_ip"),
//      fileNum = 100, format = "parquet"
//    )

//    spark.read
//      .format("parquet")
//      .load("/tmp/zindex/tb_log")
//      .createOrReplaceTempView("conn_random")
//    spark.sql("select * from conn_random where dst_ip = '239.172.108.230'")
//      .show()
//      .filter("dst_ip = '239.172.108.230'")
//      .show()
//    Thread.sleep(Int.MaxValue)
//    spark.sql(
//      """
//        |  SELECT row_number() OVER (ORDER BY file) AS file_id,
//        |       count(*) as numRecords, min(src_ip), max(src_ip), min(src_port),
//        |       max(src_port), min(dst_ip), max(dst_ip), min(dst_port), max(dst_port)
//        |  FROM (
//        |  SELECT input_file_name() AS file, * FROM conn_random)
//        |  GROUP BY file
//        |""".stripMargin)
//      .show(100)

    spark.read
      .format("parquet")
      .load("/tmp/tb_log")
      .createOrReplaceTempView("conn_random_2")
//    spark.sql(
//      """
//        |  SELECT row_number() OVER (ORDER BY file) AS file_id,
//        |       count(*) as numRecords, min(src_ip), max(src_ip), min(src_port),
//        |       max(src_port), min(dst_ip), max(dst_ip), min(dst_port), max(dst_port)
//        |  FROM (
//        |  SELECT input_file_name() AS file, * FROM conn_random)
//        |  GROUP BY file
//        |""".stripMargin)
//      .show(100)
//    spark.range(0, 100).createOrReplaceTempView("ddd")
//    spark.sql(
//      """
//        | select row_number() over(order by src_ip) as src_ip_id, src_ip from conn_random_2
//        |""".stripMargin)
//      .show()

    Seq(
      "aa", "bb", "cc", "cc", "dd", "dd", "dd"
    ).zip(
      Seq(
        "a", "b", "b", "c", "d", "d", "f"
      )).toDF("c1", "c2")
      .createOrReplaceTempView("ddd")

    spark.sql("select * from ddd").show

    spark.sql(
      """
        |
        |select DENSE_RANK() over(order by c2) as c2_id, c1, c2, c1_id from (
        | select DENSE_RANK() over(order by c1) as c1_id, c1, c2
        |  from ddd)
        |""".stripMargin)

    val df = spark.sql(
      """
        |
        |select DENSE_RANK() over(order by c2) as c2_id, c2,
        | DENSE_RANK() over(order by c1) as c1_id, c1
        |  from ddd
        |""".stripMargin)
    df.show
    df.printSchema()
//    spark.sql(
//      """
//        | select row_number() over(order by nid) as test_id, nid from (
//        |   select distinct(id) as nid from ddd
//        | )
//        |""".stripMargin)
//      .show()




  }

}

case class ZIndexFileInfo(
    file: String,
    numRecords: Int,
    minIndex: Int,
    maxIndex: Int)
