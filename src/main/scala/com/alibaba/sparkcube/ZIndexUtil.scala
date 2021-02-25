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

package com.alibaba.sparkcube

import java.util.UUID

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, GetMapValue, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.{ArrayZIndexV2, ColumnMinMax, FileStatistics, ReplaceHadoopFsRelation, TableMetadata}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitionSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object ZIndexUtil extends Logging {

  val DEFAULT_Z_INDEX_CACHE_DIR_PREFIX = "tmp/zindex/"

  val tableIndexPath = (tableName: String) => DEFAULT_Z_INDEX_CACHE_DIR_PREFIX + tableName

  private val colToIndexColName = HashMap[String, String]()

  private val indexColName = {
    (colName: String) =>
      colToIndexColName.getOrElseUpdate(
        colName,
        s"__${UUID.randomUUID().toString.replace("-", "")}__"
      )
  }
  private val minColName = {
    (colName: String) =>
      s"__min_${indexColName(colName)}__"
  }

  private val maxColName = {
    (colName: String) =>
      s"__max_${indexColName(colName)}__"
  }

  def createZIndex(spark: SparkSession,
                   inputFormat: String,
                   inputPath: String,
                   cols: Array[String],
                   fileNum: Int = 1000,
                   format: String = "parquet",
                   partitionCols: Option[Seq[String]] = None): Unit = {

    spark.udf.register("arrayZIndex", (vec: Seq[Int]) => ArrayZIndexV2.create(vec).indices)

    val df = spark.read.format(inputFormat).load(inputPath)
    val table = inputPath.split("/").last
    val dataSchema = df.schema.map(_.name)

//    val rowIdDF = df.repartition(1000).selectExpr(
//      Seq("*") ++
//        cols.map(c => s"DENSE_RANK() over(order by ${c}) - 1 as ${indexColName(c)}"): _*
//    )

    var rowIdDF: DataFrame = df
    cols.foreach(c => {
//      rowIdDF = generateGlobalRankId(rowIdDF, c, indexColName(c))
      rowIdDF = generateRankIDForSkewedTable(rowIdDF, c, indexColName(c))
    })

    val indexDF = rowIdDF.selectExpr(
        (dataSchema ++
          Array(
            s"arrayZIndex(array(${cols.map(indexColName).mkString(",")})) as __zIndex__"
          )
        ): _*
      )

    val writeDF = indexDF
      .repartitionByRange(fileNum, col("__zIndex__"), (rand() * 1000).cast(IntegerType))
      .selectExpr(dataSchema: _*)
      .write
      .format(format)
      .mode("overwrite")
    partitionCols.foreach(cols => writeDF.partitionBy(cols: _*))
    writeDF.save(tableIndexPath(table))

    val tempView = s"__cache_${UUID.randomUUID().toString.replace("-", "")}"
    val tempDF = spark.read
      .format(format)
      .load(tableIndexPath(table))
    tempDF.createOrReplaceTempView(tempView)

    val minMaxExpr = cols.map {
      c =>
        s"min(${c}) as ${minColName(c)}, max($c) as ${maxColName(c)}"
    }.mkString(",")

    val stat = spark.sql(
      s"""
        |SELECT file, count(*) as numRecords, ${minMaxExpr}
        |FROM (
        | SELECT input_file_name() AS file, * FROM ${tempView}
        |) GROUP BY file
        |""".stripMargin)

    var metadata = stat.collect()
      .map(r => {
        val minMaxInfo = cols.map(c => {
          val min = r.get(r.fieldIndex(minColName(c)))
          val max = r.get(r.fieldIndex(maxColName(c)))
          (getNormalizeColumnName(tempDF, c), ColumnMinMax(min, max))
        }).toMap
        FileStatistics(
          r.getAs[String]("file"),
          r.getAs[Long]("numRecords"),
          minMaxInfo
        )
      })

    if (partitionCols.isDefined) {
      metadata = setFilePartitionInfo(metadata, collectPartitionInfo(stat))
    }

    val zindexMetadata = TableMetadata(
      tableIndexPath(table),
      metadata
    )

    val path = getTablePath(df).toLowerCase()
    ReplaceHadoopFsRelation.relationMetadata +=
      (s"${inputFormat.toLowerCase()}.`${path}`" -> zindexMetadata)
  }

  def loadIndexInfo(spark: SparkSession,
                   inputFormat: String,
                   inputPath: String,
                   cols: Array[String],
                   fileNum: Int = 1000,
                   format: String = "parquet",
                   partitionCols: Option[Seq[String]] = None): Unit = {
    val df = spark.read.format(inputFormat).load(inputPath)
    val table = inputPath.split("/").last
    val tempView = s"__cache_${UUID.randomUUID().toString.replace("-", "")}"
    val tempDF = spark.read
      .format(format)
      .load(tableIndexPath(table))
    tempDF.createOrReplaceTempView(tempView)

    val minMaxExpr = cols.map {
      c =>
        s"min(${c}) as ${minColName(c)}, max($c) as ${maxColName(c)}"
    }.mkString(",")

    val stat = spark.sql(
      s"""
         |SELECT file, count(*) as numRecords, ${minMaxExpr}
         |FROM (
         | SELECT input_file_name() AS file, * FROM ${tempView}
         |) GROUP BY file
         |""".stripMargin)

    var metadata = stat.collect()
      .map(r => {
        val minMaxInfo = cols.map(c => {
          val min = r.get(r.fieldIndex(minColName(c)))
          val max = r.get(r.fieldIndex(maxColName(c)))
          (getNormalizeColumnName(tempDF, c), ColumnMinMax(min, max))
        }).toMap
        FileStatistics(
          r.getAs[String]("file"),
          r.getAs[Long]("numRecords"),
          minMaxInfo
        )
      })

    if (partitionCols.isDefined) {
      metadata = setFilePartitionInfo(metadata, collectPartitionInfo(stat))
    }

    val zindexMetadata = TableMetadata(
      tableIndexPath(table),
      metadata
    )

    val path = getTablePath(df).toLowerCase()
    ReplaceHadoopFsRelation.relationMetadata +=
      (s"${inputFormat.toLowerCase()}.`${path}`" -> zindexMetadata)
  }

  /**
   * reference: https://medium.com/swlh/computing-global-rank-of-a-row-in-a-dataframe-with-spark-sql-34f6cc650ae5
   *
   * @param df
   * @param colName
   * @param rankColName
   * @return
   */
  def generateGlobalRankId(df: DataFrame, colName: String, rankColName: String): DataFrame = {

    // if the input column is a nested column, we should normalize the input column name.
    if (isNestedColumn(df, colName)) {
      val newColName = s"__col_${colName.hashCode.toString.replaceAll("-", "")}__"
      val newDF = df.selectExpr("*", s"$colName as $newColName")
      return generateGlobalRankId(newDF, newColName, rankColName)
        .selectExpr((Seq(rankColName) ++ df.schema.map(_.name)): _*)
    }

    val inputSchema = df.schema.map(_.name)
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val partDF = df
      .orderBy(colName) // ==> 通过range partition来实现的
      // =====> 在这里实现bin packing 的分区方法？
      .withColumn("partitionId", spark_partition_id())

//    partDF.createOrReplaceTempView("aaa")
//    spark.sql("select distinct(partitionId) as pid from aaa").show()

    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy("partitionId").orderBy(colName)

//    val rankDF = partDF.withColumn("local_rank", dense_rank().over(w))
    val rankDF = partDF.withColumn("local_rank", row_number().over(w))
      .withColumn("rand_id", rand())
      .repartition(1000, col("rand_id"))
      .persist()

    rankDF.count

    val tempDf =
      rankDF.groupBy("partitionId").agg(max("local_rank").alias("max_rank"))

    val w2 = Window.orderBy("partitionId").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val statsDf = tempDf.withColumn("cum_rank", sum("max_rank").over(w2))

    val joinDF = statsDf.alias("l")
      .join(
        statsDf.alias("r"), $"l.partitionId" === $"r.partitionId" +1, "left"
      ).select(
        col("l.partitionId"),
        coalesce(col("r.cum_rank"), lit(0)).alias("sum_factor")
    )

    // todo: (fchen) 转为long类型
    val finalDF = rankDF.join(
      broadcast(joinDF), Seq("partitionId"),"inner")
      .withColumn(rankColName, ($"local_rank" + $"sum_factor" - 1).cast(IntegerType))

    finalDF.selectExpr((rankColName :: Nil ++ inputSchema): _*)
  }

  def generateRankIDForSkewedTable(df: DataFrame,
                                   colName: String,
                                   rankColName: String): DataFrame = {

    // if the input column is a nested column, we should normalize the input column name.
    if (isNestedColumn(df, colName)) {
      val newColName = s"__col_${colName.hashCode.toString.replaceAll("-", "")}__"
      val newDF = df.selectExpr("*", s"$colName as $newColName")
      return generateRankIDForSkewedTable(newDF, newColName, rankColName)
        .selectExpr((Seq(rankColName) ++ df.schema.map(_.name)): _*)
    }

    val inputSchema = df.schema.map(_.name)
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

//    df.withColumn("rand_id", (rand() * 1000).cast(IntegerType))
    val cachedDF = df.persist(StorageLevel.DISK_ONLY_2)

    try {
      val idDF = cachedDF.dropDuplicates(colName)
        .withColumn(rankColName, rank.over(Window.orderBy(colName)) - 1)

      cachedDF.alias("l")
        .join(
          broadcast(idDF.alias("r")), $"l.${colName}" === $"r.${colName}"
        )
        .selectExpr(inputSchema.map(i => s"l.$i") ++ Array(rankColName): _*)
    } finally {
      cachedDF.unpersist()
    }
  }

  def extractRecursively(expr: Expression): Seq[String] = expr match {
    case attr: Attribute => Seq(attr.name)

    case Alias(c, _) => extractRecursively(c)

    case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

    case GetMapValue(left, lit: Literal) => extractRecursively(left) :+ lit.eval().toString

//    case _: ExtractValue =>
//      throw new RuntimeException("extract nested fields is only supported for StructType.")
    case other =>
      throw new RuntimeException(
        s"Found unsupported expression '$other' while parsing target column name parts")
  }

  private def getNormalizeColumnName(df: DataFrame, colName: String): String = {
    df.selectExpr(colName)
      .queryExecution
      .analyzed
      .collect {case proj: Project => proj}
      .headOption
      .map(proj => {
        extractRecursively(proj.projectList.head).mkString(".")
      })
      .getOrElse {
        throw new RuntimeException(s"can't normalize the ${colName}")
      }
  }

  private def isNestedColumn(df: DataFrame, colName: String): Boolean = {
    !df.schema.fields.map(_.name).contains(colName)
  }

  private def collectPartitionInfo(df: DataFrame): Option[PartitionSpec] = {
    var res: Option[PartitionSpec] = None
    df.queryExecution.analyzed.foreach {
      case _ @ LogicalRelation(
        _ @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _) =>
        res = Some(location.partitionSpec())
      case _ =>
    }
    res
  }

  private def setFilePartitionInfo(metadata: Array[FileStatistics],
      partitionSpec: Option[PartitionSpec]): Array[FileStatistics] = {
    metadata.map(info => {
      val partitionPath = partitionSpec.get
        .partitions
        .filter(pp => info.file.contains(pp.path.toUri.normalize.getPath.toString))
        .head
      info.copy(filePartitionPath = Some(partitionPath))
    })
  }

  private def getTablePath(df: DataFrame): String = {
    df.queryExecution.analyzed.map {
      case _ @ LogicalRelation(
      _ @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _) =>
        location.rootPaths.mkString("")
      case _ => ""
    }.mkString("")
  }
}
