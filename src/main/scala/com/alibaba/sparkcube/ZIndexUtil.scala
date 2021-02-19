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

import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.HiveHash
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitionSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.{ArrayZIndexV2, ColumnMinMax, FileStatistics, ReplaceHadoopFsRelation, TableMetadata, ZIndexFileInfoV2}

object ZIndexUtil {

  val DEFAULT_Z_INDEX_CACHE_DIR_PREFIX = "/tmp/zindex/"

  val tableIndexPath = (tableName: String) => DEFAULT_Z_INDEX_CACHE_DIR_PREFIX + tableName

  def createZIndex(spark: SparkSession,
                   inputFormat: String,
                   inputPath: String,
                   cols: Array[String],
                   fileNum: Int = 1000,
                   format: String = "parquet",
                   partitionCols: Option[Seq[String]] = None): Unit = {

    spark.udf.register("arrayZIndex", (vec: Seq[Int]) => ArrayZIndexV2.create(vec).indices)

//    val df = spark.sql(s"select * from ${table}")
    val df = spark.read.format(inputFormat).load(inputPath)
    val table = inputPath.split("/").last
    val dataSchema = df.schema.map(_.name)

    val rowIdDF = df.selectExpr(
      Seq("*") ++
        cols.map(c => s"DENSE_RANK() over(order by ${c}) - 1 as __${c}_id__"): _*
    )

//    val hashDF = rowIdDF
//      .select(
//        cols.map(c => new Column(HiveHash(Seq(col(c).expr))).as(s"__index_${c}__")): _*,
//        col("*")
//      )
    val indexDF = rowIdDF.selectExpr(
        (Array("*") ++
          Array(
            s"arrayZIndex(array(${cols.map(c => s"__${c}_id__").mkString(",")})) as __zIndex__"
          )
        ): _*
      )

    val writeDF = indexDF
      .repartitionByRange(fileNum, col("__zIndex__"))
      .write
      .format(format)
      .mode("overwrite")
    partitionCols.foreach(cols => writeDF.partitionBy(cols: _*))
    writeDF.save(tableIndexPath(table))

    val tempView = s"__cache_${UUID.randomUUID().toString.replace("-", "")}"
    spark.read
      .format(format)
      .load(tableIndexPath(table))
      .createOrReplaceTempView(tempView)

    val minMaxExpr = cols.map {
      c =>
        s"min(${c}) as __min_${c}__, max($c) as __max_${c}__"
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
          val min = r.get(r.fieldIndex(s"__min_${c}__"))
          val max = r.get(r.fieldIndex(s"__max_${c}__"))
          (c, ColumnMinMax(min, max))
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

    // compute data length.
    val dataLength = rowIdDF.selectExpr(
      cols.map(c => s"max(__${c}_id__) as __max_id_${c}__"): _*
    ).collect()
      .headOption
      .map(r => {
        cols.map(c => {
          r.getAs[Int](s"__max_id_${c}__")
        }).max
      }).get

    println("----- metadata -----")
    metadata.foreach(m => {
      println(m.file)
      println(m.minMax)
    })
    println("----- metadata -----")

    val zindexMetadata = TableMetadata(
      tableIndexPath(table),
      metadata
    )

    ReplaceHadoopFsRelation.relationMetadata +=
      (s"${inputFormat.toLowerCase()}.`file:${inputPath.toLowerCase()}`" -> zindexMetadata)
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
}
