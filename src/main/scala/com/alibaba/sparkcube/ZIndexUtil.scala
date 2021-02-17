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
import org.apache.spark.sql.execution.{ArrayZIndexV2, ReplaceHadoopFsRelation, ZIndexFileInfoV2, ZIndexMetadata}

object ZIndexUtil {

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

    val hashDF = df
      .select(
        Seq(
          col("*")) ++
          cols.map(c => new Column(HiveHash(Seq(col(c).expr))).as(s"__index_${c}__")
          ): _*
      )
    val indexDF = hashDF.selectExpr(
        (dataSchema ++
          Array(
            s"arrayZIndex(array(${cols.map(c => s"__index_${c}__").mkString(",")})) as __zIndex__"
          )
        ): _*
      )
    val writeDF = indexDF
      .repartitionByRange(fileNum, col("__zIndex__"))
      .write
      .format(format)
      .mode("overwrite")
    partitionCols.foreach(cols => writeDF.partitionBy(cols: _*))
    writeDF.save(s"/tmp/zindex/${table}")

    val tempView = s"__cache_${UUID.randomUUID().toString.replace("-", "")}"
    spark.read
      .format(format)
      .load(s"/tmp/zindex/${table}")
      .createOrReplaceTempView(tempView)

    val stat = spark.sql(
      s"""
        |SELECT file, count(*) as numRecords, min(__zIndex__) as minZIndex, max(__zIndex__) as maxZIndex
        |FROM (
        | SELECT input_file_name() AS file, * FROM ${tempView}
        |) GROUP BY file
        |""".stripMargin)
//    stat.show()
    collectPartitionInfo(stat)
    var metadata = stat.collect()
      .map(r => {
        ZIndexFileInfoV2(
          r.getAs[String]("file"),
          r.getAs[Long]("numRecords"),
          ArrayZIndexV2(r.getAs[WrappedArray[Long]]("minZIndex").map(_.toInt).toArray),
          ArrayZIndexV2(r.getAs[WrappedArray[Long]]("maxZIndex").map(_.toInt).toArray)
        )
      })

    if (partitionCols.isDefined) {
      metadata = setFilePartitionInfo(metadata, collectPartitionInfo(stat))
    }

    // compute data length.
    val dataLength = hashDF.selectExpr(
      cols.map(c => s"max(__index_${c}__) as max_hash_${c}"): _*
    ).collect()
      .headOption
      .map(r => {
        cols.map(c => {
          r.getAs[Int](s"max_hash_${c}")
        }).max
      }).get

    println("----- metadata -----")
    metadata.foreach(m => {
      println(m.file)
      println(m.minIndex.toBinaryString)
      println(m.maxIndex.toBinaryString)
    })
    println("----- metadata -----")

    val zindexMetadata = ZIndexMetadata(
      s"/tmp/zindex/${table}",
      Integer.toBinaryString(dataLength - 1).length,
      cols.zipWithIndex.toMap,
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

  private def setFilePartitionInfo(metadata: Array[ZIndexFileInfoV2],
      partitionSpec: Option[PartitionSpec]): Array[ZIndexFileInfoV2] = {
    metadata.map(info => {
      val partitionPath = partitionSpec.get
        .partitions
        .filter(pp => info.file.contains(pp.path.toUri.normalize.getPath.toString))
        .head
      info.copy(filePartitionPath = Some(partitionPath))
    })
  }
}
