// scalastyle:off
package org.apache.spark.sql.execution

import com.alibaba.sparkcube.ZIndexFileInfo
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, BinaryOperator, EqualNullSafe, EqualTo, Expression, HiveHash, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitionDirectory, PartitionPath}
import org.apache.spark.sql.types.StructType

//class replacehadoopfsrelation extends Strategy {
//  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
//    plan match {
//      case hadoopfsrelation
//    }
//  }
//}

case class ReplaceHadoopFsRelation() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
//      case logicalRelation @ LogicalRelation(relation: HadoopFsRelation, _, _, _)
//        if ReplaceHadoopFsRelation.inCacheRelation.contains(
//          s"${relation.fileFormat.toString.toLowerCase()}.`${relation.location.rootPaths.mkString(",").toLowerCase()}`") =>
//
//        val tableIdentifier = s"${relation.fileFormat.toString.toLowerCase()}.`${relation.location.rootPaths.mkString(",").toLowerCase()}`"
//        val paths = Seq(new Path(
//          ReplaceHadoopFsRelation.inCacheRelation(tableIdentifier)
//        ))
//        val newLocation = new InMemoryFileIndex(SparkSession.active, paths, Map.empty, Option(relation.schema))
//        val newRelation = relation.copy(location = newLocation)(SparkSession.active)
//        logicalRelation.copy(relation = newRelation)

//      case logicalRelation @ LogicalRelation(relation: HadoopFsRelation, _, _, _) =>
//        println("dddddebug")
//        val y = s"${relation.fileFormat.toString.toLowerCase()}.`${relation.location.rootPaths.mkString(",").toLowerCase()}`"
//        println(y)
//        val x = ReplaceHadoopFsRelation.relationMetadata.keys.headOption.getOrElse("")
//        println(x == y)
//        ReplaceHadoopFsRelation.relationMetadata.foreach(println)
//        logicalRelation
//      case f @ Filter(cond,
//      logicalRelation @ LogicalRelation(
//      relation @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _)) =>
//        println("ddddda")
//        f

      case f @ Filter(cond,
      logicalRelation @ LogicalRelation(
      relation @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _))
        if ReplaceHadoopFsRelation.relationMetadata.contains(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`") =>
        println("aaaaa")
        println(relation.fileFormat)
        println(s"${relation.fileFormat}.`${location.rootPaths.mkString("")}`")
//        val paths = ReplaceHadoopFsRelation.relationMetadata(
//          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`"
//        )
////        val paths = ReplaceHadoopFsRelation.metadata
//          .findTouchFileByExpression(f.condition)
//          .map(i => {
//            logInfo(s"input file: ${i}")
//            i
//          })
//          .map(i => new Path(i.file))
//        val paths = location.inputFiles.map(f => new Path(f))
        val zIndexMetadata = ReplaceHadoopFsRelation.relationMetadata(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`"
        )
//        val newLocation2 = new InMemoryFileIndex(relation.sparkSession,
//          Seq(new Path(zIndexMetadata.basePath)), Map.empty, Option(relation.schema)) {
//          override def listFiles(partitionFilters:  Seq[Expression], dataFilters:  Seq[Expression]): Seq[PartitionDirectory] = {
//            println("dddddddddddddddfff")
//            val files = super.listFiles(partitionFilters, dataFilters)
//            if (partitionSpec().partitionColumns.isEmpty) {
//              val nameToFileStatus = files.flatMap(_.files.map(f => (f.getPath.getName, f))).toMap
//              val paths = ReplaceHadoopFsRelation.relationMetadata(
//                s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`"
//              )
//                //        val paths = ReplaceHadoopFsRelation.metadata
//                .findTouchFileByExpression(f.condition)
//                .map(i => {
//                  logInfo(s"input file: ${i}")
//                  i
//                })
//                .map(zi => nameToFileStatus(zi.file))
//                .toSeq
////              PartitionDirectory(InternalRow.empty, paths) :: Nil
//              throw new UnsupportedOperationException
//            } else {
//              throw new UnsupportedOperationException
//            }
//
//          }
//        }

        val newLocation = new ZIndexInMemoryFileIndex(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`",
          relation.sparkSession, Seq(new Path(zIndexMetadata.basePath)), Map.empty, Option(relation.schema))
        val newRelation = relation.copy(location = newLocation)(relation.sparkSession)
        val newLogicalRelation = logicalRelation.copy(relation = newRelation)
        val newFilter = f.copy(cond, newLogicalRelation)
        println("-------new filter------")
        println(newFilter)
        println(newLocation)
        newFilter
    }
  }
}

class ZIndexInMemoryFileIndex(
     tableIdentifier: String,
     sparkSessionx: SparkSession,
     rootPathsSpecifiedx: Seq[Path],
     parametersx: Map[String, String],
     userSpecifiedSchemax: Option[StructType]) extends InMemoryFileIndex(
  sparkSessionx, rootPathsSpecifiedx, parametersx, userSpecifiedSchemax) {

  override def listFiles(partitionFilters:  Seq[Expression], dataFilters:  Seq[Expression]): Seq[PartitionDirectory] = {
    val files = super.listFiles(partitionFilters, dataFilters)
    if (partitionSpec().partitionColumns.isEmpty) {
      val nameToFileStatus = files.flatMap(_.files.map(f => (f.getPath.toUri.normalize.toString.toLowerCase, f))).toMap
      println("------name to file-----")
      nameToFileStatus.foreach(println)
      println("------name to file-----")
      val paths = ReplaceHadoopFsRelation.relationMetadata(tableIdentifier)
        //        val paths = ReplaceHadoopFsRelation.metadata
        .findTouchFileByExpression(dataFilters.head)
        .map(i => {
          logInfo(s"input file: ${i}")
          i
        })
        .map(zi => nameToFileStatus(zi.file.toLowerCase))
        .toSeq
      PartitionDirectory(InternalRow.empty, paths) :: Nil
    } else {
//      val ps = partitionSpec()
//      val pathToPartitionValue = ps.partitions.map(partitionPath => {
//        (partitionPath.path, partitionPath.values)
//      }).toMap
      val nameToFileStatus = files.flatMap(_.files.map(f => (f.getPath.toUri.normalize.toString.toLowerCase, f))).toMap

      val selectedPartitions = files.map(_.values).toSet
      val metadata = ReplaceHadoopFsRelation.relationMetadata(tableIdentifier)

      val selectedFiles = metadata.fileMetadta
        .filter(fm => {
          fm.filePartitionPath.isDefined && selectedPartitions.contains(fm.filePartitionPath.get.values)
        })

      val selectedMetadata = metadata.copy(fileMetadta = selectedFiles)

      logInfo(s"dataFilters size: ${dataFilters.size}.")
      val filter = if (dataFilters.size > 1) {
        dataFilters.reduce((l, r) => And(l, r))
      } else {
        dataFilters.head
      }
      selectedMetadata.findTouchFileByExpression(filter)
        .map(i => {
          logInfo(s"input file: ${i}")
          i
        })
        .map(zi => (zi, nameToFileStatus(zi.file.toLowerCase)))
        .groupBy {
          case (zi, fs) =>
            zi.filePartitionPath
        }.map {
          case (Some(partitionPath), array) =>
            PartitionDirectory(partitionPath.values, array.map(_._2))
        }.toSeq
//      throw new UnsupportedOperationException
    }

  }

}

object ReplaceHadoopFsRelation {

//  var inCacheRelation = Map[String, String]()
  var relationMetadata = Map[String, ZIndexMetadata]()
  var metadata: ZIndexMetadata = _

}

case class ZIndexFileInfoV2(
    file: String,
    numRecords: Long,
    minIndex: ArrayZIndexV2,
    maxIndex: ArrayZIndexV2,
    filePartitionPath: Option[PartitionPath] = None) {

  def zIndexPrefix: Array[Int] = {
    var i = 0
    while (i < maxIndex.indices.length && minIndex.indices(i) == maxIndex.indices(i)) {
      i += 1
    }
    maxIndex.indices.slice(0, i)
  }
}

/**
 * 分区表的情况，可能部分分区命中cache，而部分分区未能命中。
 *
 * @param bitLength
 * @param colIndices
 * @param fileMetadta
 */
case class ZIndexMetadata(
    basePath: String,
    bitLength: Int,
    colIndices: Map[String, Int],
    fileMetadta: Array[ZIndexFileInfoV2]) {
  def skipFiles(condition: Filter): Unit = {
//    condition.map {
//    }

  }

  def inCachedFiles(selectedPartitions: Seq[PartitionDirectory]): (Seq[PartitionDirectory], Array[ZIndexFileInfoV2]) = {

//    var result = (Seq[PartitionDirectory], Array[ZIndexFileInfoV2])
    var inCachedResult: Array[ZIndexFileInfoV2] = null
    var nonInCachedResult: Seq[PartitionDirectory] = null

    val cachedFiles = fileMetadta.map(_.file)
    selectedPartitions.foreach {
      case partitionDirectory: PartitionDirectory
        if partitionDirectory.files.exists(fileStat => cachedFiles.contains(fileStat.getPath.getName)) =>

        partitionDirectory.values
//      case pd => nonInCachedResult += pd
//        null
    }

    null
  }

  // 遍历filter条件，获取命中的文件
  def findTouchFileByExpression(condition: Expression): Array[ZIndexFileInfoV2] = {
//    op match {
//      case e @ EqualTo(att: AttributeReference, expr: _) =>
//        att.name
//
//    }

    def f(col: String, value: Int, prefix: Array[Int]): Boolean = {
      val colIndex = colIndices(col)
      (31 to 0 by -1).foreach(i => {
        val mask = 1 << i
        // 计算在prefix中的位置
        val index = (31 - i) * colIndices.size + colIndex
        if (index >= prefix.length) {
          return true
        }
        (value & mask) match {
          case 0 if (prefix(index) != 0) => return false
          case res if (res !=0 && prefix(index) != 1) => return false
          case _ => // do nothing
        }
//        if ((value & mask) != 0) {
//          1
//        } else {
//          0
//        }
      })
      true
    }

    condition match {
      case e @ EqualTo(left: AttributeReference, right: Literal) =>
        val l = lowerBound(left.name, HiveHash(Seq(right)).eval())
        val h = upperBound(left.name, HiveHash(Seq(right)).eval())
        fileMetadta.filter {
          case file =>
            f(left.name, HiveHash(Seq(right)).eval(), file.zIndexPrefix) &&
            file.minIndex <= h && file.maxIndex >= l
        }
      case e @ EqualTo(left: Literal, right: AttributeReference) =>
        val l = lowerBound(right.name, HiveHash(Seq(left)).eval())
        val h = upperBound(right.name, HiveHash(Seq(left)).eval())
//        fileMetadta.filter {
//          case f =>
//            // 包含下边界
//            (f.maxIndex >= l && f.minIndex <= l) ||
//              // 包含上边界
//              (f.maxIndex >= h && f.minIndex <= h)
//        }
        fileMetadta.filter {
          case file =>
            f(right.name, HiveHash(Seq(left)).eval(), file.zIndexPrefix) &&
            file.minIndex <= h && file.maxIndex >= l
        }
      case e @ EqualNullSafe(left: AttributeReference, right: Literal) =>
        fileMetadta
      case e @ EqualNullSafe(left: Literal, right: AttributeReference) =>
        fileMetadta
      case a: And =>
        val resLeft = findTouchFileByExpression(a.left)
        val resRight = findTouchFileByExpression(a.right)
        // 并集
        resLeft.intersect(resRight)
      case or: Or =>
        val resLeft = findTouchFileByExpression(or.left)
        val resRight = findTouchFileByExpression(or.right)
        // 并集
        resLeft.union(resRight).distinct
      case _ =>
        fileMetadta
    }

  }

  def upperBound(col: String, value: Int): ArrayZIndexV2 = {
    val colIndex = colIndices(col)
    ArrayZIndexV2.create(
      Array.tabulate(colIndices.size) {
        case i if i == colIndex => value
        case _ => (1 << bitLength) - 1
      }
    )
  }

  def lowerBound(col: String, value: Int): ArrayZIndexV2 = {
    val colIndex = colIndices(col)
    ArrayZIndexV2.create(
      Array.tabulate(colIndices.size) {
        case i if i == colIndex => value
        case _ => 0
      }
    )
  }

  private def traverse[T](condition: Expression,
                          comparisonOp: (AttributeReference, Literal, BinaryComparison) => T,
                          binaryOperatorOp: (T, T, BinaryOperator) => T): T = {
    condition match {
      case e @ EqualTo(left: AttributeReference, right: Literal) =>
        comparisonOp(left, right, e)
      case e @ EqualTo(left: Literal, right: AttributeReference) =>
        comparisonOp(right, left, e)
      case e @ EqualNullSafe(left: AttributeReference, right: Literal) =>
        comparisonOp(left, right, e)
      case e @ EqualNullSafe(left: Literal, right: AttributeReference) =>
        comparisonOp(right, left, e)
      case a: And =>
        val resLeft = traverse(a.left, comparisonOp, binaryOperatorOp)
        val resRight = traverse(a.right, comparisonOp, binaryOperatorOp)
        binaryOperatorOp(resLeft, resRight, a)
      //      case _ =>
    }
  }
}

// 实现两种类型
// 1. int 用于调试和演示
// 2. array[Int]

trait ZIndex[T] {

  def indices: T

  def toBinaryString: String

  /**
   * @return 1 means this > that, 0 means this = that, -1 means this < that.
   */
  def compare(that: ZIndex[T]): Int

  def >=(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == 1 || f == 0
  }

  def >(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == 1
  }

  def <=(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == -1 || f == 0
  }

  def <(that: ZIndex[T]): Boolean = {
    val f = compare(that)
    f == -1
  }
}

case class ArrayZIndex(override val indices: Array[Int]) extends ZIndex[Array[Int]] {
  override def toBinaryString: String = {
    (31 to 0 by -1).flatMap(i => {
      val mask = 1 << i
      (0 until indices.length).map( colIndex => {
        if ((indices(colIndex) & mask) != 0) {
          1
        } else {
          0
        }
      })
    }).mkString("")
  }

  override def compare(that: ZIndex[Array[Int]]): Int = {
    require(this.indices.length == that.indices.length)
    val colNum = this.indices.length
    for(byteIndex <- (31 to 0 by -1)) {
      val mask = 1 << byteIndex
      for(colIndex <- (0 until colNum)) {
        val byteAtThisZIndex = this.indices(colIndex) & mask
        val byteAtThatZIndex = that.indices(colIndex) & mask
        if (byteAtThisZIndex > byteAtThatZIndex) {
          return 1
        } else if (byteAtThisZIndex < byteAtThatZIndex) {
          return -1
        }
      }
    }
    return 0
  }
}


case class ArrayZIndexV2(override val indices: Array[Int]) extends ZIndex[Array[Int]] {
  override def toBinaryString: String = {
    indices.mkString("")
  }

  /**
   * @return 1 means this > that, 0 means this = that, -1 means this < that.
   */
  override def compare(that: ZIndex[Array[Int]]): Int = {
    require(this.indices.length == that.indices.length)
    for (i <- (0 until indices.length)) {
      val diff = indices(i) - that.indices(i)
      if (diff != 0) {
        return diff
      }
    }
    return 0
  }
}

object ArrayZIndexV2 {
  def create(input: Array[Int]): ArrayZIndexV2 = {
    val zindex = (31 to 0 by -1).flatMap(i => {
      val mask = 1 << i
      (0 until input.length).map( colIndex => {
        if ((input(colIndex) & mask) != 0) {
          1
        } else {
          0
        }
      })
    })
    // todo: 优化
    // 31位一组转换成Int类型
    ArrayZIndexV2(zindex.toArray)
  }

  def create(input: Seq[Int]): ArrayZIndexV2 = {
    create(input.toArray)
  }

}
