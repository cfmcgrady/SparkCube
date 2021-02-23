// scalastyle:off
package org.apache.spark.sql.execution

import com.alibaba.sparkcube.{SchemaUtils, ZIndexUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualNullSafe, EqualTo, Expression, GetMapValue, GetStructField, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NonNullLiteral, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitionDirectory, PartitionPath}
import org.apache.spark.sql.types.StructType

case class ReplaceHadoopFsRelation() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case f @ Filter(cond,
      logicalRelation @ LogicalRelation(
      relation @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _))
        if ReplaceHadoopFsRelation.relationMetadata.contains(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`") =>

        val zIndexMetadata = ReplaceHadoopFsRelation.relationMetadata(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`"
        )
        val newLocation = new ZIndexInMemoryFileIndex(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`",
          relation.sparkSession, Seq(new Path(zIndexMetadata.basePath)), Map.empty, Option(relation.schema))
        val newRelation = relation.copy(location = newLocation)(relation.sparkSession)
        val newLogicalRelation = logicalRelation.copy(relation = newRelation)
        val newFilter = f.copy(cond, newLogicalRelation)
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
    logInfo(s"dataFilters size: ${dataFilters.size}.")
    val filter = if (dataFilters.size > 1) {
      dataFilters.reduce((l, r) => And(l, r))
    } else if (dataFilters.size == 1) {
      dataFilters.head
    } else {
      return files
    }

    println(s"filter----${filter}")
    if (partitionSpec().partitionColumns.isEmpty) {
      val nameToFileStatus = files.flatMap(_.files.map(f => (f.getPath.toUri.normalize.toString.toLowerCase, f))).toMap
      val paths = ReplaceHadoopFsRelation.relationMetadata(tableIdentifier)
        .findTouchFileByExpression(filter)
        .map(i => {
          logInfo(s"input file: ${i}")
          i
        })
        .map(zi => nameToFileStatus(zi.file.toLowerCase))
        .toSeq
      PartitionDirectory(InternalRow.empty, paths) :: Nil
    } else {
      val nameToFileStatus = files.flatMap(_.files.map(f => (f.getPath.toUri.normalize.toString.toLowerCase, f))).toMap

      val selectedPartitions = files.map(_.values).toSet
      val metadata = ReplaceHadoopFsRelation.relationMetadata(tableIdentifier)

      val selectedFiles = metadata.fileMetadata
        .filter(fm => {
          fm.filePartitionPath.isDefined && selectedPartitions.contains(fm.filePartitionPath.get.values)
        })

      val selectedMetadata = metadata.copy(fileMetadata = selectedFiles)

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
    }

  }

}

object ReplaceHadoopFsRelation {

  var relationMetadata = Map[String, TableMetadata]()
  var metadata: TableMetadata = _

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

case class ColumnMinMax(min: Any, max: Any)

case class FileStatistics(
    file: String,
    numRecords: Long,
    minMax: Map[String, ColumnMinMax],
    filePartitionPath: Option[PartitionPath] = None)

/**
 * 分区表的情况，可能部分分区命中cache，而部分分区未能命中。
 *
 * @param fileMetadata
 */
case class TableMetadata(
    basePath: String,
    fileMetadata: Array[FileStatistics]) {

  // 遍历filter条件，获取命中的文件
  def findTouchFileByExpression(condition: Expression): Array[FileStatistics] = {

    def equalTo(left: String, right: Literal): Unit = {
//      fileMetadata.filter {
//        case fileStatistics =>
//          fileStatistics.minMax
//            .get(left)
//            .map {
//              case ColumnMinMax(min, max) =>
//                val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
//                ordering.lteq(Literal(min).value, right.value) && ordering.gteq(Literal(max).value, right.value)
//            }.getOrElse(true)
//      }
    }

    condition match {
      case equalTo @ EqualTo(_: AttributeReference |
                             _: GetStructField |
                             _: GetMapValue, right: Literal) =>
        val colName = ZIndexUtil.extractRecursively(equalTo.left).mkString(".")
        fileMetadata.foreach(println)
        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(equalTo.left.dataType)
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(colName)
              .map {
                case ColumnMinMax(min, max) =>
                  (Literal(min), Literal(max)) match {
                    // handle null value case.
                    case (Literal(null, _), max @ NonNullLiteral(_, _)) if Some(right.value).isDefined =>
                      ordering.gteq(Literal.create(max).value, right.value)
                    case (min @ NonNullLiteral(_, _), Literal(null, _)) if Some(right.value).isDefined =>
                      ordering.lteq(Literal.create(min).value, right.value)
                    case (min @ NonNullLiteral(_, _), max @ NonNullLiteral(_, _)) if Some(right.value).isDefined =>
                      ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
                    case (Literal(null, _), Literal(null, _)) if Some(right.value).isDefined =>
                      false
                    case (min: Literal, max: Literal) if Some(right.value).isEmpty && (Some(min).isEmpty || Some(max).isEmpty) =>
                      true
                    case (NonNullLiteral(_, _), NonNullLiteral(_, _)) if Some(right.value).isEmpty =>
                      false
                  }
              }.getOrElse(true)
        }

      case equalTo @ EqualTo(_: AttributeReference |
                             _: GetStructField |
                             _: GetMapValue, right: Literal) =>
        val colName = ZIndexUtil.extractRecursively(equalTo.left).mkString(".")
        fileMetadata.foreach(println)
        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(equalTo.left.dataType)
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(colName)
              .map {
                // handle null value case.
                case ColumnMinMax(null, max) if Some(right.value).isDefined =>
                  ordering.gteq(Literal.create(max).value, right.value)
                case ColumnMinMax(min, null) if Some(right.value).isDefined =>
                  ordering.lteq(Literal.create(min).value, right.value)
                case ColumnMinMax(min, max) if Some(right.value).isEmpty && (Some(min).isEmpty || Some(max).isEmpty) =>
                  true
                case ColumnMinMax(min, max) =>
                  ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
              }.getOrElse(true)
        }

      case equalTo @ EqualTo(left: Literal, _: AttributeReference |
                                            _: GetStructField |
                                            _: GetMapValue) =>
        val colName = ZIndexUtil.extractRecursively(equalTo.right).mkString(".")
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(colName)
              .map {
                case ColumnMinMax(min, max) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(equalTo.right.dataType)
                  ordering.lteq(Literal(min).value, left.value) && ordering.gteq(Literal(max).value, left.value)
              }.getOrElse(true)
        }
      case _ @ LessThan(left: AttributeReference, right: Literal) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(left.name)
              .map {
                case ColumnMinMax(min, _) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
                  ordering.lt(Literal(min).value, right.value)
              }.getOrElse(true)
        }
      case _ @ LessThan(left: Literal, right: AttributeReference) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(right.name)
              .map {
                case ColumnMinMax(_, max) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(right.dataType)
                  ordering.lt(left.value, Literal(max).value)
              }.getOrElse(true)
        }
      case _ @ LessThanOrEqual(left: AttributeReference, right: Literal) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(left.name)
              .map {
                case ColumnMinMax(min, _) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
                  ordering.lteq(Literal(min).value, right.value)
              }.getOrElse(true)
        }

      case _ @ LessThanOrEqual(left: Literal, right: AttributeReference) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(right.name)
              .map {
                case ColumnMinMax(_, max) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(right.dataType)
                  ordering.lteq(left.value, Literal(max).value)
              }.getOrElse(true)
        }

      case _ @ GreaterThan(left: AttributeReference, right: Literal) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(left.name)
              .map {
                case ColumnMinMax(_, max) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
                  ordering.gt(Literal(max).value, right.value)
              }.getOrElse(true)
        }

      case _ @ GreaterThan(left: Literal, right: AttributeReference) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(right.name)
              .map {
                case ColumnMinMax(min, _) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(right.dataType)
                  ordering.gt(left.value, Literal(min).value)
              }.getOrElse(true)
        }

      case _ @ GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(left.name)
              .map {
                case ColumnMinMax(_, max) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
                  ordering.gteq(Literal(max).value, right.value)
              }.getOrElse(true)
        }
      case _ @ GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.minMax
              .get(right.name)
              .map {
                case ColumnMinMax(min, _) =>
                  val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(right.dataType)
                  ordering.gteq(left.value, Literal(min).value)
              }.getOrElse(true)
        }

      case e @ EqualNullSafe(left: AttributeReference, right: Literal) =>
        fileMetadata
      case e @ EqualNullSafe(left: Literal, right: AttributeReference) =>
        fileMetadata
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
        fileMetadata
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
