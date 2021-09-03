// scalastyle:off
package org.apache.spark.sql.execution

import com.alibaba.sparkcube.ZIndexUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualNullSafe, EqualTo, Expression, GetMapValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, InSet, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, NonNullLiteral, Not, Or, StartsWith}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitionDirectory, PartitionPath}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class ReplaceHadoopFsRelation() extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case logicalRelation @ LogicalRelation(
      relation @ HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _), _, _, _)
        if ReplaceHadoopFsRelation.relationMetadata.contains(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`") =>

        val zIndexMetadata = ReplaceHadoopFsRelation.relationMetadata(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`"
        )
        val newLocation = new ZIndexInMemoryFileIndex(
          s"${relation.fileFormat.toString.toLowerCase()}.`${location.rootPaths.mkString(",").toLowerCase}`",
          relation.sparkSession, Seq(new Path(zIndexMetadata.basePath)), Map.empty, Option(relation.schema))
        val newRelation = relation.copy(location = newLocation)(relation.sparkSession)
        logicalRelation.copy(relation = newRelation)
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

    try {
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
    } catch {
      case ex: Exception =>
        logError(s"fail on match condition ${filter}", ex)
        files
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

case class ColumnStatistics(
    colName: String,
    min: Option[Any],
    max: Option[Any],
    containsNull: Boolean)

case class FileStatistics(
    file: String,
    numRecords: Long,
    columnStatistics: Map[String, ColumnStatistics],
    filePartitionPath: Option[PartitionPath] = None)

/**
 * 分区表的情况，可能部分分区命中cache，而部分分区未能命中。
 *
 * @param fileMetadata
 */
case class TableMetadata(
    basePath: String,
    fileMetadata: Array[FileStatistics]) extends Logging {

  // 遍历filter条件，获取命中的文件
  /**
   * transforms the conditions and return the hit files.
   *
   * for more about detail of null value semantics, please visit: https://spark.apache.org/docs/latest/sql-ref-null-semantics.html
   *
   * @param condition
   * @return
   */
  def findTouchFileByExpression(condition: Expression): Array[FileStatistics] = {

    def equalTo(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName).forall {
            // `any value = null` return null in spark sql, and this expression will be optimize by catalyst
            // (means this operation will be execute by spark engine).
            // so we don't consider null value for right expression.
            case ColumnStatistics(_, None, None, _) =>
              false
            case ColumnStatistics(_, Some(min), Some(max), _) =>
              ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
          }
      }
    }

    def equalNullSafe(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName).forall {
            // start--- right == null case --- //
            case ColumnStatistics(_, _, _, true) if Option(right.value).isEmpty =>
              true
            case ColumnStatistics(_, _, _, false) if Option(right.value).isEmpty =>
              false
            // end--- right == null case --- //
            // right not null case, the same with EqualTo
            case ColumnStatistics(_, None, None, _) =>
              false
            case ColumnStatistics(_, Some(min), Some(max), _) =>
              ordering.lteq(Literal.create(min).value, right.value) && ordering.gteq(Literal.create(max).value, right.value)
          }
      }
    }

    def lessThan(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName)
            .forall {
              // the same with EqualTo, we don't need consider null value for right Expression.
              case ColumnStatistics(_, None, None, _) =>
                false
              case ColumnStatistics(_, Some(min), _, _) =>
                ordering.lt(Literal(min).value, right.value)
            }
      }
    }

    def lessThanOrEqual(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName)
            .forall {
              // the same with EqualTo, we don't need consider null value for right Expression.
              case ColumnStatistics(_, None, None, _) =>
                false
              case ColumnStatistics(_, Some(min), _, _) =>
                ordering.lteq(Literal(min).value, right.value)
            }
      }
    }

    def greaterThan(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName)
            .forall {
              // the same with EqualTo, we don't need consider null value for right Expression.
              case ColumnStatistics(_, None, None, _) =>
                false
              case ColumnStatistics(_, _, Some(max), _) =>
                ordering.gt(Literal(max).value, right.value)
            }
      }
    }

    def greaterThanOrEqual(left: Expression, right: Literal): Array[FileStatistics] = {
      val colName = ZIndexUtil.extractRecursively(left).mkString(".")
      val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(left.dataType)
      fileMetadata.filter {
        case fileStatistics =>
          fileStatistics.columnStatistics
            .get(colName)
            .forall {
              // the same with EqualTo, we don't need consider null value for right Expression.
              case ColumnStatistics(_, None, None, _) =>
                false
              case ColumnStatistics(_, _, Some(max), _) =>
                ordering.gteq(Literal(max).value, right.value)
            }
      }
    }

    condition match {
      case et @ EqualTo(_: AttributeReference |
                        _: GetStructField |
                        _: GetMapValue, right: Literal) =>
        equalTo(et.left, right)

      case et @ EqualTo(left: Literal, _: AttributeReference |
                                       _: GetStructField |
                                       _: GetMapValue) =>
        equalTo(et.right, left)

      case lt @ LessThan(_: AttributeReference |
                         _: GetStructField |
                         _: GetMapValue, right: Literal) =>
        lessThan(lt.left, right)
      case lt @ LessThan(left: Literal, _: AttributeReference |
                                        _: GetStructField |
                                        _: GetMapValue) =>
        greaterThan(lt.right, left)
      case lteq @ LessThanOrEqual(_: AttributeReference |
                                  _: GetStructField |
                                  _: GetMapValue, right: Literal) =>
        lessThanOrEqual(lteq.left, right)

      case lteq @ LessThanOrEqual(left: Literal, _: AttributeReference |
                                                 _: GetStructField |
                                                 _: GetMapValue) =>
        greaterThanOrEqual(lteq.right, left)

      case gt @ GreaterThan(_: AttributeReference |
                            _: GetStructField |
                            _:GetMapValue, right: Literal) =>
        greaterThan(gt.left, right)

      case gt @ GreaterThan(left: Literal, _: AttributeReference |
                                           _: GetStructField |
                                           _: GetMapValue) =>
        lessThan(gt.right, left)

      case gt @ GreaterThanOrEqual(_: AttributeReference |
                                   _: GetStructField |
                                   _: GetMapValue, right: Literal) =>
        greaterThanOrEqual(gt.left, right)

      case gt @ GreaterThanOrEqual(left: Literal, _: AttributeReference |
                                                  _: GetStructField |
                                                  _: GetMapValue) =>
        lessThanOrEqual(gt.right, left)
      case isNull @ IsNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        val colName = ZIndexUtil.extractRecursively(isNull.child).mkString(".")
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.columnStatistics
              .get(colName)
              .forall {
                case ColumnStatistics(_, _, _, containsNull) => containsNull
              }
        }

      case isNotNull@ IsNotNull(_: AttributeReference | _: GetStructField | _: GetMapValue) =>
        val colName = ZIndexUtil.extractRecursively(isNotNull.child).mkString(".")
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.columnStatistics
              .get(colName)
              .forall {
                case ColumnStatistics(_, _, _, containsNull) => !containsNull
              }
        }

      case _ @ StartsWith(attribute, value @ Literal(_: UTF8String, _)) =>
        val colName = ZIndexUtil.extractRecursively(attribute).mkString(".")
        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(StringType)
        fileMetadata.filter {
          case fileStatistics =>
            fileStatistics.columnStatistics
              .get(colName).forall {
              case ColumnStatistics(_, None, None, _) =>
                false
              case ColumnStatistics(_, Some(min: String), Some(max: String), _) =>
                val minLit = Literal(min)
                val maxLit = Literal(max)
                ordering.lteq(minLit.value, value.value) && ordering.gteq(maxLit.value, value.value) ||
                // if min/max start with value, we should also return this file.
                minLit.value.asInstanceOf[UTF8String].startsWith(value.value.asInstanceOf[UTF8String]) ||
                maxLit.value.asInstanceOf[UTF8String].startsWith(value.value.asInstanceOf[UTF8String])
            }
        }

      case in@ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal]) =>
        list.flatMap {
          // `column in (null)` will not transform to psychical plan, just exclude null value and return empty array.
          case Literal(null, _) => Array.empty[FileStatistics]
          case lit: Literal =>
            equalTo(in.value, lit)
        }.distinct
          .toArray
      // TODO:(fchen) inset support!
      case ens @ EqualNullSafe(_: AttributeReference |
                               _: GetStructField |
                               _: GetMapValue, right: Literal) =>
        equalNullSafe(ens.left, right)
      case ens @ EqualNullSafe(left: Literal, _: AttributeReference |
                                              _: GetStructField |
                                              _: GetMapValue) =>
        equalNullSafe(ens.right, left)
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

      case not @ Not(in @ In(_: AttributeReference | _: GetStructField | _: GetMapValue, list: Seq[Literal])) =>
        val colName = ZIndexUtil.extractRecursively(in.value).mkString(".")
        val ordering: Ordering[Any] = TypeUtils.getInterpretedOrdering(in.value.dataType)
        list.flatMap {
          // `column in (null)` will not transform to psychical plan, just exclude null value and return empty array.
          case Literal(null, _) => return Array.empty[FileStatistics]
          case lit: Literal =>
            fileMetadata.filter {
              case fileStatistics =>
                fileStatistics.columnStatistics
                  .get(colName).forall {
                  // `any value = null` return null in spark sql, and this expression will be optimize by catalyst
                  // (means this operation will be execute by spark engine).
                  // so we don't consider null value for right expression.
                  case ColumnStatistics(_, None, None, _) =>
                    false
                  case ColumnStatistics(_, Some(min), Some(max), _) =>
                    !(ordering.equiv(Literal(min).value, lit.value) && ordering.equiv(Literal(max).value, lit.value))
                }
            }
        }.distinct
          .toArray
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
