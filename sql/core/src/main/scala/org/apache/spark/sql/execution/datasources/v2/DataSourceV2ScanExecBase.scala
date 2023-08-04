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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, BoundReference, Expression, RowOrdering, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.catalyst.util.{truncatedString, InternalRowComparableWrapper}
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.{ExplainUtils, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

trait DataSourceV2ScanExecBase extends LeafExecNode {

  lazy val customMetrics = scan.supportedCustomMetrics().map { customMetric =>
    customMetric.name() -> SQLMetrics.createV2CustomMetric(sparkContext, customMetric)
  }.toMap

  override lazy val metrics = {
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) ++
      customMetrics
  }

  def scan: Scan

  def readerFactory: PartitionReaderFactory

  /** Optional partitioning expressions provided by the V2 data sources, through
   * `SupportsReportPartitioning` */
  def keyGroupedPartitioning: Option[Seq[Expression]]

  /** Optional ordering expressions provided by the V2 data sources, through
   * `SupportsReportOrdering` */
  def ordering: Option[Seq[SortOrder]]

  protected def inputPartitions: Seq[InputPartition]

  override def simpleString(maxFields: Int): String = {
    val result =
      s"$nodeName${truncatedString(output, "[", ", ", "]", maxFields)} ${scan.description()}"
    redact(result)
  }

  def partitions: Seq[Seq[InputPartition]] =
    groupedPartitions.map(_.map(_._2)).getOrElse(inputPartitions.map(Seq(_)))

  /**
   * Shorthand for calling redact() without specifying redacting rules
   */
  protected def redact(text: String): String = {
    Utils.redact(session.sessionState.conf.stringRedactionPattern, text)
  }

  override def verboseStringWithOperatorId(): String = {
    val metaDataStr = scan match {
      case s: SupportsMetadata =>
        s.getMetaData().toSeq.sorted.flatMap {
          case (_, value) if value.isEmpty || value.equals("[]") => None
          case (key, value) => Some(s"$key: ${redact(value)}")
          case _ => None
        }
      case _ =>
        Seq(scan.description())
    }
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metaDataStr.mkString("\n")}
       |""".stripMargin
  }

  override def outputPartitioning: physical.Partitioning = {
    keyGroupedPartitioning match {
      case Some(exprs) if KeyGroupedPartitioning.supportsExpressions(exprs) =>
        groupedPartitions
          .map { partitionValues =>
            KeyGroupedPartitioning(exprs, partitionValues.size, partitionValues.map(_._1))
          }
          .getOrElse(super.outputPartitioning)
      case _ =>
        super.outputPartitioning
    }
  }

  @transient lazy val groupedPartitions: Option[Seq[(InternalRow, Seq[InputPartition])]] = {
    // Early check if we actually need to materialize the input partitions.
    keyGroupedPartitioning match {
      case Some(_) => groupPartitions(inputPartitions)
      case _ => None
    }
  }

  /**
   * Group partition values for all the input partitions. This returns `Some` iff:
   *   - [[SQLConf.V2_BUCKETING_ENABLED]] is turned on
   *   - all input partitions implement [[HasPartitionKey]]
   *   - `keyGroupedPartitioning` is set
   *
   * For case where join keys are less than partition keys, we also need to group by join keys.
   *
   * The result, if defined, is a list of tuples where the first element is a partition group value,
   * and the second element is a list of input partitions that share the same partition group value.
   *
   * A non-empty result means each partition is clustered on a single key and therefore eligible
   * for further optimizations to eliminate shuffling in some operations such as join and aggregate.
   */
  def groupPartitions(
      inputPartitions: Seq[InputPartition],
      partitionGroupByPositions: Option[Seq[Boolean]] = None,
      groupSplits: Boolean = !conf.v2BucketingPushPartValuesEnabled ||
        !conf.v2BucketingPartiallyClusteredDistributionEnabled):
    Option[Seq[(InternalRow, Seq[InputPartition])]] = {

    if (!SQLConf.get.v2BucketingEnabled) return None
    keyGroupedPartitioning.flatMap { expressions =>
      val results: Seq[(InternalRow, InputPartition)] = inputPartitions.takeWhile {
        case _: HasPartitionKey => true
        case _ => false
      }.map(p => (p.asInstanceOf[HasPartitionKey].partitionKey(), p))

      if (results.length != inputPartitions.length || inputPartitions.isEmpty) {
        // Not all of the `InputPartitions` implements `HasPartitionKey`, therefore skip here.
        None
      } else {
        // also sort the input partitions according to their partition key order. This ensures
        // a canonical order from both sides of a bucketed join, for example.
        val partitionDataTypes = expressions.map(_.dataType)
        val projectedDataTypes = if (groupSplits && partitionGroupByPositions.isDefined) {
          project(partitionDataTypes, partitionGroupByPositions.get, "partition expressions")
        } else {
          partitionDataTypes
        }
        val partitionOrdering: Ordering[(InternalRow, Seq[InputPartition])] = {
          RowOrdering.createNaturalAscendingOrdering(projectedDataTypes).on(_._1)
        }

        val partitions = if (groupSplits) {
          // Group the splits by their partition value
          results
            .map(t => (projectGroupingKeyIfNecessary(t._1,
              expressions,
              partitionGroupByPositions),
              t._2))
            .groupBy(_._1)
            .toSeq
            .map {
              case (key, s) => (key.row, s.map(_._2))
            }
        } else {
          // No splits grouping, each split will become a separate Spark partition
          results.map(t => (t._1, Seq(t._2)))
        }

        Some(partitions.sorted(partitionOrdering))
      }
    }
  }

  def groupPartitionsByJoinKey(
    partitions: Seq[InternalRow],
    partitionExpressions: Seq[Expression],
    partitionGroupByPositions: Option[Seq[Boolean]]): Seq[InternalRow] = {
    if (partitionGroupByPositions.isDefined) {
      val groupedPartitions = new mutable.HashSet[InternalRowComparableWrapper]
      partitions.foreach { p =>
        groupedPartitions.add(
          projectGroupingKey(p, partitionExpressions, partitionGroupByPositions.get))
      }
      groupedPartitions.map(_.row).toSeq
    } else {
      partitions
    }
  }


  /**
   * Group the common partition values.
   *
   * Similar to groupPartitions, we only do this when
   *   - all input partitions implement [[HasPartitionKey]]
   *   - `keyGroupedPartitioning` is set
   *   - [[SQLConf.V2_BUCKETING_ENABLED]] is turned on (checked before the call)
   *   - we do not replicate partitions (checked before the call)
   *
   * In these cases, when join keys are less than partition keys, we need to group the common
   * partition values by the join keys, and aggregate their value of numSplits.
   *
   * @param commonValuesOption common partition values, if v2.bucketing.pushPartValues enabled
   * @param partitionGroupByPositionsOption position of join keys among partition keys
   * @return a sorted sequence of join key values projected on the common partition values,
   *         with aggregate numSplits of all common partition values with those join key values
   */
  def groupCommonPartitionsByJoinKey(
    commonValuesOption: Option[Seq[(InternalRow, Int)]],
    partitionGroupByPositionsOption: Option[Seq[Boolean]]): Option[Seq[(InternalRow, Int)]] = {
      for {
        expressions <- keyGroupedPartitioning
        commonValues <- commonValuesOption
        partitionGroupByPositions <- partitionGroupByPositionsOption
      } yield {
        val grouped = new mutable.HashMap[InternalRowComparableWrapper, Int]
        commonValues.map(p => {
          val key = projectGroupingKey(p._1, expressions, partitionGroupByPositions)
          (key, p._2)
        }).foreach(p =>
          grouped.put(p._1, grouped.getOrElse(p._1, 0) + p._2)
        )
        grouped.map(r => (r._1.row, r._2)).toSeq
      }
  }

  /**
   * Return common partition values, ordered by the join keys.
   *
   * This is needed in cases where we have fewer join keys than partition keys.
   * @param commonValuesOption
   * @param expressions
   * @param partitionGroupByPositions
   * @param grouped
   * @return
   */
  def sortCommonPartitionsByJoinKey(
    commonValuesOption: Option[Seq[(InternalRow, Int)]],
    expressions: Seq[Expression],
    partitionGroupByPositions: Seq[Boolean],
    grouped: Boolean): Option[Seq[(InternalRow, Int)]] = {
    commonValuesOption.map { commonValues =>
      val ordering: Ordering[(InternalRow, Int)] = if (grouped) {
        val projectedExpressions = project(
          expressions.map(_.dataType),
          partitionGroupByPositions,
          "partition expressions"
        )
        RowOrdering.createNaturalAscendingOrdering(projectedExpressions).on(_._1)
      } else {
        val orders: Seq[SortOrder] = expressions.zipWithIndex.collect {
          case (e, i) if partitionGroupByPositions(i) =>
            SortOrder(BoundReference(i, e.dataType, nullable = true), Ascending)
        }
        RowOrdering.create(orders, Seq.empty).on(_._1)
      }
      commonValues.sorted(ordering)
    }
  }

  def projectGroupingKeyIfNecessary(row: InternalRow, partitionExpressions: Seq[Expression],
    partitionGroupByPositions: Option[Seq[Boolean]]): InternalRowComparableWrapper = {
    if (partitionGroupByPositions.isDefined) {
      projectGroupingKey(row, partitionExpressions, partitionGroupByPositions.get)
    } else {
      InternalRowComparableWrapper(row, partitionExpressions)
    }
  }

  def projectGroupingKey(row: InternalRow, partitionExpressions: Seq[Expression],
    partitionGroupByPositions: Seq[Boolean]): InternalRowComparableWrapper = {
    val values = row.toSeq(partitionExpressions.map(_.dataType))
    val filteredValues = project(values, partitionGroupByPositions, "partition values")
    val filteredExpressions = project(partitionExpressions,
      partitionGroupByPositions, "partition expressions")
    InternalRowComparableWrapper(InternalRow.fromSeq(filteredValues), filteredExpressions)
  }

  def project[T](values: Seq[T], positions: Seq[Boolean], desc: String): Seq[T] = {
    assert(values.size == positions.size,
          s"partition group-by positions map does not match $desc")
    values.zip(positions).filter(_._2).map(_._1)
  }

  def projectIfNecessary[T](
    values: Seq[T],
    positions: Option[Seq[Boolean]],
    desc: String): Seq[T] = {
    if (positions.isDefined) {
      project(values, positions.get, desc)
    } else {
      values
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    val partitioningPreservesOrdering = groupedPartitions.forall(_.forall(_._2.length <= 1))
    ordering.filter(_ => partitioningPreservesOrdering).getOrElse(super.outputOrdering)
  }

  override def supportsColumnar: Boolean = {
    scan.columnarSupportMode() match {
      case Scan.ColumnarSupportMode.PARTITION_DEFINED =>
        require(
          inputPartitions.forall(readerFactory.supportColumnarReads) ||
            !inputPartitions.exists(readerFactory.supportColumnarReads),
          "Cannot mix row-based and columnar input partitions.")
        inputPartitions.exists(readerFactory.supportColumnarReads)
      case Scan.ColumnarSupportMode.SUPPORTED => true
      case Scan.ColumnarSupportMode.UNSUPPORTED => false
    }
  }

  def inputRDD: RDD[InternalRow]

  def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.map { r =>
      numOutputRows += 1
      r
    }
  }

  protected def postDriverMetrics(): Unit = {
    val driveSQLMetrics = scan.reportDriverMetrics().map(customTaskMetric => {
      val metric = metrics(customTaskMetric.name())
      metric.set(customTaskMetric.value())
      metric
    })

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      driveSQLMetrics)
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].map { b =>
      numOutputRows += b.numRows()
      b
    }
  }
}
