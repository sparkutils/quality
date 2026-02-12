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
package org.apache.spark.sql.execution.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, NonSQLExpression, UnsafeProjection, UnsafeRow, UserDefinedExpression}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.DataType

// lifted out of 2.4 to allow Aggregator, only for stats usage
/**
 * The internal wrapper used to hook a [[UserDefinedAggregateFunction]] `udaf` in the
 * internal aggregation code path.
 */
case class ScalaAggregator[IN, BUF, OUT](
                                          children: Seq[Expression],
                                          agg: Aggregator[IN, BUF, OUT],
                                          inputEncoder: ExpressionEncoder[IN],
                                          bufferEncoder: ExpressionEncoder[BUF],
                                          nullable: Boolean = true,
                                          isDeterministic: Boolean = true,
                                          mutableAggBufferOffset: Int = 0,
                                          inputAggBufferOffset: Int = 0,
                                          aggregatorName: Option[String] = None)
  extends TypedImperativeAggregate[BUF]
    with NonSQLExpression
    with UserDefinedExpression
    with ImplicitCastInputTypes
    with Logging {

  // input and buffer encoders are resolved by ResolveEncodersInScalaAgg
  private[this] lazy val inputDeserializer = inputEncoder.resolveAndBind().fromRow(_)
  private[this] lazy val bufferSerializer = bufferEncoder.resolveAndBind().toRow(_)
  private[this] lazy val bufferDeserializer = bufferEncoder.resolveAndBind().fromRow(_)
  private[this] lazy val outputEncoder = agg.outputEncoder.asInstanceOf[ExpressionEncoder[OUT]]
  private[this] lazy val outputSerializer = outputEncoder.resolveAndBind().toRow(_)

  def dataType: DataType = outputEncoder.schema

  def inputTypes: Seq[DataType] = inputEncoder.schema.map(_.dataType)

  override lazy val deterministic: Boolean = isDeterministic

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ScalaAggregator[IN, BUF, OUT] =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ScalaAggregator[IN, BUF, OUT] =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private[this] lazy val inputProjection = UnsafeProjection.create(children)

  def createAggregationBuffer(): BUF = agg.zero

  def update(buffer: BUF, input: InternalRow): BUF =
    agg.reduce(buffer, inputDeserializer(inputProjection(input)))

  def merge(buffer: BUF, input: BUF): BUF = agg.merge(buffer, input)

  def eval(buffer: BUF): Any = {
    val row = outputSerializer(agg.finish(buffer))

    //if (outputEncoder.isSerializedAsStructForTopLevel)  just for stats
      row
    //else row.get(0, dataType)
  }

  private[this] lazy val bufferRow = new UnsafeRow(bufferEncoder.namedExpressions.length)

  def serialize(agg: BUF): Array[Byte] =
    bufferSerializer(agg).asInstanceOf[UnsafeRow].getBytes()

  def deserialize(storageFormat: Array[Byte]): BUF = {
    bufferRow.pointTo(storageFormat, storageFormat.length)
    bufferDeserializer(bufferRow)
  }

  override def toString: String = s"""${nodeName}(${children.mkString(",")})"""

  override def nodeName: String = name

  def name: String = aggregatorName.getOrElse(agg.getClass.getSimpleName)

}
