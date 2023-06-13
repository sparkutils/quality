package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.{MapUtils, HigherOrderFunctionLike}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MapData}
import org.apache.spark.sql.types.{AbstractDataType, DataType, MapType}

object MapTransform {
  def create(arguement: Expression, key: Expression, function: Expression, zero: DataType => Option[Any]): MapTransform = {
    MapTransform(arguement, key, function, zero)
  }
}

/**
 * Transforms a map
 *
 * @param argument map of type x to y,
 * @param key expr for key
 * @param function value to value transformation for that key entry
 */
case class MapTransform(argument: Expression, key: Expression, function: Expression, zeroF: DataType => Option[Any]) extends HigherOrderFunctionLike with CodegenFallback with SeqArgs {

  lazy val zero = {
    val MapType(_, valueType, _) = argument.dataType

    zeroF(valueType).getOrElse(qualityException(s"Could not find zero for type ${valueType}"))
  }

  // thread local map for indexes
  @transient val indexMap = new java.lang.ThreadLocal[scala.collection.mutable.Map[Any, Int]] {
    override def initialValue: scala.collection.mutable.Map[Any, Int] = {
      scala.collection.mutable.Map[Any, Int]()
    }
  }

  lazy val MapType(keyType, valueType, _) = argument.dataType

  override def children: Seq[Expression] = arguments ++ functions

  override def arguments: Seq[Expression] = Seq(argument, key)

  override def argumentTypes: Seq[AbstractDataType] = arguments.map(_.dataType)

  override def functions: Seq[Expression] = Seq(function)

  override def functionTypes: Seq[AbstractDataType] = Seq(function.dataType)

  protected def bindInternal(f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): HigherOrderFunction =
    copy(function = f(function,
      Seq((valueType, argument.nullable))))

  @transient lazy val LambdaFunction(_, Seq(elementVar: NamedLambdaVariable), _) = function

  override def eval(inputRow: InternalRow): Any = {
    // set up the variable to be evaluated
    val theMap = argument.eval(inputRow).asInstanceOf[MapData]
    val theKey = key.eval(inputRow)

    val iMap = indexMap.get()

    val index =
      iMap.getOrElse(theKey,{
        val i = theMap.keyArray.array.indexOf(theKey)
        if (i < 0) {
          iMap.put(theKey, i)
        }
        i
      })
    if (index > -1) {
      val theValue = theMap.valueArray.array(index)
      elementVar.value.set(theValue)
      /// copy() in case we need copys do the map first
      val theRes = function.eval(inputRow)
      if (theRes != null) {
        theMap.valueArray.update(index, theRes)
      }
      theMap
    } else {
      iMap.put(theKey, theMap.numElements() )
      // first time, needs to be added in
      val keyAr = theMap.keyArray.array
      val valAr = theMap.valueArray.array
      elementVar.value.set(zero)
      val theRes = function.eval(inputRow)
      val toAdd = if (theRes == null) zero else theRes

      new ArrayBasedMapData(new GenericArrayData(keyAr :+ theKey), new GenericArrayData(valAr :+ toAdd) )
    }
  }

  override def dataType: DataType = argument.dataType

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(argument = newChildren(0), key = newChildren(1), function = newChildren(2))
}



/**
 * Transforms a map
 *
 * @param children seq of maps of type x to y, they must all have the same types
 * @param addF function to derive the add expr for monoidal add on values
 */
case class MapMerge(children: Seq[Expression], addF: DataType => Option[( Expression, Expression ) => Expression]) extends Expression with CodegenFallback {

  lazy val MapType(keyType, valueType, _) = children(0).dataType

  lazy val add = addF(valueType).
    getOrElse(qualityException(s"Could not find monoidal add for valueType $valueType"))

  override def eval(inputRow: InternalRow): Any = {

    // set up the variable to be evaluated
    val smap = children.foldLeft( Map[Any, Any]() ) {
      (map, exp ) =>
        val theMap = exp.eval(inputRow).asInstanceOf[MapData]
        val smap = MapUtils.toScalaMap(theMap, keyType, valueType)

        // merge on the matching items
        val added =
          smap.map{ case (key1, value1) =>

            val newVal =
              map.get(key1).fold(
                  // no value, so use original
                  value1
                )( value2 =>
                  // combine
                  add(Literal(value1, valueType), Literal(value2, valueType)).eval(inputRow)
              )

            (key1 -> newVal)
          }

        // merge maps
        map ++ added
    }

    ArrayBasedMapData.apply(smap)
  }

  override def dataType: DataType = children(0).dataType

  override def nullable: Boolean = false

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}