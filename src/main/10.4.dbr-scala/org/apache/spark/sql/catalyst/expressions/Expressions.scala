package org.apache.spark.sql.catalyst.expressions

import java.util.Locale

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, QuaternaryLike, TernaryLike, TreeNode, UnaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// taken from OSS master https://github.com/apache/spark/commit/cd32c22c9373333d2bd3b89a4ffae1b549396658
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed. A typical use case: [[org.apache.spark.sql.catalyst.optimizer.ConstantFolding]]
   *
   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children. The non-deterministic expressions should not change in number and order. They should
   * not be evaluated during the query planning.
   *
   * Note that this means that an expression should be considered as non-deterministic if:
   * - it relies on some mutable internal state, or
   * - it relies on some implicit input that is not part of the children expression list.
   * - it has non-deterministic child or children.
   * - it assumes the input satisfies some certain condition via the child operator.
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  lazy val deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  /**
   * Workaround scala compiler so that we can call super on lazy vals
   */
  @transient
  private lazy val _references: AttributeSet =
  AttributeSet.fromAttributeSets(children.map(_.references))

  def references: AttributeSet = _references

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[ExprCode]], that contains the Java source code to generate the result of
   * evaluating the expression on an input row.
   *
   * @param ctx a [[CodegenContext]]
   * @return [[ExprCode]]
   */
  def genCode(ctx: CodegenContext): ExprCode = {
    ctx.subExprEliminationExprs.get(ExpressionEquals(this)).map { subExprState =>
      // This expression is repeated which means that the code to evaluate it has already been added
      // as a function before. In that case, we just re-use it.
      ExprCode(
        ctx.registerComment(this.toString),
        subExprState.eval.isNull,
        subExprState.eval.value)
    }.getOrElse {
      val isNull = ctx.freshName("isNull")
      val value = ctx.freshName("value")
      val eval = doGenCode(ctx, ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)))
      reduceCodeSize(ctx, eval)
      if (eval.code.toString.nonEmpty) {
        // Add `this` in the comment.
        eval.copy(code = ctx.registerComment(this.toString) + eval.code)
      } else {
        eval
      }
    }
  }

  private def reduceCodeSize(ctx: CodegenContext, eval: ExprCode): Unit = {
    // TODO: support whole stage codegen too
    val splitThreshold = SQLConf.get.methodSplitThreshold
    if (eval.code.length > splitThreshold && ctx.INPUT_ROW != null && ctx.currentVars == null) {
      val setIsNull = if (!eval.isNull.isInstanceOf[LiteralValue]) {
        val globalIsNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "globalIsNull")
        val localIsNull = eval.isNull
        eval.isNull = JavaCode.isNullGlobal(globalIsNull)
        s"$globalIsNull = $localIsNull;"
      } else {
        ""
      }

      val javaType = CodeGenerator.javaType(dataType)
      val newValue = ctx.freshName("value")

      val funcName = ctx.freshName(nodeName)
      val funcFullName = ctx.addNewFunction(funcName,
        s"""
           |private $javaType $funcName(InternalRow ${ctx.INPUT_ROW}) {
           |  ${eval.code}
           |  $setIsNull
           |  return ${eval.value};
           |}
           """.stripMargin)

      eval.value = JavaCode.variable(newValue, dataType)
      eval.code = code"$javaType $newValue = $funcFullName(${ctx.INPUT_ROW});"
    }
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodegenContext]]
   * @param ev an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  // Expression canonicalization is done in 2 phases:
  //   1. Recursively canonicalize each node in the expression tree. This does not change the tree
  //      structure and is more like "node-local" canonicalization.
  //   2. Find adjacent commutative operators in the expression tree, reorder them to get a
  //      static order and remove cosmetic variations. This may change the tree structure
  //      dramatically and is more like a "global" canonicalization.
  //
  // The first phase is done by `preCanonicalized`. It's a `lazy val` which recursively calls
  // `preCanonicalized` on the children. This means that almost every node in the expression tree
  // will instantiate the `preCanonicalized` variable, which is good for performance as you can
  // reuse the canonicalization result of the children when you construct a new expression node.
  //
  // The second phase is done by `canonicalized`, which simply calls `Canonicalize` and is kind of
  // the actual "user-facing API" of expression canonicalization. Only the root node of the
  // expression tree will instantiate the `canonicalized` variable. This is different from
  // `preCanonicalized`, because `canonicalized` does "global" canonicalization and most of the time
  // you cannot reuse the canonicalization result of the children.

  /**
   * An internal lazy val to implement expression canonicalization. It should only be called in
   * `canonicalized`, or in subclass's `preCanonicalized` when the subclass overrides this lazy val
   * to provide custom canonicalization logic.
   */
  lazy val preCanonicalized: Expression = {
    val canonicalizedChildren = children.map(_.preCanonicalized)
    withNewChildren(canonicalizedChildren)
  }

  /**
   * Returns an expression where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, etc.)  See [[Canonicalize]] for more details.
   *
   * `deterministic` expressions where `this.canonicalized == other.canonicalized` will always
   * evaluate to the same result.
   */
  lazy val canonicalized: Expression = Canonicalize.reorderCommutativeOperators(preCanonicalized)

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   *
   * See [[Canonicalize]] for more details.
   */
  final def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this expression. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   *
   * See [[Canonicalize]] for more details.
   */
  def semanticHash(): Int = canonicalized.hashCode()

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
  def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse(nodeName.toLowerCase(Locale.ROOT))

  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

  /**
   * Returns SQL representation of this expression.  For expressions extending [[NonSQLExpression]],
   * this method may return an arbitrary user facing string.
   */
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

  override def simpleStringWithNodeId(): String = {
    throw new IllegalStateException(s"$nodeName does not implement simpleStringWithNodeId")
  }

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}
