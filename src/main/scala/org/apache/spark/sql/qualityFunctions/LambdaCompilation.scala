package org.apache.spark.sql.qualityFunctions

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.util.Testing
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression, HigherOrderFunction, LambdaFunction, LeafExpression, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Replaces NamedLambdaVariables for simple inlined codegen.
 *
 * @param name
 * @param dataType
 * @param nullable
 * @param exprId
 * @param valueRef
 */
case class NamedLambdaVariableCodeGen(
                                       name: String,
                                       dataType: DataType,
                                       nullable: Boolean,
                                       exprId: ExprId,
                                       valueRef: String)
  extends LeafExpression with NamedExpression {

  /**
   * Only used when this is accessed from within a CodegenFallback
   */
  var value: Any = _

  override def eval(input: InternalRow): Any =
    value

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val boxed = CodeGenerator.boxedType(dataType)
    ctx.references += this
    val className = classOf[NamedLambdaVariableCodeGen].getName
    val codeGenExpressionIdx = ctx.references.size - 1

    ev.copy(code =
      code"""
            // LambdaVariable - $name
            $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};

            boolean ${ev.isNull} = $valueRef == null;
            if (!${ev.isNull}) {
              ${ev.value} = ($boxed) $valueRef;
            }
            // for calling other non codegen expressions
            if (!${ev.isNull}) {
              (($className)references[$codeGenExpressionIdx]).value_$$eq( $valueRef );
            } else {
              (($className)references[$codeGenExpressionIdx]).value_$$eq( null );
            }
            """)
  }

  def qualifier: scala.collection.immutable.Seq[String] = qualityException("Should not be called on placeholder after codegen")

  def toAttribute: Attribute = qualityException("Should not be called on placeholder after codegen")

  def newInstance(): NamedExpression = qualityException("Should not be called on placeholder after codegen")
}


/**
 * Functionality related to LambdaCompilation.  Seemingly all HigherOrderFunctions use a lazy val match to extract the
 * NamedLambdaVariable's from the spark LambdaFunction after bind has been called.
 * When doGenCode is called eval _could_ have been called and the lazy val evaluated, as such simply rewriting the tree
 * may not fully work.  Additionally the type for NamedLambdaVariable is bound in the lazy val's which means _ANY_
 * HigherOrderFunction may not tolerate swapping out NamedLambdaVariables for another NamedExpression.
 *
 * To add to the fun OpenSource Spark HoF's all use CodegenFallback, as does NamedLambdaVariable, so it's possible to
 * swap out some of these implementations if an array_transform is nested in a Fun1 or Fun2.  Similarly Fun1's can call Fun2
 * so the assumptions are for each Fun1/FunN doCodeGen:
 *
 * 1. Use the processLambda function to evaluate the function
 * 2. compilationHandlers uses the quality.lambdaHandlers environment variable to load a comma separated list of fqn=handler pairs
 * 3. each fully qualified class name pair (e.g. org.apache.spark.sql.catalyst.expressions.ZipWith=handler.fqn) handler is loaded
 * 4. processLambda then evaluates the expression tree, for each matching HoF classname it will call the handler
 * 5. handlers are used to perform the custom doGenCode for that expression rather than the default OSS CodegenFallback
 * 6. handlers return the ExprCode AND a list of NamedLambdaVariables who must have .value.set called upon them (e.g. we can't optimise them)
 *
 * NB The fqn will also be used to check for named' lambdas used through registerLambdaFunctions.
 *
 * https://github.com/apache/spark/pull/21954 introduced the lambdavariable with AtomicReference, it's inherent performance hit and,
 * due to the difficulty of threading the holder through the expression chain did not have a compilation approach.  After
 * it's threaded and bind has been called the variable id is stable as is the AtomicReference, as such it can be swapped out
 * for a simple variable in the same object.
 *
 * quality.lambdaHandlers will override the default for a given platform on an fqn basis, so you only need to "add" or "replace" the
 * HoFs that cause issue not the entire list of OSS HigherOrderFunctions for example TransformValues.  Note that some versions
 * of Databricks provide compilation of their HoF's that may not be compatible in approach.
 *
 * Disable this approach by using the quality.lambdaHandlers to disable FunN with the default DoCodegenFallbackHandler:
 *   quality.lambdaHandlers=org.apache.spark.sql.qualityFunctions.FunN=org.apache.spark.sql.qualityFunctions.DoCodegenFallbackHandler
 */
object LambdaCompilationUtils {

  trait LambdaCompilationHandler {
    /**
     *
     * @param expr
     * @return empty if the expression should be transformed (i.e. there is a custom solution for it).  Otherwise return the full set of NamedLambdaVariables found
     */
    def shouldTransform(expr: Expression): Seq[NamedLambdaVariable]

    /**
     * Transform the expression using the scope of replaceable named lambda variable expression
     * @param expr
     * @param scope
     * @return
     */
    def transform(expr: Expression, scope: Map[ExprId, NamedLambdaVariableCodeGen]): Expression
  }

  val lambdaENV = "quality.lambdaHandlers"

  def getLambdaEnv = com.sparkutils.quality.getConfig(lambdaENV)

  /**
   * Parses lambda handlers from the quality.lambdaHandlers environment variable as a comma separated lambda|fqn=fqn pair.
   * The key can be either a lambda function name used in registerLambdaFunctions or a fully qualified class name
   * referring to a HigherOrderFunction.
   * The value must be a fully qualified class name for a LambdaCompilationHandler (it is suggested you use a top level class).
   * If you wish to disable compilation for a given lambda or HigherOrderFunction use org.apache.spark.sql.qualityFunctions.DoCodegenFallbackHandler
   * You may use your own to re-write compilation for HoF classes that do not provide it but are hotspots.
   * @return
   */
  def envLambdaHandlers(env: String = getLambdaEnv) =
    if ((env eq null) || env.isEmpty)
      Map.empty[String, String]
    else
      env.split(",").map(_.trim).map { s =>
        val bits = s.split("=")
        bits(0).trim -> bits(1).trim
      }.toMap

  /**
   * Combines the defaultLambdaHandlers for a spark version with environment specific overrides
   * @return
   */
  def loadLambdaCompilationHandlers(lambdaHandlers: Map[String, String] = envLambdaHandlers()): Map[String, Any] =
    lambdaHandlers.map { p =>
      p._1 -> (
        try {
          this.getClass.getClassLoader.loadClass(p._2).newInstance
        } catch {
          case _ : ClassNotFoundException =>
            qualityException(s"$lambdaENV configured handler ${p._1} class ${p._2} cannot be found, please check the configuration")
          case _ : InstantiationException =>
            qualityException(s"$lambdaENV configured handler ${p._1} class ${p._2} has no default constructor, please check the configuration and code")
        }
        )
    }

  def convertToCompilationHandlers(objects: Map[String, Any] = loadLambdaCompilationHandlers()) =
    objects.map{ p =>
      p._1 ->  (
        if ( p._2.isInstanceOf[LambdaCompilationHandler] )
          p._2.asInstanceOf[LambdaCompilationHandler]
        else
          qualityException(s"quality.lambdaHandlers entry ${p._1} does not implement LambdaCompilationHandler")
        )
    }

  private lazy val testing = {
    Testing.testing
  }
  private lazy val cached = convertToCompilationHandlers()

  def compilationHandlers: Map[String, LambdaCompilationHandler] =
    if (testing)
      convertToCompilationHandlers() // re-evaluate
    else
      cached

  val defaultGen = new DoCodegenFallbackHandler

  /**
   * Replaces all NamedLambdaVariables for expressions which are not in compilationHandlers with a simple offset generator
   *
   * If a handler exists (i.e. something should be skipped) it's NamedLambdaVariables are returned as these must be kept.
   *
   * @param expr
   * @param ctx
   * @param ev
   * @return
   */
  def processLambda(expr: Expression, ctx: CodegenContext, ev: ExprCode): Expression = {
    // collection is expensive, do both the nlv and exprs together
    val (exprMaps, allNLVs) =
      expr.collect {
        case e: HigherOrderFunction if compilationHandlers.contains(e.getClass.getName) =>
          Left(e -> compilationHandlers(e.getClass.getName).shouldTransform(e))
        case e @ FunN(_, _, Some(name), _, _, _) if compilationHandlers.contains(name) =>
          Left(e -> compilationHandlers(name).shouldTransform(e))
        case f: FunN =>
          Left(f -> Seq())
        case e: HigherOrderFunction =>
          // default should be to collect as we don't know about it, default catch all for OSS and Databricks, if you have an impl add it
          Left(e -> defaultGen.shouldTransform(e))
        case e: NamedLambdaVariable =>
          Right(e.exprId -> e)
      }.partition(_.isLeft)

    val allToBeExcluded = exprMaps.flatMap(p => p.left.get._2.map( n => n.exprId -> n)).toMap

    val exprIdMaps = allNLVs.map(_.right.get).toMap.filterKeys(k => !allToBeExcluded.contains(k)).map(p => p._2.toCodeGenPair(ctx)).toMap

    def replaceNLVs(expr: Expression) =
      expr.transform{
        case n: NamedLambdaVariable =>
          exprIdMaps.get(n.exprId).fold[Expression](n)(identity)
      }

    def replaceWithHandler(expr: HigherOrderFunction, name: String) =
      compilationHandlers(name).transform(expr, exprIdMaps)

    // correctness with map lookup, ref eq should be fine
    val newTree =
      expr.transformUp {
        case e: HigherOrderFunction if compilationHandlers.contains(e.getClass.getName) =>
          replaceWithHandler(e, e.getClass.getName)
        case e @ FunN(_, _, Some(name), _, _, _) if compilationHandlers.contains(name) =>
          replaceWithHandler(e, name)
        case e: FunN =>
          replaceNLVs(e).asInstanceOf[FunN].
            copy(processed = true, attemptCodeGen = true)
        case e: HigherOrderFunction =>
          // default is don't transform at all
          e
      }

    newTree
  }

  implicit class NamedLambdaVariableOps(nvl: NamedLambdaVariable) {
    def toCodeGenPair(ctx: CodegenContext): (ExprId, NamedLambdaVariableCodeGen) =
      nvl.exprId -> NamedLambdaVariableCodeGen(nvl.name, nvl.dataType, nvl.nullable, nvl.exprId,
        ctx.addMutableState(classOf[Object].getName, "nvlGen"+nvl.name))
  }

}

/**
 * Defaults to calling codeGen, this can either be an original compilation approach or the CodegenFallback depending on implementation.
 * It will evaluate the entire tree of expr and return all NamedLambdaVariables as they will not be using the same compilation approach.
 *
 * This is the default for known OSS implementations and should also be used if compilation will not be within the same class
 */
class DoCodegenFallbackHandler() extends LambdaCompilationUtils.LambdaCompilationHandler {
  def shouldTransform(expr: Expression): Seq[NamedLambdaVariable] =
    expr.collect {
      case LambdaFunction(function, arguments, _) =>
        function.collect {
          case exp: NamedLambdaVariable => exp
        } ++ arguments.collect {
          case exp: NamedLambdaVariable => exp
        }
    }.flatten

  def transform(expr: Expression, scope: Map[ExprId, NamedLambdaVariableCodeGen]): Expression =
    expr
}

/**
 * Generate code for any FunX including nested, normal doGenCode defaults to codegenfallback
 */
trait FunDoGenCode extends CodegenFallback {

  def processed: Boolean
  def attemptCodeGen: Boolean

  def doActualGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /**
   * Will only be called as a top level Fun1 lambda so there is no outerscope to be used
   * @param ctx
   * @param ev
   * @return
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val transformed =
      if (!processed)
      // it's possibly been replaced
        LambdaCompilationUtils.processLambda(this, ctx, ev)
      else
        this // already the right tree

    val newf = transformed.asInstanceOf[FunDoGenCode]
    if (newf.attemptCodeGen)
      newf.doActualGenCode(ctx, ev)
    else
      super[CodegenFallback].doGenCode(ctx, ev)
  }

}

