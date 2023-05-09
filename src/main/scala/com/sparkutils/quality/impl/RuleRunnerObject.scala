package com.sparkutils.quality.impl

import com.sparkutils.quality.utils.{PrintCode, StructFunctions}
import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.impl.aggregates.AggregateExpressions
import com.sparkutils.quality.impl.bloom.{BucketedArrayParquetAggregator, ParquetAggregator}
import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory, ZALongHashFunctionFactory, ZALongTupleHashFunctionFactory}
import com.sparkutils.quality.impl.id.{AsBase64Fields, AsBase64Struct, GenericLongBasedIDExpression, GuaranteedUniqueID, GuaranteedUniqueIdIDExpression, SizeOfIDString, model}
import com.sparkutils.quality.impl.rng.{RandLongsWithJump, RandomBytes, RandomLongs}
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPairExpression, PrefixedToLongPair}
import com.sparkutils.quality.impl.util.{ComparableMapConverter, ComparableMapReverser}
import com.sparkutils.quality.{ExprLogic, RuleSuite, impl}
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.QualitySparkUtils.add
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, And, AttributeReference, EqualTo, Expression, LambdaFunction, Literal, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.qualityFunctions.LambdaFunctions.processTopCallFun
import org.apache.spark.sql.qualityFunctions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QualitySparkUtils, SparkSession, functions}
import org.apache.spark.unsafe.types.UTF8String

trait RuleRunnerFunctionsImport {

  import RuleRunnerFunctions._

  val maxDec = DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE)

  private val noopAdd = (dt: DataType) => None

  /**
   * Provides the default monoidal add for a dataType, used for merging summed results when aggregating
   *
   * @param dataType
   * @return
   */
  def defaultAdd(dataType: DataType, extension: DataType => Option[(Expression, Expression) => Expression] = noopAdd): Option[(Expression, Expression) => Expression] =
    dataType match {
      case _: MapType => Some((left, right) => MapMerge(Seq(left, right), (dataType: DataType) => defaultAdd(dataType, extension)))
      case _: IntegerType | LongType | DoubleType =>
        Some((left, right) => add(left, right, null))
      case a: DecimalType =>
        Some((left, right) => add(left, right, a))
      case _ => extension(dataType)
    }

  /**
   * Provides the default monoidal Zero for a dataType, used for defaults when aggregating
   *
   * @param dataType
   * @return
   */
  def defaultZero(dataType: DataType): Option[Any] =
    dataType match {
      case _: MapType => Some(EmptyMap)
      case _: IntegerType | LongType => Some(0L)
      case _: DoubleType => Some(0.0)
      case d: DecimalType => Some(Decimal.createUnsafe(0, d.precision, d.scale))
      case _ => None
    }

  /**
   * Wrap to provide the default lookup for registerFunctions to change type parsing from DDL based to other or
   * when None to add additional lookups should ddl fail
   *
   * @param string
   * @return
   */
  def defaultParseTypes(string: String): Option[DataType] =
    try {
      Some(
        DataType.fromDDL(string)
      )
    } catch {
      case _: Throwable => None
    }

  val INC_REWRITE_GENEXP_ERR_MSG: String = "inc('DDL', generic expression) is not supported in NO_REWRITE mode, use inc(generic expression) without NO_REWRITE mode enabled"

  /**
   * Must be called before using any functions like Passed, Failed or Probability(X)
   * @param parseTypes override type parsing (e.g. DDL, defaults to defaultParseTypes / DataType.fromDDL)
   * @param zero override zero creation for aggExpr (defaults to defaultZero)
   * @param add override the "add" function for aggExpr types (defaults to defaultAdd(dataType))
   * @param writer override the printCode and printExpr print writing function (defaults to println)
   * @param register function to register the sql extensions
   */
  def registerQualityFunctions(parseTypes: String => Option[DataType] = defaultParseTypes _,
                               zero: DataType => Option[Any] = defaultZero _,
                               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType),
                               mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType),
                               writer: String => Unit = println(_),
                               register: (String, Seq[Expression] => Expression) => Unit =
                                  QualitySparkUtils.registerFunction(SparkSession.getActiveSession.get.sessionState.functionRegistry) _
                       ) {
    register("comparableMaps", exps => ComparableMapConverter(exps(0), mapCompare))
    register("reverseComparableMaps", exps => ComparableMapReverser(exps(0)))

    val f = (exps: Seq[Expression]) => ProbabilityExpr(exps.head)
    register("probability", f)
    val ff = (exps: Seq[Expression]) => FlattenResultsExpression(exps.head, FlattenStruct.ruleSuiteDeserializer)
    register("flattenResults", ff)
    register("flattenRuleResults", exps => FlattenRulesResultsExpression(exps(0), FlattenStruct.ruleSuiteDeserializer))
    register("flattenFolderResults", exps => FlattenFolderResultsExpression(exps(0), FlattenStruct.ruleSuiteDeserializer))

    register("passed", _ => com.sparkutils.quality.PassedExpr)
    register("failed", _ => com.sparkutils.quality.FailedExpr)
    register("softFailed", _ => com.sparkutils.quality.SoftFailedExpr)
    register("disabledRule", _ => com.sparkutils.quality.DisabledRuleExpr)

    val pif = (exps: Seq[Expression]) => Pack(exps(0), exps(1))
    register("packInts", pif)

    val upif = (exps: Seq[Expression]) => UnPack(exps(0))
    register("unpack", upif)

    val uptif = (exps: Seq[Expression]) => UnPackIdTriple(exps(0))
    register("unpackIdTriple", uptif)


    val sf = (exps: Seq[Expression]) => SoftFailExpr(exps.head)
    register("softFail", sf)

    def strType(exp: Expression) = {
      val Literal(str: UTF8String, StringType) = exp // only accept type as string
      str.toString
    }

    def parse(exp: Expression) = {
      val Literal(str: UTF8String, StringType) = exp // only accept type as string
      parseTypes(str.toString).getOrElse(qualityException(s"Could not parse the type $str"))
    }

    register(LambdaFunctions.PlaceHolder, {
      case Seq(e, Literal(bol: Boolean, BooleanType)) =>
        PlaceHolderExpression(parse(e), bol)
      case Seq(e) =>
        PlaceHolderExpression(parse(e))
      case _ =>
        PlaceHolderExpression(LongType)
    })

    /* Note - both Lambda and CallFun are only called in top level expressions,
          nested calls are handled within the lambda expression
          creation that "calls" this. */

    register(LambdaFunctions.Lambda, {
      case Seq(fun: FunForward) =>
        val res = FunCall(fun)
        res
      case Seq(fun: FunN) =>
        // placeholders that are 1:1
        val res = fun.function
        res
    })

    register(LambdaFunctions.CallFun, {
      case (fun@ FunN(_, l@ LambdaFunction(ff : FunForward, _, _), _, _, _)) +: args =>
        processTopCallFun(fun, l, ff, args)
      case t => qualityException(s"${LambdaFunctions.CallFun} should only be used to process partially applied functions returned by a user lambda, got $t instead")
    })

    val afx = (exps: Seq[Expression]) => {
      val (sumType, filter, sum, count) =
        exps.size match {
          case 3 =>
            // attempt to take a look at exps1 to identify if it's a FunN or mapWith
            val typ =
              exps(1) match {
                case FunN(Seq(RefExpression(dataType, _, _)), _, _, _, _) => dataType // would default to long anyway
                case MapTransform(RefExpression(t: MapType, _, _), _, _, _) => t
                case _ => LongType
              }
            (typ, exps(0), exps(1), exps(2))
          case 4 =>
            val Literal(str: UTF8String, StringType) = exps(0) // only accept type as string
            if (str.toString == "NO_REWRITE")
              // signal not to replace types
              (null, exps(1), exps(2), exps(3))
            else
              (parse(exps(0)), exps(1), exps(2), exps(3))
        }
      AggregateExpressions(sumType, filter, sum, count, zero, add)
    }
    register("aggExpr", afx)

    register("sumWith", (exps: Seq[Expression]) => {
      val (dataType, origExp) = exps.size match {
        case 1 => (LongType, exps(0))
        // backwards compat
        case 2 => (parse(exps(0)), exps(1))
      }
      FunN(Seq(RefExpression(dataType)), origExp, Some("sumWith"))
    })

    val ff2 = (exps: Seq[Expression]) => {
      // real type for param1 is changed by aggrExpr, but last works for all compat as well
      val (sumType, exp) =
        exps.size match {
          case 1 => (LongType, exps(0))
          case 2 => (parse(exps(0)), exps(1)) // support the NO_REWRITE override case
        }

      FunN(Seq(RefExpression(sumType), RefExpression(LongType)), exp, Some("resultsWith"))
    }
    register("resultsWith", ff2)

    val mapFX = (exps: Seq[Expression]) => exps.size match {
      case 3 =>
        // parse it for old sql to support backwards aggExpr
        MapTransform.create(RefExpression(parse(exps(0))), exps(1), exps(2), zero)
      case 2 =>
        // default to LongType, aggrExpr must fix, 2nd param is key, third the func manipulating the key
        MapTransform.create(RefExpression(MapType(LongType, LongType)), exps(0), exps(1), zero)
    }
    register("mapWith", mapFX)

    def aggFWith(fun: String) = (what: String) => (exps: Seq[Expression]) => (
      if (exps.size == 0)
        functions.expr(s"$fun( $what )")
      else
        functions.expr(s"$fun('${strType(exps(0))}', $what )")
      ).expr

    val retWith = aggFWith("resultsWith")

    // common cases
    register("meanF", retWith("(sum, count) -> sum / count"))

    val sumWith = aggFWith("sumWith")

    val incX = (exps: Seq[Expression]) => exps match {
      case Seq(x: AttributeReference) =>
        val name = x.qualifier.mkString(".") + x.name // that is bad code man should be option
        sumWith(s"sum -> sum + $name")(Seq())
      case Seq(Literal(str: UTF8String, StringType)) =>
        // case for type passing
        sumWith("sum -> sum + 1")(exps)
      case Seq(Literal(str: UTF8String, StringType), x: AttributeReference) =>
        val name = x.qualifier.mkString(".") + x.name
        sumWith(s"sum -> sum + $name")(Seq(exps(0))) // keep the type, drop the attr
      case Seq(Literal(str: UTF8String, StringType), y) =>
        qualityException(INC_REWRITE_GENEXP_ERR_MSG)
      case Seq( y ) =>
        val LambdaFunction(a: Add, Seq(sum: UnresolvedNamedLambdaVariable), hidden ) = functions.expr("sumWith(sum -> sum + 1)").expr.children(0)
        import QualitySparkUtils.{add => addf}
        // could be a cast around x or three attributes plusing each other or....
        FunN(Seq(RefExpression(LongType)),
          LambdaFunction(addf(a.left, y, LongType), Seq(sum), hidden )
          , Some("inc")) // keep the type
      case Seq() => functions.expr(s"sumWith(sum -> sum + 1)").expr
    }
    register("inc", incX)

    // return sum
    register("returnSum", retWith("(sum, count) -> sum"))


    // random generators
    val brf = (exps: Seq[Expression]) => {
      def getRandom(exp: Expression) = {
        val str = getString(exp)
        RandomSource.valueOf(str)
      }

      //numBytes: Int, randomSource: RandomSource, seed: Long constructor but needs to use random, seed, numbytes
      val (numBytes: Int, randomSource, seed: Long) =
        exps.size match {
          case 0 => (16, RandomSource.XO_RO_SHI_RO_128_PP, 0L)
          case 1 => (16, getRandom(exps(0)), 0L)
          case 2 => (16, getRandom(exps(0)), getLong(exps(1)))
          case 3 => (getLong(exps(2)).toInt, getRandom(exps(0)), getLong(exps(1)))
          case _ => literalsNeeded
        }

      RandomBytes(numBytes, randomSource, seed)
    }
    register("rngBytes", brf)
    def getRandom(exp: Expression) = {
      val str = getString(exp)
      RandomSource.valueOf(str)
    }

    // random generators
    val lrf = (exps: Seq[Expression]) => {

      //randomSource: RandomSource, seed: Long constructor but needs to use random, seed, numbytes
      val (randomSource, seed: Long) =
        exps.size match {
          case 0 => (RandomSource.XO_RO_SHI_RO_128_PP, 0L)
          case 1 => (getRandom(exps(0)), 0L)
          case 2 => (getRandom(exps(0)), getLong(exps(1)))
          case _ => literalsNeeded
        }

      RandomLongs.create(randomSource, seed)
    }
    register("rng", lrf)

    register("rngUUID", (exps: Seq[Expression]) => RngUUIDExpression(exps.head))

    register("longPair", (exps: Seq[Expression]) => LongPairExpression(exps(0), exps(1)))
    register("longPairFromUUID", (exps: Seq[Expression]) => UUIDToLongsExpression(exps.head))

    register("smallBloom", (exps: Seq[Expression]) => ParquetAggregator(exps(0), exps(1), exps(2)))

    register("bigBloom", (exps: Seq[Expression]) => BucketedArrayParquetAggregator(exps(0), exps(1), exps(2), exps(3)))

    val longPairEqual = (exps: Seq[Expression]) => {
      val Seq(Literal(a, StringType), Literal(b, StringType)) = exps

      def lower(a: Any) = UnresolvedAttribute(s"${a}_lower")

      def higher(a: Any) = UnresolvedAttribute(s"${a}_higher")

      And(EqualTo(lower(a), lower(b)), EqualTo(higher(a), higher(b)))
    }
    register("longPairEqual", longPairEqual)

    val idEqual = (exps: Seq[Expression]) => {
      val Seq(Literal(a, StringType), Literal(b, StringType)) = exps

      def attr(a: Any, field: String) = UnresolvedAttribute(s"${a}_$field")

      And(And(EqualTo(attr(a,"base"), attr(b, "base")),
        EqualTo(attr(a,"i0"), attr(b, "i0"))),
        EqualTo(attr(a,"i1"), attr(b, "i1")))
    }
    register("idEqual", idEqual)

    register("as_uuid", exps => AsUUID(exps(0), exps(1)))

    register("ruleSuiteResultDetails", (exps: Seq[Expression]) => impl.RuleSuiteResultDetails(exps(0)))

    register("digestToLongsStruct", digestToLongs(true))

    def digestToLongs(asStruct: Boolean = true) = (exps: Seq[Expression]) => {
      val digestImpl = getString(exps.head)
      HashFunctionsExpression(exps.tail, digestImpl, asStruct, MessageDigestFactory(digestImpl))
    }
    register("digestToLongs", digestToLongs(false))

    def fieldBasedID(factory: String => DigestFactory) = (exps: Seq[Expression]) =>
        exps.size match {
          case a if a > 2 =>
            val digestImpl = getString(exps(1))
            GenericLongBasedIDExpression(model.FieldBasedID,
              HashFunctionsExpression(exps.drop(2), digestImpl, true, factory(digestImpl)), getString(exps.head))

          case _ => literalsNeeded
        }

    register("fieldBasedID", fieldBasedID(MessageDigestFactory))
    register("zaLongsFieldBasedID", fieldBasedID(ZALongTupleHashFunctionFactory))
    register("zaFieldBasedID", fieldBasedID(ZALongHashFunctionFactory))
    register("hashFieldBasedID", fieldBasedID(HashFunctionFactory(_)))

    val providedID = (exps: Seq[Expression]) =>
      exps.size match {
        case 2 =>
          GenericLongBasedIDExpression(model.ProvidedID,
            exps(1), getString(exps.head))

        case _ => literalsNeeded
      }

    register("providedID", providedID)

    val prefixedToLongPair = (exps: Seq[Expression]) =>
      exps.size match {
        case 2 =>
          PrefixedToLongPair(exps(1), getString(exps.head))

        case _ => literalsNeeded
      }
    register("prefixedToLongPair", prefixedToLongPair)

    val rngID = (exps: Seq[Expression]) => {
      val (randomSource, seed: Long, prefix) =
        exps.size match {
          case 1 => ( RandomSource.XO_RO_SHI_RO_128_PP, 0L, getString(exps.head))
          case 2 => ( getRandom(exps(1)), 0L,  getString(exps.head))
          case 3 => ( getRandom(exps(1)), getLong(exps(2)),  getString(exps.head))
          case _ => literalsNeeded
        }

      GenericLongBasedIDExpression(model.RandomID,
        RandLongsWithJump(seed, randomSource), prefix) // only jumpables work
    }
    register("rngID", rngID)

    val uniqueID = (exps: Seq[Expression]) => {
      val (prefix) =
        exps.size match {
          case 1 => getString(exps.head)
          case _ => literalsNeeded
        }

      GuaranteedUniqueIdIDExpression(
        GuaranteedUniqueID()
        , prefix
      )
    }
    register("uniqueID", uniqueID)

    register("id_size", exps => SizeOfIDString(exps.head))
    register("id_base64", {
      case Seq(e) => AsBase64Struct(e)
      case s => AsBase64Fields(s)
    })

    val Murmur3_128_64 = (exps: Seq[Expression]) => {
      val (prefix) =
        exps.size match {
          case a if a < 2 => literalsNeeded
          case _ => getString(exps.head)
        }
      GenericLongBasedIDExpression(model.FieldBasedID,
        HashFunctionsExpression(exps.tail, "IGNORED", true, HashFunctionFactory("IGNORED")), prefix)
    }
    register("murmur3ID", Murmur3_128_64)

    def hashWithF(asStruct: Boolean) = (exps: Seq[Expression]) => {
      val (impl) =
        exps.size match {
          case a if a < 2 => literalsNeeded
          case _ => getString(exps.head)
        }
      HashFunctionsExpression(exps.tail, impl, asStruct, HashFunctionFactory(impl))
    }
    register("hashWith", hashWithF(false))
    register("hashWithStruct", hashWithF(true))

    def zahashF(asStruct: Boolean) = (exps: Seq[Expression]) => {
      val (digestImpl) =
        exps.size match {
          case a if a < 2 => literalsNeeded
          case _ => getString(exps.head)
        }
      HashFunctionsExpression(exps.tail, digestImpl, asStruct, ZALongHashFunctionFactory(digestImpl))
    }
    register("zaHashWith", zahashF(false)) // 64bit only, not a great id choice
    register("zaHashWithStruct", zahashF(true)) // 64bit only, not a great id choice

    def zaTuplehashF(asStruct: Boolean) = (exps: Seq[Expression]) => {
      val (digestImpl) =
        exps.size match {
          case a if a < 2 => literalsNeeded
          case _ => getString(exps.head)
        }
      HashFunctionsExpression(exps.tail, digestImpl, asStruct, ZALongTupleHashFunctionFactory(digestImpl))
    }
    register("zaHashLongsWith", zaTuplehashF(false))
    register("zaHashLongsWithStruct", zaTuplehashF(true))

    // here to stop these functions being used and allow validation
    register("coalesceIfAttributesMissing", _ => qualityException("coalesceIf functions cannot be created") )
    register("coalesceIfAttributesMissingDisable", _ => qualityException("coalesceIf functions cannot be created") )

    // The MSE library adds this lens functionality, 3.1.1 introduces this to dsl but neither does an sql interface
    register("updateField", StructFunctions.withFieldFunction)

    def msgAndExpr(msgDefault: String, exps: Seq[Expression]) = exps match {
      case Seq(Literal(str: UTF8String, StringType), e: Expression) =>
        (str.toString, e)
      case Seq(e: Expression) =>
        (msgDefault, e)
    }

    register("printCode", (exps: Seq[Expression]) => {
      val (msg, exp) = msgAndExpr(PrintCode(exps(0)).msg, exps)
      PrintCode(exp, msg, writer)
    })
    register("printExpr", (exps: Seq[Expression]) => {
      val (msg, exp) = msgAndExpr("Expression toStr is ->", exps)
      writer(s"$msg $exp .  Sql is ${exp.sql}")
      exp
    })
  }

}

object RuleRunnerFunctions {

  protected[quality] def literalsNeeded = qualityException("Cannot setup expression with non-literals")
  protected[quality] def getLong(exp: Expression) =
    exp match {
      case Literal(seed: Long, LongType) => seed
      case _ => literalsNeeded
    }
  protected[quality] def getString(exp: Expression) =
    exp match {
      case Literal(str: UTF8String, StringType) => str.toString()
      case _ => literalsNeeded
    }

  protected[quality] def flattenExpressions(ruleSuite: RuleSuite): Seq[Expression] =
    ruleSuite.ruleSets.flatMap( ruleSet => ruleSet.rules.map(rule =>
      rule.expression match {
        case r: ExprLogic => r.expr // only ExprLogic are possible here
      }))

  val qualityFunctions = Set("murmur3ID","uniqueID","rngID","providedID","fieldBasedID",
    "digestToLongs","digestToLongsStruct","ruleSuiteResultDetails","idEqual","longPairEqual","bigBloom","smallBloom",
    "longPairFromUUID","longPair","rngUUID","rng","rngBytes","returnSum","sumWith","resultsWith",
    "inc","meanF","aggExpr","passed","failed","softFailed","disabledRule","packInts","unpack",
    "unpackIdTriple","softFail","probability","flattenResults","flattenRuleResults", "flattenFolderResults", "probabilityIn",
    "mapLookup","mapContains","saferLongPair","hashWith","hashWithStruct","zaHashWith", "zaHashLongsWith",
    "hashFieldBasedID","zaLongsFieldBasedID","zaHashLongsWithStruct", "zaHashWithStruct", "zaFieldBasedID", "prefixedToLongPair",
    "coalesceIfAttributesMissing", "coalesceIfAttributesMissingDisable", "updateField", LambdaFunctions.PlaceHolder,
    LambdaFunctions.Lambda, LambdaFunctions.CallFun, "printExpr", "printCode", "comparableMaps", "reverseComparableMaps", "as_uuid",
    "id_size", "id_base64"
  )
}