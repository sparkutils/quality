package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.functions._
import com.sparkutils.quality.impl.aggregates.AggregateExpressions
import com.sparkutils.quality.impl.bloom.{BucketedArrayParquetAggregator, ParquetAggregator}
import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression}
import com.sparkutils.quality.impl.id.{GenericLongBasedIDExpression, model}
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPairExpression}
import com.sparkutils.quality.impl.rng.{RandomBytes, RandomLongs}
import com.sparkutils.quality.impl.util.{ComparableMapConverter, ComparableMapReverser, PrintCode}
import com.sparkutils.quality.impl.yaml.{YamlDecoderExpr, YamlEncoderExpr}
import com.sparkutils.quality.{QualityException, impl}
import com.sparkutils.shim
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.ShimUtils.{add, column, expression}
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, CreateMap, Expression, Literal, UnresolvedNamedLambdaVariable, LambdaFunction => SLambdaFunction}
import org.apache.spark.sql.qualityFunctions.LambdaFunctions.processTopCallFun
import org.apache.spark.sql.qualityFunctions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, ShimUtils, SparkSession, functions}
import org.apache.spark.unsafe.types.UTF8String

object RuleRegistrationFunctions {

  protected[quality] def literalsNeeded(pos: Int): Nothing =
    if (pos == -1)
      qualityException("Cannot setup Quality Expression with non-literals")
    else
      qualityException(s"Quality Expression requires a string literal in (starts with position 0) position $pos")

  protected[quality] def literalsNeeded: Nothing = literalsNeeded( -1 )

  protected[quality] def getLong(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(seed: Long, LongType) => seed
      case _ => literalsNeeded(pos)
    }
  protected[quality] def getInteger(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(seed: Int, IntegerType) => seed
      case _ => literalsNeeded(pos)
    }
  protected[quality] def getString(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(str: UTF8String, StringType) => str.toString()
      case _ => literalsNeeded(pos)
    }

  protected[quality] val mustKeepNames = Set(LambdaFunctions.PlaceHolder,
    LambdaFunctions.Lambda, LambdaFunctions.CallFun)

  val qualityFunctions = {
    val withUnderscores = Set("murmur3_ID","unique_ID","rng_ID","provided_ID","field_Based_ID",
      "digest_To_Longs","digest_To_Longs_Struct","rule_Suite_Result_Details","id_Equal","long_Pair_Equal","big_Bloom","small_Bloom",
      "long_Pair_From_UUID","long_Pair","rng_UUID","rng","rng_Bytes","return_Sum","sum_With","results_With",
      "inc","meanF","agg_Expr","passed","failed","soft_Failed","disabled_Rule","pack_Ints","unpack",
      "unpack_Id_Triple","soft_Fail","probability","flatten_Results","flatten_Rule_Results", "flatten_Folder_Results", "probability_In",
      "map_Lookup","map_Contains","hash_With","hash_With_Struct","za_Hash_With", "za_Hash_Longs_With",
      "hash_Field_Based_ID","za_Longs_Field_Based_ID","za_Hash_Longs_With_Struct", "za_Hash_With_Struct", "za_Field_Based_ID", "prefixed_To_Long_Pair",
      "coalesce_If_Attributes_Missing", "coalesce_If_Attributes_Missing_Disable", "update_Field", LambdaFunctions.PlaceHolder,
      LambdaFunctions.Lambda, LambdaFunctions.CallFun, "print_Expr", "print_Code", "comparable_Maps", "reverse_Comparable_Maps", "as_uuid",
      "id_size", "id_base64", "id_from_base64", "id_raw_type", "rule_result", "strip_result_ddl", "drop_field",
      "to_yaml", "from_yaml"
    )
    withUnderscores ++ withUnderscores.map(n => if (mustKeepNames(n)) n else n.replaceAll("_",""))
  }

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

  // #12 - use underscore names but keep the old camel case approach around for compat
  def registerWithChecks(registerFunction: (String, Seq[Expression] => Expression) => Unit, name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) = {
    val create =
      if (paramNumbers.isEmpty && minimum == -1 )
        argsf
      else
        (exps: Seq[Expression]) => {
          if ((paramNumbers.nonEmpty && !paramNumbers.contains(exps.size)) || (minimum > exps.size)) {
            val sizeerr =
              if (paramNumbers.nonEmpty)
                s"Valid parameter counts are ${paramNumbers.mkString(", ")}"
              else
                s"A minimum of $minimum parameters is required."
            throw QualityException(s"Wrong number of arguments provided to Quality function $name. $sizeerr")
          }
          argsf(exps)
        }
    registerFunction(name, create)
    if (!mustKeepNames(name)) {
      registerFunction(name.replaceAll("_",""), create)
    }
  }

  /**
   * Must be called before using any functions like Passed, Failed or Probability(X)
   * @param parseTypes override type parsing (e.g. DDL, defaults to defaultParseTypes / DataType.fromDDL)
   * @param zero override zero creation for aggExpr (defaults to defaultZero)
   * @param add override the "add" function for aggExpr types (defaults to defaultAdd(dataType))
   * @param writer override the printCode and printExpr print writing function (defaults to println)
   * @param registerFunction function to register the sql extensions
   */
  def registerQualityFunctions(parseTypes: String => Option[DataType] = defaultParseTypes _,
                               zero: DataType => Option[Any] = defaultZero _,
                               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType),
                               mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType),
                               writer: String => Unit = println(_),
                               registerFunction: (String, Seq[Expression] => Expression) => Unit =
                                 ShimUtils.registerFunction(SparkSession.getActiveSession.get.sessionState.functionRegistry) _
                              ) {

    def register(name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(registerFunction, name, argsf, paramNumbers, minimum)

    def parse(exp: Expression) = {
      val Literal(str: UTF8String, StringType) = exp // only accept type as string
      parseTypes(str.toString).getOrElse(qualityException(s"Could not parse the type $str"))
    }

    def getMap(exp: Expression) = exp match {
      case c: CreateMap if c.children.grouped(2).forall{
        case Seq(Literal(_: UTF8String, StringType), _: Literal) =>
          true
        case _ => false
      } =>
        c.children.grouped(2).map{
          case Seq(Literal(str: UTF8String, StringType), value: Literal) =>
            str.toString -> value.value.toString()
        }.toMap
      case _ => throw QualityException(s"Could not process a literal map with expression $exp")
    }

    register("to_yaml", exps => YamlEncoderExpr(exps.head, if (exps.size == 1) Map.empty else getMap(exps.last)), Set(1, 2))
    register("from_yaml", exps => YamlDecoderExpr(exps.head, parse(exps.last)), Set(2))

    register("strip_result_ddl", exps => StripResultTypes(exps.head), Set(1))
    register("rule_result", exps => RuleResultExpression(Seq(exps(0), exps(1), exps(2), exps(3))), Set(4))

    register("comparable_Maps", exps => ComparableMapConverter(exps(0), mapCompare), Set(1))
    register("reverse_Comparable_Maps", exps => ComparableMapReverser(exps.head), Set(1))

    register("probability", exps => ProbabilityExpr(exps.head), Set(1))
    register("flatten_Results", exps => FlattenResultsExpression(exps.head, FlattenStruct.ruleSuiteDeserializer), Set(1))
    register("flatten_Rule_Results", exps => FlattenRulesResultsExpression(exps.head, FlattenStruct.ruleSuiteDeserializer), Set(1))
    register("flatten_Folder_Results", exps => FlattenFolderResultsExpression(exps.head, FlattenStruct.ruleSuiteDeserializer), Set(1))

    register("passed", _ => com.sparkutils.quality.impl.imports.RuleResultsImports.PassedExpr, Set(0))
    register("failed", _ => com.sparkutils.quality.impl.imports.RuleResultsImports.FailedExpr, Set(0))
    register("soft_Failed", _ => com.sparkutils.quality.impl.imports.RuleResultsImports.SoftFailedExpr, Set(0))
    register("disabled_Rule", _ => com.sparkutils.quality.impl.imports.RuleResultsImports.DisabledRuleExpr, Set(0))

    register("pack_Ints", exps => Pack(exps(0), exps(1)), Set(2))

    register("unpack", exps => UnPack(exps.head), Set(1))

    register("unpack_Id_Triple", exps => UnPackIdTriple(exps.head), Set(1))

    register("soft_Fail", exps => SoftFailExpr(exps.head), Set(1))

    def strType(exp: Expression) = {
      val Literal(str: UTF8String, StringType) = exp // only accept type as string
      str.toString
    }

    register(LambdaFunctions.PlaceHolder, {
      case Seq(e, Literal(bol: Boolean, BooleanType)) =>
        PlaceHolderExpression(parse(e), bol)
      case Seq(e) =>
        PlaceHolderExpression(parse(e))
      case _ =>
        PlaceHolderExpression(LongType)
    }, Set(0, 1, 2))

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
    }, Set(1))

    register(LambdaFunctions.CallFun, {
      case (fun@ FunN(_, l@ SLambdaFunction(ff : FunForward, _, _), _, _, _)) +: args =>
        processTopCallFun(fun, l, ff, args)
      case t => qualityException(s"${LambdaFunctions.CallFun} should only be used to process partially applied functions returned by a user lambda, got $t instead")
    }, minimum = 1)

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
    register("agg_Expr", afx, Set(3, 4))

    register("sum_With", (exps: Seq[Expression]) => {
      val (dataType, origExp) = exps.size match {
        case 1 => (LongType, exps(0))
        // backwards compat
        case 2 => (parse(exps(0)), exps(1))
      }
      FunN(Seq(RefExpression(dataType)), origExp, Some("sum_With"))
    }, Set(1, 2))

    val ff2 = (exps: Seq[Expression]) => {
      // real type for param1 is changed by aggrExpr, but last works for all compat as well
      val (sumType, exp) =
        exps.size match {
          case 1 => (LongType, exps(0))
          case 2 => (parse(exps(0)), exps(1)) // support the NO_REWRITE override case
        }

      FunN(Seq(RefExpression(sumType), RefExpression(LongType)), exp, Some("results_With"))
    }
    register("results_With", ff2, Set(1, 2))

    val mapFX = (exps: Seq[Expression]) => exps.size match {
      case 3 =>
        // parse it for old sql to support backwards aggExpr
        MapTransform.create(RefExpression(parse(exps(0))), exps(1), exps(2), zero)
      case 2 =>
        // default to LongType, aggrExpr must fix, 2nd param is key, third the func manipulating the key
        MapTransform.create(RefExpression(MapType(LongType, LongType)), exps(0), exps(1), zero)
    }
    register("map_With", mapFX, Set(2, 3))

    def aggFWith(fun: String) = (what: String) => (exps: Seq[Expression]) => expression(
      if (exps.size == 0)
        functions.expr(s"$fun( $what )")
      else
        functions.expr(s"$fun('${strType(exps(0))}', $what )")
      )

    val retWith = aggFWith("results_With")

    // common cases
    register("meanF", retWith("(sum, count) -> sum / count"), Set(0, 1))

    val sumWith = aggFWith("sum_With")

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
        val SLambdaFunction(a: Add, Seq(sum: UnresolvedNamedLambdaVariable), hidden ) = expression(functions.expr("sumWith(sum -> sum + 1)")).children(0)
        import ShimUtils.{add => addf}
        // could be a cast around x or three attributes plusing each other or....
        FunN(Seq(RefExpression(LongType)),
          SLambdaFunction(addf(a.left, y, LongType), Seq(sum), hidden )
          , Some("inc")) // keep the type
      case Seq() => expression(functions.expr(s"sumWith(sum -> sum + 1)"))
    }
    register("inc", incX, Set(1, 0, 2))

    // return sum
    register("return_Sum", retWith("(sum, count) -> sum"), Set(0, 1))
    def getRandom(exp: Expression, pos: Int) = {
      val str = getString(exp, pos)
      RandomSource.valueOf(str)
    }

    // random generators
    val brf = (exps: Seq[Expression]) => {
      //numBytes: Int, randomSource: RandomSource, seed: Long constructor but needs to use random, seed, numbytes
      val (numBytes: Int, randomSource, seed: Long) =
        exps.size match {
          case 0 => (16, RandomSource.XO_RO_SHI_RO_128_PP, 0L)
          case 1 => (16, getRandom(exps(0), 0), 0L)
          case 2 => (16, getRandom(exps(0), 0), getLong(exps(1), 1))
          case 3 => (getLong(exps(2)).toInt, getRandom(exps(0), 0), getLong(exps(1), 1))
          case _ => literalsNeeded
        }

      RandomBytes(numBytes, randomSource, seed)
    }
    register("rng_Bytes", brf, Set(0,1,2,3))

    // random generators
    val lrf = (exps: Seq[Expression]) => {

      //randomSource: RandomSource, seed: Long constructor but needs to use random, seed, numbytes
      val (randomSource, seed: Long) =
        exps.size match {
          case 0 => (RandomSource.XO_RO_SHI_RO_128_PP, 0L)
          case 1 => (getRandom(exps(0), 0), 0L)
          case 2 => (getRandom(exps(0), 0), getLong(exps(1), 1))
          case _ => literalsNeeded
        }

      RandomLongs.create(randomSource, seed)
    }
    register("rng", lrf, Set(0,1,2))

    register("rng_UUID", exps =>
      RngUUIDExpression(exps.head),
      Set(1))

    register("long_Pair", exps => LongPairExpression(exps(0), exps(1)), Set(2))
    register("long_Pair_From_UUID", exps => UUIDToLongsExpression(exps.head), Set(1))

    register("small_Bloom", exps => ParquetAggregator(exps(0), exps(1), exps(2)), Set(3))

    register("big_Bloom", exps => exps.size match {
      case 4 =>
        BucketedArrayParquetAggregator(exps(0), exps(1), exps(2), exps(3))
      case 3 =>
        BucketedArrayParquetAggregator(exps(0), exps(1), exps(2), Literal(java.util.UUID.randomUUID().toString))
    }, Set(3,4))


    val longPairEqual = (exps: Seq[Expression]) => {
      val Seq(Literal(a, StringType), Literal(b, StringType)) = exps
      expression(long_pair_equal(a.toString, b.toString))
    }
    register("long_Pair_Equal", longPairEqual, Set(2))

    val idEqual = (exps: Seq[Expression]) => {
      val Seq(Literal(a, StringType), Literal(b, StringType)) = exps
      expression(id_equal(a.toString, b.toString))
    }
    register("idEqual", idEqual, Set(2))

    register("as_uuid", exps => AsUUID(exps(0), exps(1)), Set(2))

    register("rule_Suite_Result_Details", exps => impl.RuleSuiteResultDetailsExpr(exps(0)), Set(1))

    register("digest_To_Longs_Struct",  exps => expression(digest_to_longs_struct(getString(exps.head, 0), exps.tail.map(column(_)): _*)), minimum = 2)
    register("digest_To_Longs", exps => expression(digest_to_longs(getString(exps.head, 0), exps.tail.map(column(_)): _*)), minimum = 2)

    def fieldBasedID(func: (String, String, Seq[Column]) => Column) = (exps: Seq[Expression]) =>
      exps.size match {
        case a if a > 3 =>
          val digestImpl = getString(exps(1), 1)
          val prefix = getString(exps.head, 0)
          expression(func(prefix, digestImpl, exps.drop(2).map(column(_)) ))

        case _ => literalsNeeded
      }

    register("field_Based_ID", fieldBasedID(field_based_id _), minimum = 3)
    register("za_Longs_Field_Based_ID", fieldBasedID(za_longs_field_based_id _), minimum = 3)
    register("za_Field_Based_ID", fieldBasedID(za_field_based_id _), minimum = 3)
    register("hash_Field_Based_ID", fieldBasedID(hash_field_based_id _), minimum = 3)

    val providedID = (exps: Seq[Expression]) =>
      exps.size match {
        case 2 =>
          GenericLongBasedIDExpression(model.ProvidedID,
            exps(1), getString(exps.head, 0))

        case _ => literalsNeeded
      }

    register("provided_ID", providedID, Set(2))

    val prefixedToLongPair = (exps: Seq[Expression]) =>
      exps.size match {
        case 2 =>
          expression(prefixed_to_long_pair(column(exps(1)), getString(exps.head, 0)))

        case _ => literalsNeeded
      }
    register("prefixed_To_Long_Pair", prefixedToLongPair, Set(2))

    val rngID = (exps: Seq[Expression]) => {
      val (randomSource, seed: Long, prefix) =
        exps.size match {
          case 1 => ( RandomSource.XO_RO_SHI_RO_128_PP, 0L, getString(exps.head, 0))
          case 2 => ( getRandom(exps(1), 1), 0L,  getString(exps.head, 0))
          case 3 => ( getRandom(exps(1), 1), getLong(exps(2), 1),  getString(exps.head, 0))
          case _ => literalsNeeded
        }

      expression(rng_id(prefix, randomSource, seed))
    }
    register("rng_ID", rngID, Set(1,2,3))

    val uniqueID = (exps: Seq[Expression]) => {
      val (prefix) =
        exps.size match {
          case 1 => getString(exps.head, 0)
          case _ => literalsNeeded
        }

      expression(unique_id(prefix))
    }
    register("unique_ID", uniqueID, Set(1))

    register("id_size", exps => expression(id_size(column(exps.head))), Set(1))
    register("id_base64", exps => expression(id_base64(exps.map(column(_)): _*)), minimum = 1)
    register("id_from_base64", {
      case Seq(e) => expression(id_from_base64(column(e)))
      case Seq(e, s) => expression(id_from_base64(column(e), getInteger(s)))
    }, Set(1,2))
    register("id_raw_type", exps => expression(id_raw_type(column(exps.head))), Set(1))

    val Murmur3_128_64 = (exps: Seq[Expression]) => {
      val (prefix) =
        exps.size match {
          case a if a < 2 => literalsNeeded
          case _ => getString(exps.head, 0)
        }
      GenericLongBasedIDExpression(model.FieldBasedID,
        HashFunctionsExpression(exps.tail, "IGNORED", true, HashFunctionFactory("IGNORED")), prefix)
    }
    register("murmur3_ID", Murmur3_128_64, minimum = 2)

    register("hash_With", exps => expression(hash_with(getString(exps.head, 0), exps.tail.map(column(_)) :_*)), minimum = 2)
    register("hash_With_Struct", exps => expression(hash_with_struct(getString(exps.head, 0), exps.tail.map(column(_)) :_*)), minimum = 2)

    register("za_Hash_With", exps => expression(za_hash_with(getString(exps.head, 0), exps.tail.map(column(_)) :_*)), minimum = 2) // 64bit only, not a great id choice
    register("za_Hash_With_Struct", exps => expression(za_hash_with_struct(getString(exps.head,0 ), exps.tail.map(column(_)) :_*)), minimum = 2) // 64bit only, not a great id choice

    register("za_Hash_Longs_With", exps => expression(za_hash_longs_with(getString(exps.head, 0), exps.tail.map(column(_)) :_*)), minimum = 2)
    register("za_Hash_Longs_With_Struct", exps => expression(za_hash_longs_with_struct(getString(exps.head, 0), exps.tail.map(column(_)) :_*)), minimum = 2)

    // here to stop these functions being used and allow validation
    register("coalesce_If_Attributes_Missing", _ => qualityException("coalesceIf functions cannot be created") )
    register("coalesce_If_Attributes_Missing_Disable", _ => qualityException("coalesceIf functions cannot be created") )

    // 3.0.1 adds this #37 drops 3.0.0 and we can remove the c+p from 3.4.1 needed due to #36
    register("update_field", exps => {
      expression(update_field(column(exps.head), ( exps.tail.grouped(2).map(p => getString(p.head, 0) -> column(p.last)).toSeq): _*))
    }, minimum = 3)
    register("drop_field", exps => {
      expression(drop_field(column(exps.head), exps.tail.zipWithIndex.map{case (p, i) => getString(p, i+1)} : _*))
    }, minimum = 2)

    def msgAndExpr(msgDefault: String, exps: Seq[Expression]) = exps match {
      case Seq(Literal(str: UTF8String, StringType), e: Expression) =>
        (str.toString, e)
      case Seq(e: Expression) =>
        (msgDefault, e)
    }

    register("print_Code", (exps: Seq[Expression]) => {
      val (msg, exp) = msgAndExpr(PrintCode(exps(0)).msg, exps)
      PrintCode(exp, msg, writer)
    }, Set(1, 2))
    register("print_Expr", (exps: Seq[Expression]) => {
      val (msg, exp) = msgAndExpr("Expression toStr is ->", exps)
      writer(s"$msg $exp .  Sql is ${exp.sql}")
      exp
    }, Set(1,2))
  }

}
