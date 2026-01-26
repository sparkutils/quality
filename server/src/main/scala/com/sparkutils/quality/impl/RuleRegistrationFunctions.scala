package com.sparkutils.quality.impl

import com.sparkutils.quality.QualityException.qualityException
import com.sparkutils.quality.classicFunctions._
import com.sparkutils.quality.impl.CollectRunner.collectRunnerClassic
import com.sparkutils.quality.impl.ReWriteConstants.INC_REWRITE_GENEXP_ERR_MSG
import com.sparkutils.quality.impl.RuleSuiteHelpers.deserialize
import com.sparkutils.quality.impl.VariableProcessIfMissingFunctions.registerProcessIfAttributeMissingForAgnostic
import com.sparkutils.quality.impl.aggregates.AggregateExpressions
import com.sparkutils.quality.impl.bloom.{BucketedArrayParquetAggregator, ParquetAggregator}
import com.sparkutils.quality.impl.hash.{HashFunctionFactory, HashFunctionsExpression, MessageDigestFactory, ZALongHashFunctionFactory, ZALongTupleHashFunctionFactory}
import com.sparkutils.quality.impl.id.{AsBase64Fields, AsBase64Struct, GenericLongBasedIDExpression, GuaranteedUniqueID, GuaranteedUniqueIdIDExpression, IDFromBase64, IDToRawIDDataType, SizeOfIDString, model}
import com.sparkutils.quality.impl.longPair.{AsUUID, LongPairExpression, PrefixedToLongPair}
import com.sparkutils.quality.impl.mapLookup.MapLookupFunctionsImpl.registerMapLookupsForAgnostic
import com.sparkutils.quality.impl.rng.{RandLongsWithJump, RandomBytes, RandomLongs}
import com.sparkutils.quality.impl.util.{ComparableMapConverter, ComparableMapReverser, InputWrapper, PrintCode}
import com.sparkutils.quality.impl.yaml.{YamlDecoderExpr, YamlEncoderExpr}
import com.sparkutils.quality.{QualityException, impl}
import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.sql.ShimUtils.{add, column, expression}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, And, AttributeReference, CreateMap, EqualTo, Expression, Literal, UnresolvedNamedLambdaVariable, LambdaFunction => SLambdaFunction}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.qualityFunctions.LambdaFunctions.processTopCallFun
import org.apache.spark.sql.qualityFunctions._
import org.apache.spark.sql.shim.hash.DigestFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ClassicQualitySparkUtils, ShimUtils, SparkSession, functions}
import org.apache.spark.unsafe.types.UTF8String

object RuleRegistrationFunctions {

  protected[quality] def literalsNeeded(pos: Int, typ: String = "String"): Nothing =
    if (pos == -1)
      qualityException("Cannot setup Quality Expression with non-literals")
    else
      qualityException(s"Quality Expression requires a $typ literal in (starts with position 0) position $pos")

  protected[quality] def literalsNeeded: Nothing = literalsNeeded( -1 )

  protected[quality] def getLong(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(seed: Long, LongType) => seed
      case _ => literalsNeeded(pos, "Long")
    }
  protected[quality] def getInteger(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(seed: Int, IntegerType) => seed
      case _ => literalsNeeded(pos, "Integer")
    }
  protected[quality] def getBoolean(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(seed: Boolean, BooleanType) => seed
      case _ => literalsNeeded(pos, "Boolean")
    }
  protected[quality] def getString(exp: Expression, pos: Int = -1) =
    exp match {
      case Literal(str: UTF8String, StringType) => str.toString()
      case _ => literalsNeeded(pos)
    }

  protected[quality] def getBinary(exp: Expression, pos: Int = -1): Array[Byte] =
    exp match {
      case Literal(ar: Array[Byte], _: BinaryType) => ar
      case _ => literalsNeeded(pos, "Binary")
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
            throw QualityException(s"Wrong number of arguments provided to Quality function $name, received ${exps.size}. $sizeerr")
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
  def registerQualityFunctions(parseTypes: String => Option[DataType] = defaultParseTypes,
                               zero: DataType => Option[Any] = defaultZero,
                               add: DataType => Option[(Expression, Expression) => Expression] = (dataType: DataType) => defaultAdd(dataType),
                               mapCompare: DataType => Option[(Any, Any) => Int] = (dataType: DataType) => utils.defaultMapCompare(dataType),
                               writer: String => Unit = println,
                               registerFunction: (String, Seq[Expression] => Expression) => Unit =
                                (n, f) => ShimUtils.registerFunction(SparkSession.active)(n,f)
                              ) : Unit = {

    def register(name: String, argsf: Seq[Expression] => Expression, paramNumbers: Set[Int] = Set.empty, minimum: Int = -1) =
      registerWithChecks(registerFunction, name, argsf, paramNumbers, minimum)

    def parse(exp: Expression) = {
      val Literal(str: UTF8String, StringType) = exp // only accept type as string
      parseTypes(str.toString).getOrElse(qualityException(s"Could not parse the type $str"))
    }

    def getMap(exp: Expression, pos: Int = -1) = exp match {
      case l: Literal if l.dataType.isInstanceOf[MapType] =>
        MapUtils.toScalaMap(l.value.asInstanceOf[ArrayBasedMapData], StringType, StringType).map(p => (p._1.toString, p._2.toString))
      case c: CreateMap if c.children.grouped(2).forall{
        case Seq(Literal(_: UTF8String, StringType), _: Literal) =>
          true
        case _ => false
      } =>
        c.children.grouped(2).map{
          case Seq(Literal(str: UTF8String, StringType), value: Literal) =>
            str.toString -> value.value.toString()
        }.toMap
      case _ => throw QualityException(s"Could not process a literal map with expression $exp index $pos")
    }

    register("processor_input_wrapper", exps => InputWrapper(exps.head, exps.last), minimum = 2)

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

    register("passed", _ => com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.PassedExpr, Set(0))
    register("failed", _ => com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.FailedExpr, Set(0))
    register("soft_Failed", _ => com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.SoftFailedExpr, Set(0))
    register("disabled_Rule", _ => com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.DisabledRuleExpr, Set(0))
    register("ignored_rule", _ => com.sparkutils.quality.impl.imports.ClassicRuleResultsImports.IgnoredRuleExpr, Set(0))

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
      case (fun@ FunN(_, l@ SLambdaFunction(ff : FunForward, _, _), _, _, _, _)) +: args =>
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
                case FunN(Seq(RefExpression(dataType, _, _)), _, _, _, _, _) => dataType // would default to long anyway
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
      FunN(Seq(RefExpression(dataType)), origExp, Some("sum_With"), usedAsLambda = true)
    }, Set(1, 2))

    val ff2 = (exps: Seq[Expression]) => {
      // real type for param1 is changed by aggrExpr, but last works for all compat as well
      val (sumType, exp) =
        exps.size match {
          case 1 => (LongType, exps(0))
          case 2 => (parse(exps(0)), exps(1)) // support the NO_REWRITE override case
        }

      FunN(Seq(RefExpression(sumType), RefExpression(LongType)), exp, Some("results_With"), usedAsLambda = true)
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
          , Some("inc"), usedAsLambda = true) // keep the type
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
          case 3 => (getInteger(exps(2), 2), getRandom(exps(0), 0), getLong(exps(1), 1))
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

      def lower(a: Any) = UnresolvedAttribute(s"${a}_lower")

      def higher(a: Any) = UnresolvedAttribute(s"${a}_higher")

      And(EqualTo(lower(a), lower(b)), EqualTo(higher(a), higher(b)))
    }
    register("long_Pair_Equal", longPairEqual, Set(2))

    val idEqual = (exps: Seq[Expression]) => {
      val Seq(Literal(a, StringType), Literal(b, StringType)) = exps
      def attr(a: Any, field: String) = UnresolvedAttribute(s"${a}_$field")

      And(And(EqualTo(attr(a, "base"), attr(b, "base")),
        EqualTo(attr(a, "i0"), attr(b, "i0"))),
        EqualTo(attr(a, "i1"), attr(b, "i1")))
    }
    register("id_equal", idEqual, Set(2))

    register("as_uuid", exps => AsUUID(exps(0), exps(1)), Set(2))

    register("rule_Suite_Result_Details", exps => impl.RuleSuiteResultDetailsExpr(exps(0)), Set(1))

    def fieldsToHash(asStruct: Boolean, factory: String => DigestFactory) = (exps: Seq[Expression]) =>
      exps.size match {
        case a if a > 3 =>
          val digestImpl = getString(exps(0), 0)

          HashFunctionsExpression(exps.drop(1), digestImpl, asStruct, factory(digestImpl))

        case _ => literalsNeeded
      }

    register("digest_To_Longs_Struct", fieldsToHash(true, MessageDigestFactory), minimum = 2)
    register("digest_To_Longs",  fieldsToHash(false, MessageDigestFactory), minimum = 2)

    def fieldBasedID(factory: String => DigestFactory) = (exps: Seq[Expression]) =>
      exps.size match {
        case a if a > 3 =>
          val digestImpl = getString(exps(1), 1)
          val prefix = getString(exps.head, 0)

          GenericLongBasedIDExpression(model.FieldBasedID,
            HashFunctionsExpression(exps.drop(2), digestImpl, true, factory(digestImpl)), prefix)

        case _ => literalsNeeded
      }

    register("field_Based_ID", fieldBasedID(MessageDigestFactory), minimum = 3)
    register("za_Longs_Field_Based_ID", fieldBasedID(ZALongTupleHashFunctionFactory), minimum = 3)
    register("za_Field_Based_ID", fieldBasedID(ZALongHashFunctionFactory), minimum = 3)
    register("hash_Field_Based_ID", fieldBasedID(HashFunctionFactory(_)), minimum = 3)

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
          PrefixedToLongPair(exps(1), getString(exps.head, 0))

        case _ => literalsNeeded
      }
    register("prefixed_To_Long_Pair", prefixedToLongPair, Set(2))

    val rngID = (exps: Seq[Expression]) => {
      val (randomSource, seed: Long, prefix) =
        exps.size match {
          case 1 => ( RandomSource.XO_RO_SHI_RO_128_PP, 0L, getString(exps.head, 0))
          case 2 => ( getRandom(exps(1), 1), 0L,  getString(exps.head, 0))
          case 3 => ( getRandom(exps(1), 1), getLong(exps(2), 2),  getString(exps.head, 0))
          case _ => literalsNeeded
        }

      GenericLongBasedIDExpression(model.RandomID,
        RandLongsWithJump(seed, randomSource), prefix)
    }
    register("rng_ID", rngID, Set(1,2,3))

    val uniqueID = (exps: Seq[Expression]) => {
      val (prefix) =
        exps.size match {
          case 1 => getString(exps.head, 0)
          case _ => literalsNeeded
        }

      GuaranteedUniqueIdIDExpression(
        GuaranteedUniqueID() // defaults are all fine, ms just relates to definition instead of action
        , prefix
      )
    }
    register("unique_ID", uniqueID, Set(1))

    register("id_size", exps => SizeOfIDString(exps.head), Set(1))
    register("id_base64", exps => exps match {
      case Seq(e) => AsBase64Struct(e)
      case _ => AsBase64Fields(exps)
    }, minimum = 1)
    register("id_from_base64", {
      case Seq(e) => IDFromBase64(e, 2)
      case Seq(e, s) => IDFromBase64(e, getInteger(s))
    }, Set(1,2))
    register("id_raw_type", exps => IDToRawIDDataType(exps.head), Set(1))

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

    register("hash_With", fieldsToHash(false, HashFunctionFactory(_)), minimum = 2)
    register("hash_With_Struct", fieldsToHash(true, HashFunctionFactory(_)), minimum = 2)

    register("za_Hash_With", fieldsToHash(false, ZALongHashFunctionFactory(_)), minimum = 2) // 64bit only, not a great id choice
    register("za_Hash_With_Struct", fieldsToHash(true, ZALongHashFunctionFactory(_)), minimum = 2) // 64bit only, not a great id choice

    register("za_Hash_Longs_With", fieldsToHash(false, ZALongTupleHashFunctionFactory(_)), minimum = 2)
    register("za_Hash_Longs_With_Struct", fieldsToHash(true, ZALongTupleHashFunctionFactory(_)), minimum = 2)

    // here to stop these functions being used and allow validation
    register("coalesce_If_Attributes_Missing", _ => qualityException("coalesceIf functions cannot be created") )
    register("coalesce_If_Attributes_Missing_Disable", _ => qualityException("coalesceIf functions cannot be created") )

    // 3.0.1 adds this #37 drops 3.0.0 and we can remove the c+p from 3.4.1 needed due to #36
    register("update_field", exps => {
      expression(ClassicQualitySparkUtils.update_field(column(exps.head), ( exps.tail.grouped(2).map(p => getString(p.head, 0) -> column(p.last)).toSeq): _*))
    }, minimum = 3)
    register("drop_field", exps => {
      expression(ClassicQualitySparkUtils.drop_field(column(exps.head), exps.tail.zipWithIndex.map{case (p, i) => getString(p, i+1)} : _*))
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

    // additional functions for agg_expr and FunN, purposefully not documented
    register("qualityrefexpression", {
      case Seq(e) => RefExpression(parse(e))
      case _ => literalsNeeded
    }, Set(1))
    register("qualityfunn", exps => {
      val args = exps.dropRight(4)
      val params = exps.drop(args.length)
      val (function, name, attemptCodeGen, useAsLambda) =
        params match {
          case Seq(f, n, a, u) => (f, {
            val s = getString(n)
            if (s.isEmpty)
              None
            else
              Some(s)
          }, getBoolean(a), getBoolean(u))
        }
      FunN(args, function, name, attemptCodeGen = attemptCodeGen, usedAsLambda = useAsLambda)
    }, minimum = 5)

    registerMapLookupsForAgnostic(registerFunction)

    // actual runners
    register("dq_rule_runner", {
      case Seq(OfRuleSuite(rs)) =>
        expression(RuleRunnerImpl.ruleRunnerImplClassic(rs))
      case Seq(OfRuleSuite(rs), varPer, varG) =>
        expression(RuleRunnerImpl.ruleRunnerImplClassic(rs, false, None,
          variablesPerFunc = getInteger(varPer, 2), variableFuncGroup = getInteger(varG, 3)))
    }, Set(1, 3))

    register("typed_expression_runner", {
      case Seq(OfRuleSuite(rs), ddl) =>
        expression(ExpressionRunner(rs, ddlType = getString(ddl, 1)))
      case Seq(OfRuleSuite(rs), ddl, name) =>
        expression(ExpressionRunner(rs, ddlType = getString(ddl, 1), name = getString(name, 2)))
      case Seq(OfRuleSuite(rs), ddl, name, varPer, varG) =>
        expression(ExpressionRunner(rs, ddlType = getString(ddl, 1), name = getString(name, 2),
          variablesPerFunc = getInteger(varPer, 3), variableFuncGroup = getInteger(varG, 4)))
    }, Set(2, 3, 5))

    register("expression_runner", {
      case Seq(OfRuleSuite(rs)) =>
        expression(ExpressionRunner(rs))
      case Seq(OfRuleSuite(rs), name) =>
        expression(ExpressionRunner(rs, name = getString(name, 1)))
      case Seq(OfRuleSuite(rs), name, options) =>
        expression(ExpressionRunner(rs, name = getString(name, 1), renderOptions = getMap(options, 2)))
      case Seq(OfRuleSuite(rs), name, options, varp, varg) =>
        expression(ExpressionRunner(rs, name = getString(name, 1), renderOptions = getMap(options, 2),
          variablesPerFunc = getInteger(varp, 3), variableFuncGroup = getInteger(varg, 4)))
    }, Set(1, 2, 3, 5))

    register("rule_engine_runner", {
      case Seq(OfRuleOutputSuite(rs)) =>
        expression(RuleEngineRunnerImpl.ruleEngineRunnerImpl(rs, None))
      case Seq(OfRuleOutputSuite(rs), dt) =>
        expression(RuleEngineRunnerImpl.ruleEngineRunnerImpl(rs, defaultParseTypes(getString(dt, 1))))
      case Seq(OfRuleOutputSuite(rs), dt, debug) =>
        expression(RuleEngineRunnerImpl.ruleEngineRunnerImpl(rs, defaultParseTypes(getString(dt, 1)),
          debugMode = getBoolean(debug, 2)))
      case Seq(OfRuleOutputSuite(rs), dt, debug, varp, varg) =>
        expression(RuleEngineRunnerImpl.ruleEngineRunnerImpl(rs, defaultParseTypes(getString(dt, 1)),
          debugMode = getBoolean(debug, 2), variablesPerFunc = getInteger(varp, 3),
          variableFuncGroup = getInteger(varg, 4)
        ))
    }, Set(1, 2, 3, 5))

    register("rule_folder_runner", {
      case Seq(OfRuleOutputSuite(rs), starter) =>
        expression(com.sparkutils.quality.classicFunctions.ruleFolderRunnerClassic(rs, column(starter)))
      case Seq(OfRuleOutputSuite(rs), starter, dt) =>
        expression(ruleFolderRunnerClassic(rs, column(starter),
          useType = defaultParseTypes(getString(dt, 2)).map(_.asInstanceOf[StructType])))
      case Seq(OfRuleOutputSuite(rs), starter, dt, debug) =>
        expression(ruleFolderRunnerClassic(rs, column(starter),
          debugMode = getBoolean(debug, 3), useType = defaultParseTypes(getString(dt, 2)).map(_.asInstanceOf[StructType])))
      case Seq(OfRuleOutputSuite(rs), starter, dt, debug, varp, varg) =>
        expression(ruleFolderRunnerClassic(rs, column(starter),
          debugMode = getBoolean(debug, 3), variablesPerFunc = getInteger(varp, 4),
          variableFuncGroup = getInteger(varg, 5),
          useType = defaultParseTypes(getString(dt, 2)).map(_.asInstanceOf[StructType])
        ))
    }, Set(2, 3, 4, 6))

    register("collect_runner", {
      case Seq(OfRuleOutputSuite(rs)) =>
        expression(collectRunnerClassic(rs, None))
      case Seq(OfRuleOutputSuite(rs), dt) =>
        expression(collectRunnerClassic(rs, defaultParseTypes(getString(dt, 1))))
      case Seq(OfRuleOutputSuite(rs), dt, flatten) =>
        expression(collectRunnerClassic(rs, defaultParseTypes(getString(dt, 1)), flatten = getBoolean(flatten, 2)))
      case Seq(OfRuleOutputSuite(rs), dt, flatten, includeNulls) =>
        expression(collectRunnerClassic(rs, defaultParseTypes(getString(dt, 1)),
          flatten = getBoolean(flatten, 2), includeNulls = getBoolean(includeNulls, 3)))
      case Seq(OfRuleOutputSuite(rs), dt, flatten, includeNulls, varp, varg) =>
        expression(collectRunnerClassic(rs, defaultParseTypes(getString(dt, 1)),
          flatten = getBoolean(flatten, 2), includeNulls = getBoolean(includeNulls, 3),
          variablesPerFunc = getInteger(varp, 4), variableFuncGroup = getInteger(varg, 5)
        ))
    }, Set(1, 2, 3, 4, 6))

    // coalesce support
    registerProcessIfAttributeMissingForAgnostic(registerFunction)
  }

}

/**
 * Safe to use with connect
 */
object ReWriteConstants {

  val INC_REWRITE_GENEXP_ERR_MSG: String = "inc('DDL', generic expression) is not supported in NO_REWRITE mode, use inc(generic expression) without NO_REWRITE mode enabled"

}
