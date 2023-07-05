package com.sparkutils.quality.impl

import com.sparkutils.quality.{Id, impl}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, NamedExpression, UnresolvedNamedLambdaVariable, LambdaFunction => SparkLambdaFunction}
import org.slf4j.LoggerFactory

// Used to pull in |+| to deep merge the maps as SemiGroups - https://typelevel.org/cats/typeclasses/semigroup.html#example-usage-merging-maps
import cats.implicits._

/**
 * For a given expression it breaks down information useful for documentation and validation for a non-resolved expression.
 *
 * All names may include optional scope (e.g. database)
 *
 * @param attributesUsed Which attributes are used in the expression
 * @param unknownSparkFunctions Which functions are used but are neither known lambdas nor registered spark expressions
 * @param lambdas Which known lambdas are used
 * @param sparkFunctions Which known spark functions are used
 */
case class ExpressionLookup(attributesUsed: impl.VariablesLookup.Identifiers = Set.empty, unknownSparkFunctions: impl.VariablesLookup.Identifiers = Set.empty, lambdas: Set[Id] = Set.empty, sparkFunctions: Set[String] = Set.empty)

/**
 * Provides a variable lookup function, after using the sql parser it will return all the fields used in an expression,
 * allowing sanity checks on rules to use only expected fields but also to attribute how much a rule does - does it check
 * just one field or use 20 of them.
 * It is also used to identify fields which are note provided by a lambda i.e. the ones bound at use.
 * Note: this cannot process nested lambdas in a simple expression unless the lambdas are also passed in, so process lambdas first.
 */
object VariablesLookup {

  val logger = LoggerFactory.getLogger("VariablesLookup")

  type Identifier = String
  type Identifiers = Set[Identifier]
  type ProcessedLambdas = Map[String, Map[Id, Identifiers]]
  type PossibleOverflowIds = Set[Id]
  type UnknownSparkFunctions = Map[Id, Set[String]]

  def toName(ne: NamedExpression): String =
    ne match {
      case nv: UnresolvedNamedLambdaVariable => toName(nv)
      case _ => toName(ne.qualifier :+ ne.name)
    }
  def toName(nv: UnresolvedNamedLambdaVariable): String =
    toName(nv.nameParts)
  def toName(parts: Seq[String]): String =
    parts.mkString(".")
  def toName(unresolvedFunction: UnresolvedFunction): String =
    unresolvedFunction.name.funcName

  def processLambdas(m: Map[String, Map[Id,Expression]]): (ProcessedLambdas, PossibleOverflowIds, UnknownSparkFunctions) =
    m.foldLeft((Map.empty[String, Map[Id, Identifiers]], Set.empty[Id], Map.empty[Id, Set[String]])){ (acc, p) =>
      if (acc._1.contains(p._1))
        acc
      else {
        val (macc, s, us) = acc
        val (res, ress, resus) = fieldsFromLambda( p._1, p._2, macc, m)
        (macc |+| res, ress |+| s, resus |+| us)
      }
    }

  def fieldsFromLambda(name: String, exprMap: Map[Id, Expression], m: ProcessedLambdas, lambdaExpressions: Map[String, Map[Id, Expression]]): (ProcessedLambdas, PossibleOverflowIds, UnknownSparkFunctions) = {
    // allow communication across tree depths
    val evaluatedLambdas = scala.collection.mutable.Map.empty[String, Map[Id, Identifiers]] ++ m
    val overflowIds = scala.collection.mutable.Set.empty[Id]
    val unknownSparkFunctionIds = scala.collection.mutable.Map.empty[Id, Set[String]]

    def children(res: Map[Id, Identifiers], children: Seq[(Id, Expression)], parent: UnresolvedFunction): Map[Id, Identifiers] =
      children.foldLeft(res){
        (curRes, exp) =>
          curRes |+| accumulate(curRes, exp, parent)
      }

    def processFields(args: Set[String], expr: Expression, id: Id, parent: UnresolvedFunction, ids: Identifiers = Set.empty): Identifiers = {
      def fieldChildren(res: Identifiers, children: Seq[Expression]): Identifiers =
        children.foldLeft(res) {
          (curRes, exp) =>
            faccumulate(curRes, exp)
        }

      def faccumulate(identifiers: Identifiers, expression: Expression): Identifiers =
        expression match {
          case a : UnresolvedNamedLambdaVariable =>
            val full = toName(a)
            if (!args.contains(full)) // don't accept args, so we should only be left with outer scopes, which may be from a nested..
              identifiers + full
            else
              identifiers

          case f @ UnresolvedFunction(functionIdentifier, argumentExpressions, _, _) => // nested....
            val nids =
              if (evaluatedLambdas.contains(functionIdentifier.identifier))
                // does it contain the same id's?
                identifiers
              else {
                if (lambdaExpressions.contains(functionIdentifier.identifier)) {
                  // we haven't yet evaluated it, pass back up to the top and recurse down
                  if ((parent ne null) && functionIdentifier.identifier == parent.name.identifier) {
                    // special case for recursion on the same identifier - are we calling the same id?
                    // get the exact arity matching
                    lambdaExpressions(functionIdentifier.identifier).find(_._2.children.size == argumentExpressions.size).fold{
                      overflowIds += id
                      logger.warn(s"Function ${functionIdentifier.identifier} calls itself, this may StackOverflowError on evaluation")
                    }{
                      i =>
                        val r = children(Map.empty, Seq(i), f)
                        evaluatedLambdas(functionIdentifier.identifier) = r
                    }
                  } else {
                    val r = children(Map.empty, lambdaExpressions(functionIdentifier.identifier).toSeq, f)
                    evaluatedLambdas(functionIdentifier.identifier) = r
                  }
                  identifiers
                } else {
                  // it's not a lambda function we know, is it inbuilt?
                  // NB you would have to register UDFs etc. before calling validate etc.
                  val exists =
                    functionIdentifier.database.fold(
                      SparkSession.active.catalog.functionExists(functionIdentifier.identifier)
                    )(d =>
                      SparkSession.active.catalog.functionExists(d, functionIdentifier.identifier)
                    )

                  if (!exists) {
                    // add it in to the unknowns list
                    val map = unknownSparkFunctionIds.getOrElse(id, Set.empty)
                    unknownSparkFunctionIds(id) = map + functionIdentifier.identifier
                  }
                  identifiers
                }
              }
            // params may be including nested children
            fieldChildren(nids, argumentExpressions)
          case p : Expression => fieldChildren(identifiers, p.children)
          case _  => identifiers
        }

      faccumulate(ids, expr)
    }

    def accumulate(res: Map[Id, Identifiers], exp: (Id, Expression), parent: UnresolvedFunction): Map[Id, Identifiers] =
      exp match {
        // unresolved case where we cannot see more unresolved functions
        case (id, SparkLambdaFunction(functionExpr, arguments, _)) =>
          // remove the arguments from any unresolved bound variables
          val names = arguments.map(v => toName(v.asInstanceOf[UnresolvedNamedLambdaVariable])).toSet
          // parse the functionExpr with the names
          Map( id -> processFields(names, functionExpr, id, parent)) //TODO is this parent?
        case (id, a : UnresolvedAttribute) => // not as part of a lambda
          val s = res.getOrElse(id, Set.empty)
          res + ( id -> (s + toName(a.nameParts)) )
        case (id, _ : LeafExpression) => res
        case (id, parent: UnresolvedFunction) => res |+| children(res, parent.children.map((id,_)), parent) // override
        case (id, newparent: Expression) => res |+| children(res, newparent.children.map((id,_)), parent)
      }

    val ids = children(Map.empty, exprMap.toSeq, null)

    (( m + (name -> ids) ) ++ evaluatedLambdas, Set() ++ overflowIds, Map() ++ unknownSparkFunctionIds)
  }

  /**
   * Identifies all variables from an expression tree, attempts to drill down into lambdas if the name is already known.
   * @param expr The root expression to be evaluated
   * @param knownLambdaLookups using a map of lambda functions to already identified late bind fields calls to this lambda will be expanded
   * @return
   */
  def fieldsFromExpression(expr: Expression, knownLambdaLookups: ProcessedLambdas = Map.empty): impl.ExpressionLookup = {
    def children(res: impl.ExpressionLookup, children: Seq[Expression]): impl.ExpressionLookup =
      children.foldLeft(res){
        (curRes, exp) =>
          accumulate(curRes, exp)
      }

    def accumulate(res: impl.ExpressionLookup, exp: Expression): impl.ExpressionLookup =
      exp match {
        // unresolved case where we cannot see more unresolved functions
        case UnresolvedFunction(name, arguments, _, _) =>
          val r =
            if (knownLambdaLookups.contains(name.identifier)) {
              val lambdas = knownLambdaLookups(name.identifier)
              res.copy(attributesUsed = res.attributesUsed ++ lambdas.flatMap(_._2), lambdas = res.lambdas ++ lambdas.keySet)  // merge the identifier set and lambdas
            } else {
              // it's not a lambda function we know, is it inbuilt?
              // NB you would have to register UDFs etc. before calling validate etc.
              val exists =
                name.database.fold(
                  SparkSession.active.catalog.functionExists(name.identifier)
                )(d =>
                  SparkSession.active.catalog.functionExists(d, name.identifier)
                )

              if (!exists)
                // add it in to the unknowns list
                res.copy(unknownSparkFunctions = res.unknownSparkFunctions + name.identifier)
              else
                res.copy(sparkFunctions = res.sparkFunctions + name.identifier)
            }

          // we still need to do the args
          children(r, arguments)
        case a : UnresolvedAttribute =>
          res.copy(attributesUsed = res.attributesUsed + a.name)
        // typically handled by the lambda functions above, but for coalesce this doesn't work, we need sub expression handling
        case a : UnresolvedNamedLambdaVariable =>
          res.copy(attributesUsed = res.attributesUsed + a.name)
        case _ : LeafExpression => res
        case parent: Expression => children(res, parent.children)
      }

    val ids = accumulate(impl.ExpressionLookup(), expr)

    ids
  }
}
