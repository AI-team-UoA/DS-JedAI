//package utils
//
//import net.sansa_stack.query.spark.sparqlify.{QueryExecutionSpark, SparqlifyUtils3}
//import net.sansa_stack.rdf.common.partition.core.{RdfPartition, RdfPartitioner, RdfPartitionerDefault}
//import net.sansa_stack.rdf.spark.partition.core.RdfPartitionUtilsSpark
//import org.apache.jena.query.{ARQ, QueryFactory}
//import org.apache.jena.sparql.core.Var
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.functions._
//
//import scala.reflect.ClassTag
//import org.apache.jena.graph.Triple
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//
//import scala.collection.JavaConversions._
//import scala.collection.JavaConversions.asScalaSet
//import scala.collection.mutable
//
//
//object SparqlExecutor {
//
//    //Renames columns to their expected names
//    def renameDfCols(mappings: Array[(String, mutable.Set[Var])], df: DataFrame): DataFrame = {
//        mappings.foldLeft(df) { (memoDf: DataFrame, colMapping: (String, mutable.Set[Var])) => concatColumns(colMapping, memoDf) }
//    }
//
//    //Merges associated columns
//    def concatColumns(mapping: (String, mutable.Set[Var]), df: DataFrame): DataFrame = {
//        val colArray = for {colName <- mapping._2} yield col(colName.getName)
//        val colNameArray = for {colName <- mapping._2} yield colName.getName
//        val colSeq = colArray.toSeq
//        val colNameSeq = colNameArray.toSeq
//
//        df.withColumn(mapping._1, concat(colSeq: _*))
//            .drop(colNameSeq: _*)
//    }
//
//    def query(spark: SparkSession, triples: RDD[Triple], sparqlQuery: String): DataFrame = {
//
//        val partitions = RdfPartitionUtilsSpark.partitionGraph(triples)
//        val rewriter = SparqlifyUtils3.createSparqlSqlRewriter(spark, partitions)
//        val query = QueryFactory.create(sparqlQuery)
//        val rewrite = rewriter.rewrite(query)
//
//        //This maps the column names specified in the sparql select to the sparqlify generated column name(s)
//        //Example output (?YourColumnName, (h_1, h_3))
//        val mappings = for {
//            colKey <- rewrite.getVarDefinition.getMap.keys.toArray
//            restrictedExpression <- rewrite.getVarDefinition.getMap.get(colKey.asInstanceOf[Var])
//        } yield (colKey.asInstanceOf[Var].getName, asScalaSet(restrictedExpression.getExpr.getVarsMentioned))
//
//        val df = QueryExecutionSpark.createQueryExecution(spark, rewrite, query)
//
//        val renamedDf = renameDfCols(mappings, df)
//        renamedDf
//    }
//
//}