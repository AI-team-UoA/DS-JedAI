package EntityMatching.BlockBasedAlgorithms

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.Constants.Relation.Relation

import scala.reflect.ClassTag

/**
 *  Link discovery
 */
case class BlockMatching(blocks:RDD[Block]) extends BlockMatchingTrait {


	/**
	 * Perform the only the necessary comparisons
	 * @return the original ids of the matches
	 */
	def apply(relation: Relation): RDD[(String, String)] ={
		blocks.flatMap(b => b.getFilteredComparisons(relation))
			.filter(c => c.entity1.relate(c.entity2, relation))
			.map(c => (c.entity1.originalID, c.entity2.originalID))
	}

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[Int, String](implicit e1: Encoder[Int], e2: Encoder[String]): Encoder[(Int,String)] = Encoders.tuple[Int,String](e1, e2)
	def disjointMatches(source: RDD[SpatialEntity], target: RDD[SpatialEntity]): RDD[(Int,Int)] ={
		val spark = SparkSession.builder().getOrCreate()
		GeoSparkSQLRegistrator.registerAll(spark)
		val disjointQuery =
			"""SELECT SOURCE._1 AS SOURCE_ID, TARGET._1 AS TARGET_ID
			  | FROM SOURCE, TARGET
			  | WHERE NOT ST_Intersects( ST_GeomFromWKT(SOURCE._2),  ST_GeomFromWKT(TARGET._2))""".stripMargin

		//import spark.implicits._
		//spark.catalog.listFunctions.filter('name like "ST_%").show(200, false)
		val sourceDT = spark.createDataset(source.map(se => (se.id, se.geometry.toText)))
		sourceDT.createOrReplaceTempView("SOURCE")

		val targetDT = spark.createDataset(target.map(se => (se.id, se.geometry.toText)))
		targetDT.createOrReplaceTempView("TARGET")

		val disjointResults = spark.sql(disjointQuery)
		disjointResults.rdd.map(r => (r.get(0).asInstanceOf[Int], r.get(1).asInstanceOf[Int]))
	}

}
