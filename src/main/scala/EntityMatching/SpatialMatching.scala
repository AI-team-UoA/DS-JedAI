package EntityMatching

import Blocking.BlockUtils
import DataStructures.{Block, SpatialEntity, TBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.Constants

import scala.reflect.ClassTag

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

// TODO meridian case
// TODO: append ids or originalIDs
/**
 *  Link discovery
 */
case class SpatialMatching(totalBlocks: Long, weightingStrategy: String = Constants.NO_USE) extends MatchingTrait {

	/**
	 * First calculate the allowed comparisons of each block. Then test which comparison
	 * might result to a match by comparing their MBB, and then perform the actual comparisons.
	 *
	 */
	def apply(blocks: RDD[Block], relation: String, cleaningStrategy: String = Constants.RANDOM): RDD[(Int, Int)] ={
		val allowedComparisons = BlockUtils.cleanBlocks2(blocks.asInstanceOf[RDD[TBlock]])
		val blocksComparisons = blocks.map(b => (b.id, b))

		allowedComparisons
			.leftOuterJoin(blocksComparisons)
			.flatMap { b =>
				val allowedComparisons = b._2._1
				b._2._2.get
					.getComparisons
					.filter(c => allowedComparisons.contains(c.id))
			}
    		.filter(c => testMBB(c.entity1.mbb, c.entity2.mbb, relation))
    		.filter(c => relate(c.entity1.geometry, c.entity2.geometry, relation))
    		.map(c => (c.entity1.id, c.entity2.id))
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
