package EntityMatching

import Blocking.BlockUtils
import DataStructures.{Block, LightBlock, MBB, SpatialEntity, TBlock}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.Constants
import utils.Utils.spark

import scala.reflect.ClassTag

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

// TODO meridian case
// TODO: append ids or originalIDs
/**
 *  Link discovery
 */
object Matching {

	/**
	 * check the relation between two geometries
	 *
	 * @param sourceGeom geometry from source set
	 * @param targetGeometry geometry from target set
	 * @param relation requested relation
	 * @return whether the relation is true
	 */
	def relate(sourceGeom: Geometry, targetGeometry: Geometry, relation: String): Boolean ={
		relation match {
			case Constants.CONTAINS => sourceGeom.contains(targetGeometry)
			case Constants.INTERSECTS => sourceGeom.intersects(targetGeometry)
			case Constants.CROSSES => sourceGeom.crosses(targetGeometry)
			case Constants.COVERS => sourceGeom.covers(targetGeometry)
			case Constants.COVEREDBY => sourceGeom.coveredBy(targetGeometry)
			case Constants.OVERLAPS => sourceGeom.overlaps(targetGeometry)
			case Constants.TOUCHES => sourceGeom.touches(targetGeometry)
			case Constants.DISJOINT => sourceGeom.disjoint(targetGeometry)
			case Constants.EQUALS => sourceGeom.equals(targetGeometry)
			case Constants.WITHIN => sourceGeom.within(targetGeometry)
			case _ => false
		}
	}

	/**
	 *  check relation among MBBs
	 *
	 * @param s MBB from source
	 * @param t MBB form target
	 * @param relation requested relation
	 * @return whether the relation is true
	 */
	def testMBB(s:MBB, t:MBB, relation: String): Boolean ={
		relation match {
			case Constants.CONTAINS | Constants.COVERS =>
				s.contains(t)
			case Constants.WITHIN | Constants.COVEREDBY =>
				s.within(t)
			case Constants.INTERSECTS | Constants.CROSSES | Constants.OVERLAPS =>
				s.intersects(t)
			case Constants.TOUCHES => s.touches(t)
			case Constants.DISJOINT => s.disjoint(t)
			case Constants.EQUALS => s.equals(t)
			case _ => false
		}
	}


	/**
	 * First calculate the allowed comparisons of each block. Then test which comparison
	 * might result to a match by comparing their MBB, and then perform the actual comparisons.
	 *
	 * @param blocks RDD of blocks
 	 * @param relation requested relation
	 * @return the matches
	 */
	def SpatialMatching(blocks: RDD[Block], relation: String): RDD[(Long, Long)] ={

		val allowedComparisons = BlockUtils.cleanBlocks(blocks.asInstanceOf[RDD[TBlock]])
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


	/**
	 * Similar to matching but using light blocks, and the smallest set is sorted and then broadcasted.
	 *
	 * @param blocks an RDD of LightBlocks
	 * @param toCollect the set that it will be collected and broadcasted
	 * @param startIdFrom the number that the ids of the toCollect set starts from
	 * @param relation requested relation
	 * @param swapped if the source, target was swapped in previous step
	 * @return the matches
	 */
	def lightMatching(blocks: RDD[LightBlock], toCollect: RDD[SpatialEntity], startIdFrom: Int, relation: String, swapped:Boolean): RDD[(Long, Long)] = {
		val collectedSet: Array[SpatialEntity] = toCollect.sortBy(_.id).collect()
		val broadcastedSet: Broadcast[Array[SpatialEntity]] = SparkContext.getOrCreate().broadcast(collectedSet)

		val allowedComparisons = BlockUtils.cleanBlocks(blocks.asInstanceOf[RDD[TBlock]])

		val blocksComparisons = blocks.map(b => (b.id, b))
		val comparisons = allowedComparisons.leftOuterJoin(blocksComparisons)
			.flatMap { b =>
				val targetArray = broadcastedSet.value
				val allowedComparisonsSet = b._2._1
				b._2._2.get
					.getComparisons
					.filter(c => allowedComparisonsSet.contains(c._1))
					.map(c => (c._2._1, targetArray(c._2._2.toInt - startIdFrom)))
			}
		if (!swapped)
			comparisons
				.filter(c => testMBB(c._1.mbb, c._2.mbb, relation))
				.filter(c => relate(c._1.geometry, c._2.geometry, relation))
				.map(c => (c._1.id, c._2.id))
		else
			comparisons
				.filter(c => testMBB(c._2.mbb, c._1.mbb, relation))
				.filter(c => relate(c._2.geometry, c._1.geometry, relation))
				.map(c => (c._2.id, c._1.id))
	}



	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleLong[A](implicit c: ClassTag[Long]): Encoder[Long] = Encoders.scalaLong

	implicit def tuple[Int, String](implicit e1: Encoder[Int], e2: Encoder[String]): Encoder[(Int,String)] = Encoders.tuple[Int,String](e1, e2)
	def disjointMatches(source: RDD[SpatialEntity], target: RDD[SpatialEntity]): RDD[(Long,Long)] ={
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
		disjointResults.rdd.map(r => (r.get(0).asInstanceOf[Long], r.get(1).asInstanceOf[Long]))
	}

}
