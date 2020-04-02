package EntityMatching

import Blocking.BlockUtils
import DataStructures.{Block, LightBlock, MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import utils.Constants
import utils.Utils.spark

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

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
	 * Perform the comparisons of the blocks. Only the comparisons inside the allowedComparison
	 * will be calculated.
	 *
	 * @param blocks RDD of blocks
	 * @param allowedComparisons allowed comparisons per Block - RDD[(blockID, Array[comparisonID])]
 	 * @param relation requested relation
	 * @return the matches
	 */
	def SpatialMatching(blocks: RDD[Block], allowedComparisons: RDD[(Int, ArrayBuffer[Int])], relation: String): RDD[(Int,Int)] ={

		val blocksComparisons = blocks.map(b => (b.id, (b.sourceSet, b.targetSet)))
		val matches = blocksComparisons.leftOuterJoin(allowedComparisons)
    		.filter(_._2._2.isDefined)
    		.map { b =>
				val sourceSet = b._2._1._1
				val targetSet = b._2._1._2
				val allowedComparisons = b._2._2.get

				val matches: ArrayBuffer[(Int,Int)] = ArrayBuffer()
				for (s <- sourceSet.toIterator; t <- targetSet.toIterator){
					val comparisonID = BlockUtils.bijectivePairing(s.id, t.id)
					if (allowedComparisons.contains(comparisonID)){
						if (testMBB(s.mbb, t.mbb, relation))
						// TODO meridian case
						// TODO: append ids or originalIDs ?
							if (relate(s.geometry, t.geometry, relation))
								matches.append((s.id, t.id))
					}
				}
				matches
			}
    		.flatMap(a => a)
			matches
	}

	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt

	implicit def tuple[Int, String](implicit e1: Encoder[Int], e2: Encoder[String]): Encoder[(Int,String)] = Encoders.tuple[Int,String](e1, e2)
	def disjointMatches(source: RDD[SpatialEntity], target: RDD[SpatialEntity]): RDD[(Int,Int)] ={
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


	def lightMatching(blocks: RDD[LightBlock], toCollect: RDD[SpatialEntity], startIdFrom: Int, allowedComparisons: RDD[(Int, ArrayBuffer[Int])], relation: String, swapped:Boolean): RDD[(Int, Int)] = {
		val collectedSet: Array[SpatialEntity] = toCollect.sortBy(_.id).collect()
		val broadcastedSet: Broadcast[Array[SpatialEntity]] = SparkContext.getOrCreate().broadcast(collectedSet)

		val blocksComparisons = blocks.map(b => (b.id, (b.sourceSet, b.targetIDs)))
		val matches = blocksComparisons.leftOuterJoin(allowedComparisons)
			.filter(_._2._2.isDefined)
			.map { b =>
				val sourceSet = b._2._1._1
				val targetIDs = b._2._1._2
				val allowedComparisons = b._2._2.get
				val targetArray = broadcastedSet.value

				val matches: ArrayBuffer[(Int,Int)] = ArrayBuffer()
				for (sourceSE <- sourceSet.toIterator; targetID <- targetIDs.toIterator){
					val comparisonID = BlockUtils.bijectivePairing(sourceSE.id, targetID)
					if (allowedComparisons.contains(comparisonID)){
						val targetSE = targetArray(targetID - startIdFrom)
						val passTest = if(!swapped) testMBB(sourceSE.mbb, targetSE.mbb, relation) else testMBB(targetSE.mbb, sourceSE.mbb, relation)
						if (passTest) {
							// TODO meridian case
							// TODO: append ids or originalIDs ?
							val doRelate = if (!swapped) relate(sourceSE.geometry, targetSE.geometry, relation) else relate(targetSE.geometry, sourceSE.geometry, relation)
							if (doRelate) matches.append((sourceSE.id, targetID))
						}
					}
				}
				matches
			}
			.flatMap(a => a)
		matches
	}
}
