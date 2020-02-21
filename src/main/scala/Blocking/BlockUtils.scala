package Blocking

import DataStructures.{Block, SpatialEntity}
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object BlockUtils {
	var swapped = false

	/**
	 * Cantor Pairing function. Map two positive integers to a unique integer number.
	 *
	 * @param a integer
	 * @param b integer
	 * @return the unique mapping of the integers
	 */
	def cantorPairing(a: Int, b: Int): Int =  (((a + b) * (a + b + 1))/2) + b

	/**
	 * Bijective cantor pairing. CantorPairing(x, y) == CantorPairing(y, x)
	 *
	 * @param x integer
	 * @param y integer
	 * @return the unique mapping of the integers
	 */
	def bijectivePairing(x: Int, y: Int): Int ={
		if (x < y)
			cantorPairing(y, x)
		else
			cantorPairing(y, x)
	}

	/**
	 * Apply cantor pairing for negative integers
	 *
	 * @param x integer
	 * @param y integer
	 * @return the unique mapping of the integers
	 */
	def signedPairing(x: Int, y: Int): Int ={
		val a = if (x < 0) (-2)*x - 1 else 2*x
		val b = if (y < 0) (-2)*y - 1 else 2*y

		cantorPairing(a, b)
	}


	/**
	 * Remove duplicate comparisons from blocks by keeping each comparison only to the block
	 * with the minimum id.
	 *
	 * @param blocks an RDD of Blocks
	 * @return an RDD of Comparisons
	 */
	def cleanBlocks(blocks: RDD[Block]): RDD[(Int, ArrayBuffer[Int])] ={
		blocks
			.map(b => (b.id, b.getComparisonsIDs))
			.flatMap(b => b._2.map(c => (c, ArrayBuffer(b._1))))
			.reduceByKey(_ ++ _)
    		.map(cb => (cb._2.min, ArrayBuffer(cb._1)))
    		.reduceByKey(_ ++ _)
	}


	/**
	 * Compute the Estimation of the Total Hyper-volume
	 *
	 * @param seRDD Spatial Entities
	 * @return Estimation of the Total Hyper-volume
	 */
	def getETH(seRDD: RDD[SpatialEntity]): Double ={
		getETH(seRDD, seRDD.count())
	}

	/**
	 * Compute the Estimation of the Total Hyper-volume
	 *
	 * @param seRDD Spatial Entities
	 * @param count number of the entities
	 * @return Estimation of the Total Hyper-volume
	 */
	def getETH(seRDD: RDD[SpatialEntity], count: Double): Double ={
		val denom = 1/count
		val coords_sum = seRDD
			.map(se => (se.mbb.maxX - se.mbb.minX, se.mbb.maxY - se.mbb.minY))
			.fold((0, 0)) { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2) }

		val eth = count * ( (denom * coords_sum._1) * (denom * coords_sum._2) )
		eth
	}

	/**
	 * Swaps source to the set with the smallest ETH, and change the relation respectively.
	 *
	 * @param sourceRDD source
	 * @param targetRDD target
	 * @param relation relation
	 * @return the swapped values
	 */
	def swappingStrategy(sourceRDD: RDD[SpatialEntity], targetRDD: RDD[SpatialEntity], relation: String):
	(RDD[SpatialEntity], RDD[SpatialEntity], String)= {

		val sourceETH = BlockUtils.getETH(sourceRDD)
		val targetETH = BlockUtils.getETH(targetRDD)

		if (targetETH < sourceETH){
			swapped = true
			val newRelation =
				relation match {
					case Constants.WITHIN => Constants.CONTAINS
					case Constants.CONTAINS => Constants.WITHIN
					case Constants.COVERS => Constants.COVEREDBY
					case Constants.COVEREDBY => Constants.COVERS;
					case _ => relation
				}
			(targetRDD, sourceRDD, newRelation)
		}
		else
			(sourceRDD, targetRDD, relation)
	}

}
