package utils

import DataStructures.{Block, Comparison}
import org.apache.spark.rdd.RDD

object BlockUtils {

	def bijectivePairing(x: Int, y: Int): Int ={
		if (x < y)
			cantorPairing(y, x)
		else
			cantorPairing(y, x)
	}

	def signedPairing(x: Int, y: Int): Int ={
		val a = if (x < 0) (-2)*x - 1 else 2*x
		val b = if (y < 0) (-2)*y - 1 else 2*y

		cantorPairing(a, b)
	}
	def cantorPairing(a: Int, b: Int): Int =  (((a + b) * (a + b + 1))/2) + b

	/**
	 * Remove duplicate comparisons from blocks by keeping each comparison only to the block
	 * with the minimum id.
	 *
	 * @param blocks an RDD of Blocks
	 * @return an RDD of Comparisons
	 */
	def cleanBlocks(blocks: RDD[Block]): RDD[Comparison] ={
		blocks
			.map(b => (b.id, b.getComparisons))
			.flatMap(b => b._2.map(c => (c, Array(b._1))))
			.reduceByKey(_ ++ _)
    		.map(cb => (cb._2.min, Array(cb._1)))
    		.reduceByKey(_ ++ _)
			.flatMap(_._2)
	}

}
