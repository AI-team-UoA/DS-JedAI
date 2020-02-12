package utils

import DataStructures.{Block, Comparison}
import org.apache.spark.SparkContext
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
	 * Remove the duplicate comparisons from blocks by keeping each comparison only to the block
	 * with the minimum id. In order to do that, for each comparison, we create a list of
	 * blocksIDs from which we keep only the minimum. Then by aggregating, for each block we
	 * have a list with only the necessary comparisons. Using a leftOuterJoin, we combine it
	 * to the RDD with the comparisons and we filter out the duplicate comparisons.
	 *
	 * @param blocks an RDD of Blocks
	 * @return an RDD of Comparisons
	 */
	def cleanBlocks(blocks: RDD[Block]): RDD[Comparison] ={
		val blockComparisonsRDD: RDD[(Int, Set[Comparison])] = blocks.map(b => (b.id, b.getComparisons)).setName("BlockComparisons").cache()

		val comparisonsPerBlock = blockComparisonsRDD
			.flatMap(b => b._2.map(c => (c.id, Array(b._1)))).reduceByKey(_ ++ _)	// RDD[(comparisonID, Array(BlockID))]
    		.map(cb => (cb._2.min, Set(cb._1))) 									// get min blockID
    		.reduceByKey(_ ++ _)													// back to RDD[(bBlockID, Set[ComparisonsID]) but now each block will contain unique comparisons

		blockComparisonsRDD.leftOuterJoin(comparisonsPerBlock) // by using leftOuterJoin we avoid collect-Broadcast, and also each block will get a set instead of a more complex structure
			.filter(b => b._2._2.isDefined)
			.flatMap { b =>
				val comparisons = b._2._1
				val acceptedComparisons = b._2._2.get
				comparisons.filter(c => acceptedComparisons.contains(c.id)).map(c => c)	// filtering out the duplicate comparisons
			}
	}

	def cleanBlocks2(blocks: RDD[Block]): RDD[Comparison] ={
		val blockComparisonsRDD: RDD[(Int, Set[Comparison])] = blocks.map(b => (b.id, b.getComparisons)).setName("BlockComparisons").cache()

		val comparisonsPerBlock = blockComparisonsRDD
			.flatMap(b => b._2.map(c => (c.id, Array(b._1)))).reduceByKey(_ ++ _)	// RDD[(comparisonID, Array(BlockID))]
			.map(cb => (cb._2.min, Set(cb._1))) 									// get min blockID
			.reduceByKey(_ ++ _)													// back to RDD[(bBlockID, Set[ComparisonsID]) but now each block will contain unique comparisons

		val comparisonsMap = comparisonsPerBlock.collectAsMap()
		val comparisonsMapBD = SparkContext.getOrCreate().broadcast(comparisonsMap)
		blockComparisonsRDD
			.filter(b => comparisonsMapBD.value.keySet.contains(b._1))
			.flatMap { b =>
				val blockID = b._1
				val comparisons = b._2
				val acceptedComparisons = comparisonsMapBD.value(blockID)
				comparisons.filter(c => acceptedComparisons.contains(c.id)).map(c => c)
			}
	}
}
