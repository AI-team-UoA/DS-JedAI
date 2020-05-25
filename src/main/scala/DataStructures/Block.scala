package DataStructures

import utils.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Block(id: Long, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]) {

	/**
	 * For each compaison in the block, return its id
	 * @return block's comparisons ids
	 */
	def getComparisonsIDs: Set[Long]={
		val comparisonsIDs = for (s <-source; t <- target)
			yield Utils.bijectivePairing(s.id, t.id)
		comparisonsIDs.toSet
	}

	def getComparisonsPairs: ArrayBuffer[(Int, Int)]={
		val comparisonsPairs = for (s <-source; t <- target)
			yield (s.id, t.id)
		comparisonsPairs
	}

	/**
	 * Return all blocks comparisons
	 * @return block's comparisons
	 */
	def getComparisons: ArrayBuffer[Comparison]={
		for (s <-source; t <- target)
			yield Comparison(s, t)
	}

	/**
	 *
	 * @return the total comparisons of the block
	 */
	def getTotalComparisons: Int = source.size * target.size

	/**
	 * Return only the comparisons that their MBBs relate and that their\
	 * reference points are inside the block
	 *
	 * @return blocks comparisons after filtering
	 */
	def getFilteredComparisons(relation: String): ArrayBuffer[Comparison]={
		for (s <-source; t <- target; if s.mbb.testMBB(t.mbb, relation) && s.mbb.referencePointFiltering(t.mbb, coords) )
			yield Comparison(s, t)
	}

	def getSourceIDs: ArrayBuffer[Int] =  source.map(se => se.id)

	def getTargetIDs: ArrayBuffer[Int] = target.map(se => se.id)

	def getSourceSize: Long = source.size

	def getTargetSize: Long = target.size

}

object Block {
	def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]): Block ={
		Block(Utils.signedPairing(coords._1, coords._2), coords, source, target)
	}
}