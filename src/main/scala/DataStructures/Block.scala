package DataStructures

import utils.Utils

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Block(id: Long, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]) extends TBlock {

	/**
	 * For each compaison in the block, return its id
	 * @return block's comparisons ids
	 */
	def getComparisonsIDs: Set[Long]={
		val comparisonsIDs = for (s <-source; t <- target)
			yield Utils.bijectivePairing(s.id, t.id)
		comparisonsIDs.toSet
	}

	def getComparisonsPairs: ArrayBuffer[(Long, Long)]={
		val comparisonsPairs = for (s <-source; t <- target)
			yield (s.id, t.id)
		comparisonsPairs
	}

	/**
	 * For each compaison in the block, return its id
	 * @return block's comparisons ids
	 */
	def getComparisons: ArrayBuffer[Comparison]={
		val comparisons = for (s <-source; t <- target)
			yield Comparison(s, t)
		comparisons
	}

	def getSourceIDs: ArrayBuffer[(Long, Long)] =  source.map(se => (se.id, id))

	def getTargetIDs: ArrayBuffer[(Long, Long)] = target.map(se => (se.id, id))
}

object Block {
	def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]): Block ={
		Block(Utils.signedPairing(coords._1, coords._2), coords, source, target)
	}
}