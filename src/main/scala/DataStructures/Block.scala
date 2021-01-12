package DataStructures

import utils.Constants.Relation.Relation
import utils.Utils


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Block(id: Long, coords: (Int, Int), source: Array[Entity], target: Array[Entity]) {

	/**
	 * For each comparison in the block, return its id
	 * @return block's comparisons ids
	 */
//	def getComparisonsIDs: Set[Long]={
//		val comparisonsIDs = for (s <-source; t <- target)
//			yield Utils.bijectivePairing(s.id, t.id)
//		comparisonsIDs.toSet
//	}
//
//	def getComparisonsPairs: ArrayBuffer[(Int, Int)]={
//		val comparisonsPairs = for (s <-source; t <- target)
//			yield (s.id, t.id)
//		comparisonsPairs
//	}

	/**
	 * Return all blocks comparisons
	 * @return block's comparisons
	 */
	def getComparisons: Array[(Entity, Entity)]= for (s <-source; t <- target) yield (s, t)

	/**
	 *
	 * @return the total comparisons of the block
	 */
	def getTotalComparisons: Int = source.length * target.length

	/**
	 * Return only the comparisons that their MBBs relate and that their\
	 * reference points are inside the block
	 *
	 * @return blocks comparisons after filtering
	 */
	def getFilteredComparisons(relation: Relation): Array[(Entity, Entity)] =
		for (s <-source; t <- target; if s.testMBB(t, relation) && s.referencePointFiltering(t, coords, Utils.thetaXY))
			yield (s, t)

	def getSourceIDs: Array[String] = source.map(se => se.originalID)

	def getTargetIDs: Array[String] = target.map(se => se.originalID)

	def getSourceSize: Long = source.length

	def getTargetSize: Long = target.length

}

object Block {
	def apply(coords: (Int, Int), source: Array[Entity], target: Array[Entity]): Block ={
		Block(Utils.signedPairing(coords._1, coords._2), coords, source, target)
	}
}