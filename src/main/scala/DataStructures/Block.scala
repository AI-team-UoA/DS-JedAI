package DataStructures

import Blocking.BlockUtils

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Block(id: Int, coords: (Int, Int), sourceSet: Set[SpatialEntity], targetSet: Set[SpatialEntity]){

	/**
	 * For each compaison in the block, return its id
	 * @return block's comparisons ids
	 */
	def getComparisonsIDs: Set[Int]={
		val comparisonsIDs = for (s <-sourceSet; t <- targetSet)
			yield BlockUtils.bijectivePairing(s.id, t.id)
		comparisonsIDs
	}
}

object Block {
	def apply(coords: (Int, Int), sourceSet: Set[SpatialEntity], targetSet: Set[SpatialEntity]): Block ={
		Block(BlockUtils.signedPairing(coords._1, coords._2), coords, sourceSet, targetSet)
	}
}