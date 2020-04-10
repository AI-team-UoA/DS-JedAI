package DataStructures

import Blocking.BlockUtils

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class Block(id: Int, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]) extends TBlock {

	/**
	 * For each compaison in the block, return its id
	 * @return block's comparisons ids
	 */
	def getComparisonsIDs: Set[Int]={
		val comparisonsIDs = for (s <-source; t <- target)
			yield BlockUtils.bijectivePairing(s.id, t.id)
		comparisonsIDs.toSet
	}
}

object Block {
	def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[SpatialEntity]): Block ={
		Block(BlockUtils.signedPairing(coords._1, coords._2), coords, source, target)
	}
}