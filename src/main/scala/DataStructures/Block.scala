package DataStructures

import Blocking.BlockUtils

case class Block(id: Int, coords: (Int, Int), sourceSet: Set[Int], targetSet: Set[Int]){

	def getComparisons: Set[Comparison]={
		val comparisons = for (s <-sourceSet; t <- targetSet)
			yield Comparison(s, t)
		comparisons
	}
}

object Block {

	def apply(coords: (Int, Int), sourceSet: Set[Int], targetSet: Set[Int]): Block ={
		Block(BlockUtils.signedPairing(coords._1, coords._2), coords, sourceSet, targetSet)
	}
}