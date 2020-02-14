package DataStructures

import Blocking.BlockUtils

case class Comparison(id: Int, entity1_id: Int, entity2_id: Int)

object Comparison {
	def apply(id1: Int, id2: Int): Comparison ={
		Comparison(BlockUtils.bijectivePairing(id1, id2), id1, id2)
	}
}
