package DataStructures

import utils.Utils

case class Block(id: Int, coords: (Int, Int), sourceSet: Set[Int], targetAr: Set[Int])

object Block {

	def apply(coords: (Int, Int), sourceSet: Set[Int], targetSet: Set[Int]): Block ={
		Block(Utils.signedPairing(coords._1, coords._2), coords, sourceSet, targetSet)
	}
}