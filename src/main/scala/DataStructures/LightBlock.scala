package DataStructures

import Blocking.BlockUtils

case class LightBlock(id: Int, coords: (Int, Int), sourceSet: Set[SpatialEntity], targetIDs: Set[Int]) extends TBlock {

    /**
     * For each compaison in the block, return its id
     * @return block's comparisons ids
     */
   def getComparisonsIDs: Set[Int]={
        val comparisonsIDs =
            for (s <-sourceSet; t <- targetIDs)
                yield BlockUtils.bijectivePairing(s.id, t)
        comparisonsIDs
    }
}

object LightBlock{
    def apply(coords: (Int, Int), sourceSet: Set[SpatialEntity], targetSet: Set[Int]): LightBlock ={
        LightBlock(BlockUtils.signedPairing(coords._1, coords._2), coords, sourceSet, targetSet)
    }
}
