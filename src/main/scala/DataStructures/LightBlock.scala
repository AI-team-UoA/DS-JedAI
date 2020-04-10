package DataStructures

import Blocking.BlockUtils

import scala.collection.mutable.ArrayBuffer

case class LightBlock(id: Int, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], targetIDs: ArrayBuffer[Int]) extends TBlock {

    /**
     * For each compaison in the block, return its id
     * @return block's comparisons ids
     */
   def getComparisonsIDs: Set[Int]={
        val comparisonsIDs =
            for (s <-source; t <- targetIDs)
                yield BlockUtils.bijectivePairing(s.id, t)
        comparisonsIDs.toSet
    }
}

object LightBlock{
    def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[Int]): LightBlock ={
        LightBlock(BlockUtils.signedPairing(coords._1, coords._2), coords, source, target)
    }
}
