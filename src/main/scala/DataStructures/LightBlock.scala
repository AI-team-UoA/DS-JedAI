package DataStructures

import utils.Utils

import scala.collection.mutable.ArrayBuffer

case class LightBlock(id: Long, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], targetIDs: ArrayBuffer[Long]) extends TBlock {

    /**
     * For each compaison in the block, return its id
     * @return block's comparisons ids
     */
   def getComparisonsIDs: Set[Long]={
        val comparisonsIDs =
            for (s <-source; t <- targetIDs)
                yield Utils.bijectivePairing(s.id, t)
        comparisonsIDs.toSet
    }

    def getComparisonsPairs: ArrayBuffer[(Long, Long)]={
        val comparisonsPairs = for (s <-source; tid <- targetIDs)
            yield (s.id, tid)
        comparisonsPairs
    }

    def getSourceIDs: ArrayBuffer[(Long, Long)] =  source.map(se => (se.id, id))

    def getTargetIDs: ArrayBuffer[(Long, Long)] = targetIDs.map(tid => (tid, id))
}

object LightBlock{
    def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[Long]): LightBlock ={
        LightBlock(Utils.signedPairing(coords._1, coords._2), coords, source, target)
    }
}
