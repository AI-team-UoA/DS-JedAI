package DataStructures

import utils.Utils

import scala.collection.mutable.ArrayBuffer

case class LightBlock(id: Long, coords: (Int, Int), source: ArrayBuffer[SpatialEntity], targetIDs: ArrayBuffer[Int]) {

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

    def getComparisonsPairs: ArrayBuffer[(Int, Int)]={
        val comparisonsPairs = for (s <-source; tid <- targetIDs)
            yield (s.id, tid)
        comparisonsPairs
    }

    /**
     * For each compaison in the block, return its id
     * @return block's comparisons ids
     */
    def getComparisons: ArrayBuffer[(Long, (SpatialEntity, Int))]={
        for (s <-source; tID <- targetIDs)
            yield (Utils.bijectivePairing(s.id, tID), (s, tID))
    }

    def getSourceIDs: ArrayBuffer[Int] =  source.map(se => se.id)

    def getTargetIDs: ArrayBuffer[Int] = targetIDs.map(tid => tid)

    def getSourceSize: Long = source.size

    def getTargetSize: Long = targetIDs.size

}

object LightBlock{
    def apply(coords: (Int, Int), source: ArrayBuffer[SpatialEntity], target: ArrayBuffer[Int]): LightBlock ={
        LightBlock(Utils.signedPairing(coords._1, coords._2), coords, source, target)
    }
}
