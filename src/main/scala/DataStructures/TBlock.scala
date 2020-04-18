package DataStructures

import scala.collection.mutable.ArrayBuffer

trait TBlock {
    val id: Long
    val coords: (Int, Int)

    def getComparisonsIDs: Set[Long]

    def getComparisonsPairs: ArrayBuffer[(Long, Long)]

    def getSourceIDs: ArrayBuffer[Long]

    def getTargetIDs: ArrayBuffer[Long]

    def getSourceSize(): Long

    def getTargetSize(): Long

}
