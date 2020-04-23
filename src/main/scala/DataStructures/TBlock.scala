package DataStructures

import scala.collection.mutable.ArrayBuffer

trait TBlock {
    val id: Long
    val coords: (Int, Int)

    def getComparisonsIDs: Set[Long]

    def getComparisonsPairs: ArrayBuffer[(Int, Int)]

    def getSourceIDs: ArrayBuffer[Int]

    def getTargetIDs: ArrayBuffer[Int]

    def getSourceSize(): Long

    def getTargetSize(): Long

}
