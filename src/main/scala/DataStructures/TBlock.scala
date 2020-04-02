package DataStructures

trait TBlock {
    val id: Int
    val coords: (Int, Int)

    def getComparisonsIDs: Set[Int]
}
