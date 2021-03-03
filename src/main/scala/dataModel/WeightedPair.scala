package dataModel

case class WeightedPair(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float)  extends Serializable with Comparable[WeightedPair]{

    var relatedMatches: Int = 0

    /**
     * Note: ID based comparison leads to violation of comparable contract
     * as may lead to cases that A > B, B > C and C > A. This is because the ids
     * indicate the index of each entity in the partitions array, if they are collected
     * it may lead to violations.
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPair): Int = {

        if (entityId1 == o.entityId1 && entityId2 == o.entityId2) return 0

        val test1 = o.getMainWeight - getMainWeight
        if (0 < test1) return 1

        if (test1 < 0) return -1

        val test2 = o.getSecondaryWeight - getSecondaryWeight
        if (0 < test2) return 1

        if (test2 < 0) return -1

        o.counter - counter
    }

    /**
     * Returns the weight between two geometries. Higher weights correspond to
     * stronger likelihood of related entities.
     *
     * @return
     */
    def getMainWeight: Float = mainWeight * (1 + relatedMatches)

    def getSecondaryWeight: Float = secondaryWeight * (1 + relatedMatches)

    def incrementRelatedMatches(): Unit = relatedMatches += 1

    override def toString: String = s"E1 : $entityId1 E2 : $entityId2 main weight : $getMainWeight secondary weight : $getSecondaryWeight"
}
