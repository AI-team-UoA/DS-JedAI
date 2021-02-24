package dataModel

case class WeightedPair(id: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float)  extends Serializable with Comparable[WeightedPair]{

    var relatedMatches: Int = 0

    override def compareTo(o: WeightedPair): Int = {
        // descendant order
        if (o.id == id) return 0

        val test1 = o.getMainWeight - getMainWeight
        if (0 < test1) return 1

        if (test1 < 0) return -1

        val test2 = o.getSecondaryWeight - getSecondaryWeight
        if (0 < test2) return 1

        if (test2 < 0) return -1
        // Note: Returning just the id leads to comparison method violation
        // as may lead to cases that A > B, B > C and C > A
        id - o.id
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

    override def toString: String = s"ID: $id E1 : $entityId1 E2 : $entityId2 main weight : $getMainWeight secondary weight : $getSecondaryWeight"
}
