package model



sealed trait WeightedPair extends Serializable with Comparable[WeightedPair] {

    val counter: Int
    val entityId1: Int
    val entityId2: Int
    val mainWeight: Float
    val secondaryWeight: Float

    var relatedMatches: Int = 0

    override def toString: String = s"s : $entityId1 t : $entityId2 main weight : $getMainWeight secondary weight : $getSecondaryWeight"

    /**
     * Returns the weight between two geometries. Higher weights indicate to
     * stronger likelihood of related entities.
     */
    def getMainWeight: Float = mainWeight * (1 + relatedMatches)

    def getSecondaryWeight: Float = secondaryWeight * (1 + relatedMatches)

    def incrementRelatedMatches(): Unit = relatedMatches += 1

    override def compareTo(o: WeightedPair): Int
}


/**
 *  In Main Weighted Pairs, we compare only using the main weight.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class MainWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float = 0f) extends WeightedPair {

    override def compareTo(o: WeightedPair): Int = {
        val mwp = o.asInstanceOf[MainWP]

        if (entityId1 == mwp.entityId1 && entityId2 == mwp.entityId2) 0
        else {
            val diff = mwp.mainWeight - mainWeight
            if (0 < diff) 1
            else if (diff < 0) -1
            else mwp.counter - counter
        }

    }
}


/**
 *  In Composite Weighted Pairs, we use the secondary weight only as a resolver of ties.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class CompositeWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPair{

    /**
     * Note: ID based comparison leads to violation of comparable contract
     * as may lead to cases that A > B, B > C and C > A.
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPair): Int = {

        val cwp = o.asInstanceOf[CompositeWP]

        if (entityId1 == cwp.entityId1 && entityId2 == cwp.entityId2) return 0

        val diff1 = cwp.getMainWeight - getMainWeight
        if (0 < diff1) return 1

        if (diff1 < 0) return -1

        val diff2 = cwp.getSecondaryWeight - getSecondaryWeight
        if (0 < diff2) return 1

        if (diff2 < 0) return -1

        cwp.counter - counter
    }
}


/**
 *  In Hybrid Weighted Pairs, the weight is determined by the product of the weights.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class HybridWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPair{

    // the weights are not constant, so we recalculate the product
    def getProduct: Float = mainWeight * secondaryWeight

    /**
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPair): Int = {

        val hwp = o.asInstanceOf[HybridWP]

        if (entityId1 == hwp.entityId1 && entityId2 == hwp.entityId2) 0
        else {
            val diff = hwp.getProduct - getProduct
            if (0 < diff) 1
            else if (diff < 0) -1
            else hwp.counter - counter
        }
    }
}
