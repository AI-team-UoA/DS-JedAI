package model.weightedPairs

import utils.configuration.Constants.{COMPOSITE, WeightingScheme}

/**
 *  In Composite Weighted Pairs, we use the secondary weight only as a resolver of ties.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class CompositeWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPairT{

    val typeWP: WeightingScheme = COMPOSITE

    /**
     * Note: ID based comparison leads to violation of comparable contract
     * as may lead to cases that A > B, B > C and C > A.
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPairT): Int = {

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