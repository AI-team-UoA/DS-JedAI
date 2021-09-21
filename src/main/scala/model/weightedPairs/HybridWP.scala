package model.weightedPairs

import utils.configuration.Constants.{HYBRID, WeightingScheme}

/**
 *  In Hybrid Weighted Pairs, the weight is determined by the product of the weights.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class HybridWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float) extends WeightedPairT{

    val typeWP: WeightingScheme = HYBRID

    // the weights are not constant, so we recalculate the product
    def getProduct: Float = getMainWeight * getSecondaryWeight

    /**
     *
     * CompareTo will sort elements in a descendant order
     *
     * @param o a weighted pair
     * @return 1 if o is greater, 0 if they are equal, -1 if o is lesser.
     */
    override def compareTo(o: WeightedPairT): Int = {

        val hwp = o.asInstanceOf[HybridWP]

        if (entityId1 == hwp.entityId1 && entityId2 == hwp.entityId2) 0
        else {
            val diff = hwp.getProduct - getProduct
            if (0 < diff) 1
            else if (diff < 0) -1
            else hwp.counter - counter
        }
    }

    def getSecondaryWeight: Float = secondaryWeight * (1 + relatedMatches)

    def getLastWeight: Float = 0f

}