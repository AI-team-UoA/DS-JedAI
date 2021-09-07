package model.weightedPairs

import utils.configuration.Constants.{SIMPLE, WeightingScheme}

/**
 *  In Main Weighted Pairs, we compare only using the main weight.
 *
 * @param counter incrementally increasing id
 * @param entityId1 id of source entity
 * @param entityId2 id of target entity
 * @param mainWeight main weight
 * @param secondaryWeight secondary weight
 */
case class SimpleWP(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float = 0f) extends WeightedPairT {

    val typeWP: WeightingScheme = SIMPLE

    override def compareTo(o: WeightedPairT): Int = {
        val mwp = o.asInstanceOf[SimpleWP]

        if (entityId1 == mwp.entityId1 && entityId2 == mwp.entityId2) 0
        else {
            val diff = mwp.getMainWeight - getMainWeight
            if (0 < diff) 1
            else if (diff < 0) -1
            else mwp.counter - counter
        }
    }
}