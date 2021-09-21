package model.weightedPairs
import utils.configuration.Constants
import utils.configuration.Constants.THIN_MULTI_COMPOSITE

case class ThinMultiCompositePair(counter: Int, entityId1: Int, entityId2: Int, mainWeight: Float, secondaryWeight: Float, lastWeight: Float) extends WeightedPairT{
    override val typeWP: Constants.WeightingScheme = THIN_MULTI_COMPOSITE

    override def compareTo(o: WeightedPairT): Int ={
        val test = getMainWeight - o.getMainWeight
        if (0 < test) return -1
        if (test < 0) return 1

        val test2 = getSecondaryWeight - o.getSecondaryWeight
        if (0 < test2) return -1
        if (test2 < 0) return 1

        val test3 = getLastWeight - o.getLastWeight
        if (0 < test3) return -1
        if (test3 < 0) return 1;
        0
    }

    def getSecondaryWeight: Float = secondaryWeight * (1 + relatedMatches)

    def getLastWeight: Float = lastWeight
}
