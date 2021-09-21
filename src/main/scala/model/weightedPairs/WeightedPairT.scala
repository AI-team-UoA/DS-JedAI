package model.weightedPairs

import utils.configuration.Constants._

trait WeightedPairT extends Serializable with Comparable[WeightedPairT] {

    val counter: Int
    val entityId1: Int
    val entityId2: Int
    val mainWeight: Float
    val typeWP: WeightingScheme

    var relatedMatches: Int = 0

    override def toString: String = s"${typeWP.value} WP s: $entityId1 t: $entityId2 main weight: $getMainWeight secondary weight: $getSecondaryWeight"

    /**
     * Returns the weight between two geometries. Higher weights indicate to
     * stronger likelihood of related entities.
     */
    def getMainWeight: Float = mainWeight * (1 + relatedMatches)

    def getSecondaryWeight: Float

    def getLastWeight: Float

    def incrementRelatedMatches(): Unit = relatedMatches += 1

    override def compareTo(o: WeightedPairT): Int
}

