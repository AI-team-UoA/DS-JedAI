package linkers.progressive

import model.entities.Entity
import model.{SpatialIndex, StaticComparisonPQ, TileGranularities}
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction



case class ReciprocalTopK(source: Array[Entity], target: Iterable[Entity],
                          tileGranularities: TileGranularities, partitionBorder: Envelope,
                          mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                          totalSourceEntities: Long, ws: Constants.WeightingScheme, totalBlocks: Double)
    extends ProgressiveLinkerT {

    /**
     * Find the top-K comparisons of target and source and keep only the comparison (i, j) that belongs to both
     * top-K comparisons of i and j.
     *
     * @param relation examining relation
     * @return prioritized comparisons as a PQ
     */
    def prioritize(relation: Relation):  StaticComparisonPQ = {
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val sourceIndex = SpatialIndex(source, tileGranularities)
        val targetAr = target.toArray

        val sourceK = (math.ceil(localBudget / source.length).toInt + 1) * 2 // +1 to avoid k=0
        val targetK = (math.ceil(localBudget / targetAr.length).toInt + 1) * 2 // +1 to avoid k=0

        val sourcePQ: Array[StaticComparisonPQ] = new Array(source.length)
        val targetPQ: StaticComparisonPQ = StaticComparisonPQ(targetK)
        val partitionPQ: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0

        val targetSet: Array[Set[Int]] = new Array(targetAr.length)
        targetAr
            .indices
            .foreach {j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder, relation)
                candidates.foreach { case (i, s) =>
                    val wp = weightedPairFactory.createWeightedPair(counter, s, i, t, j)
                    counter += 1

                    // set top-K PQ for the examining target entity
                    targetPQ.enqueue(wp)

                    // update source entities' top-K
                    if (sourcePQ(i) == null)
                        sourcePQ(i) = StaticComparisonPQ(sourceK)
                    sourcePQ(i).enqueue(wp)
                }
                // add comparisons into corresponding HashSet
                targetSet(j) = targetPQ.iterator().map(_.entityId1).toSet
                targetPQ.clear()
            }

        // add comparison into PQ only if is contained by both top-K PQs
        sourcePQ
            .filter(_ != null)
            .foreach { pq =>
                pq.iterator()
                    .filter(wp => targetSet(wp.entityId2).contains(wp.entityId1))
                    .foreach(wp => partitionPQ.enqueue(wp))
            }
        partitionPQ
    }
}