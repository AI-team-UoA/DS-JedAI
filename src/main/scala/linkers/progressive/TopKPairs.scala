package linkers.progressive

import model.entities.EntityT
import model.TileGranularities
import model.structures.StaticComparisonPQ
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

case class TopKPairs(source: Array[EntityT], target: Iterable[EntityT],
                     tileGranularities: TileGranularities, partitionBorder: Envelope,
                     mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                     totalSourceEntities: Long, ws: Constants.WeightingScheme, totalBlocks: Double)
    extends ProgressiveLinkerT {

    /**
     * First we find the top-k comparisons of each geometry in source and target,
     * then we merge them in a common PQ and for each duplicate comparison
     * maintain the max weight.
     *
     * @param relation examining relation
     * @return prioritized comparisons in a PQ
     */
    def prioritize(relation: Relation): StaticComparisonPQ = {
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        // the budget is divided based on the number of entities
        val k = (math.ceil(localBudget / (source.length + targetAr.length)).toInt + 1) * 2 // +1 to avoid k=0
        val sourcePQ: Array[StaticComparisonPQ] = new Array(source.length)
        val targetPQ: StaticComparisonPQ = StaticComparisonPQ(k)
        val partitionPQ: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0

        targetAr
            .indices
            .foreach {j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder)
                candidates.foreach { case (i, s) =>
                    val wp = weightedPairFactory.createWeightedPair(counter, s, i, t, j)
                    counter += 1

                    // set top-K PQ for the examining target entity
                    targetPQ.enqueue(wp)

                    // update source entities' top-K
                    if (sourcePQ(i) == null)
                        sourcePQ(i) = StaticComparisonPQ(k)
                    sourcePQ(i).enqueue(wp)
                }

                // add target's pairs in partition's PQ
                if (!targetPQ.isEmpty) {
                    while (targetPQ.size > 0) {
                        val wp = targetPQ.dequeueHead()
                        partitionPQ.enqueue(wp)
                    }
                }
                targetPQ.clear()
            }

        // putting target comparisons in a HasMap. Source entities will also be added in the HashMap
        // to update wights and avoid duplicate comparisons
        val existingPairs = partitionPQ.iterator().toSet
        // adding source entities' top-K in hashMap
        sourcePQ
            .filter(_ != null)
            .foreach { pq =>
                pq.dequeueAll.foreach(wp => partitionPQ.enqueue(wp))
                pq.clear()
            }
        // keep partition's top comparisons
        partitionPQ.clear()
        partitionPQ.enqueueAll(existingPairs.iterator)
        partitionPQ
    }
}