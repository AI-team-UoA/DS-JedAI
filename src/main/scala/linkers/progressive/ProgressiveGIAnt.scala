package linkers.progressive

import model._
import model.entities.Entity
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction


case class ProgressiveGIAnt(source: Array[Entity], target: Iterable[Entity],
                            tileGranularities: TileGranularities, partitionBorder: Envelope,
                            mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                            totalSourceEntities: Long, ws: Constants.WeightingScheme, totalBlocks: Double)
    extends ProgressiveLinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the input weighting scheme and sort them using a PQ.
     *
     * @return a PQ with the top comparisons
     */
    def prioritize(relation: Relation): ComparisonPQ ={
        val targetAr = target.toArray
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val pq: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0
        // weight and put the comparisons in a PQ
        targetAr
            .indices
            .foreach {j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder, relation)
                candidates.foreach { case (i, s) =>
                    val wp = weightedPairFactory.createWeightedPair(counter, s, i, t, j)
                    pq.enqueue(wp)
                    counter += 1
                }
            }
        pq
    }
}
