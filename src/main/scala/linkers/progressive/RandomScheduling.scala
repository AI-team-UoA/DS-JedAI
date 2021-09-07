package linkers.progressive

import model.entities.EntityT
import model._
import model.structures.StaticComparisonPQ
import model.weightedPairs.SimpleWP
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

case class RandomScheduling(source: Array[EntityT], target: Iterable[EntityT],
                            tileGranularities: TileGranularities, partitionBorder: Envelope,
                            mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                            totalSourceEntities: Long, ws: Constants.WeightingScheme, totalBlocks: Double)
    extends ProgressiveLinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     *
     * @return a PQ with the top comparisons
     */
    def prioritize(relation: Relation): StaticComparisonPQ = {
        val targetAr = target.toArray
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val pq: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0
        val rnd = new scala.util.Random
        // weight and put the comparisons in a PQ
        targetAr
            .indices
            .foreach {j =>
                val t = targetAr(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder)
                candidates.foreach { case (si, _) =>
                    val w = rnd.nextFloat()
                    val secW = rnd.nextFloat()
                    val wp = SimpleWP(counter, si, j, w, secW)
                    pq.enqueue(wp)
                    counter += 1
                }
            }
        pq
    }
}