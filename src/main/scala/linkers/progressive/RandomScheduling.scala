package linkers.progressive

import model.entities.Entity
import model._
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

case class RandomScheduling(source: Array[Entity], target: Iterable[Entity],
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
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partitionBorder, relation)
                candidates.foreach { case (si, _) =>
                    val w = rnd.nextFloat()
                    val secW = rnd.nextFloat()
                    val wp = MainWP(counter, si, j, w, secW)
                    pq.enqueue(wp)
                    counter += 1
                }
            }
        pq
    }
}