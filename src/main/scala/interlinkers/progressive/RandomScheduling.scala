package interlinkers.progressive

import model.entities.Entity
import model._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

case class RandomScheduling(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                            tileGranularities: TileGranularities, partitionBorders: Array[Envelope],
                            mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                            totalSourceEntities: Long, ws: Constants.WeightingScheme)
    extends ProgressiveInterlinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     *
     * @param partition the MBR: of the partition
     * @param source    source
     * @param target    target
     * @return a PQ with the top comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: Envelope, relation: Relation): StaticComparisonPQ = {

        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val sourceIndex = SpatialIndex(source, tileGranularities)
        val pq: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0
        val rnd = new scala.util.Random
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach {j =>
                val t = target(j)
                val candidates = getAllCandidatesWithIndex(t, sourceIndex, partition, relation)
                candidates.foreach { case (si, s) =>
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


/**
 * auxiliary constructor
 */
object RandomScheduling {

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)],
              tileGranularities: TileGranularities, partitionBorders: Array[Envelope], sourceCount: Long, wf: WeightingFunction,
              swf: Option[WeightingFunction] = None, budget: Int, partitioner: Partitioner,
              ws: Constants.WeightingScheme): RandomScheduling ={

        val joinedRDD = source.cogroup(target, partitioner)
        RandomScheduling(joinedRDD, tileGranularities, partitionBorders,  wf, swf, budget, sourceCount, ws)
    }

}
