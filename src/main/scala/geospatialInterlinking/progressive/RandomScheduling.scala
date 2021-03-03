package geospatialInterlinking.progressive

import dataModel.{Entity, MBR, WeightedPair, WeightedPairsPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils

case class RandomScheduling(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                            mainWS: WeightingScheme, secondaryWS: Option[WeightingScheme], budget: Int, sourceEntities: Int)
    extends ProgressiveGeospatialInterlinkingT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     *
     * @param partition the MBR: of the partition
     * @param source    source
     * @param target    target
     * @return a PQ with the top comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ = {
        val localBudget = (math.ceil(budget*source.length.toDouble/sourceEntities.toDouble)*2).toInt

        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val pq: WeightedPairsPQ = WeightedPairsPQ(localBudget)
        val rnd = new scala.util.Random
        var counter = 0
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach { j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val w = rnd.nextFloat()
                                val secW = rnd.nextFloat()
                                val wp = WeightedPair(counter, i, j, w, secW)
                                pq.enqueue(wp)
                                counter += 1
                            }
                    }
            }
        pq
    }
}


/**
 * auxiliary constructor
 */
object RandomScheduling {

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, sws: Option[WeightingScheme] = None,
              budget: Int, partitioner: Partitioner): RandomScheduling ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        val sourceEntities = Utils.sourceCount
        RandomScheduling(joinedRDD, thetaXY, ws, sws, budget, sourceEntities.toInt)
    }

}
