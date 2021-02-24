package geospatialInterlinking.progressive

import dataModel.{Entity, MBR, WeightedPair, WeightedPairsPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils


case class ProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                            mainWS: WeightingScheme, secondaryWS: Option[WeightingScheme], budget: Int)
    extends ProgressiveGeospatialInterlinkingT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the input weighting scheme and sort them using a PQ.
     *
     * @param partition the MBR: of the partition
     * @param source source
     * @param target target
     * @return a PQ with the top comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ ={
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val pq: WeightedPairsPQ = WeightedPairsPQ(budget)
        var counter: Int = 0
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach {j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val e1 = source(i)
                                val w = getMainWeight(e1, e2)
                                val secW = getSecondaryWeight(e1, e2)
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
object ProgressiveGIAnt {

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, sws: Option[WeightingScheme] = None,
              budget: Int, partitioner: Partitioner): ProgressiveGIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        ProgressiveGIAnt(joinedRDD, thetaXY, ws, sws, budget)
    }

}
