package interlinkers.progressive

import model.{Entity, MBR, StaticComparisonPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction


case class ProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                            thetaXY: (Double, Double), partitionBorders: Array[MBR],
                            mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                            totalSourceEntities: Long, ws: Constants.WeightingScheme)
    extends ProgressiveInterlinkerT {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the input weighting scheme and sort them using a PQ.
     *
     * @param partition the MBR: of the partition
     * @param source source
     * @param target target
     * @return a PQ with the top comparisons
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): StaticComparisonPQ ={
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val pq: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach {j =>
                val t = target(j)
                t.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(t, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val s = source(i)
                                val wp = getWeightedPair(counter, s, i, t, j)
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

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)],
              thetaXY: (Double, Double), partitionBorders: Array[MBR], sourceCount: Long, wf: WeightingFunction,
              swf: Option[WeightingFunction] = None, budget: Int, partitioner: Partitioner,
              ws: Constants.WeightingScheme): ProgressiveGIAnt ={

        val joinedRDD = source.cogroup(target, partitioner)
        ProgressiveGIAnt(joinedRDD, thetaXY, partitionBorders,  wf, swf, budget, sourceCount, ws)
    }

}
