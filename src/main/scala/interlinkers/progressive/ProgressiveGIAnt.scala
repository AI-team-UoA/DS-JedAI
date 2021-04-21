package interlinkers.progressive

import model.{Entity, MBR, StaticComparisonPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction
import utils.{Constants, Utils}


case class ProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                            mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                            sourceEntities: Int, ws: Constants.WeightingScheme)
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
        val localBudget = (math.ceil(budget*source.length.toDouble/sourceEntities.toDouble)*2).toLong
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

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], wf: WeightingFunction, swf: Option[WeightingFunction] = None,
              budget: Int, partitioner: Partitioner, ws: Constants.WeightingScheme = Constants.SINGLE): ProgressiveGIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        val sourceEntities = Utils.sourceCount
        ProgressiveGIAnt(joinedRDD, thetaXY, wf, swf, budget, sourceEntities.toInt, ws)
    }

}
