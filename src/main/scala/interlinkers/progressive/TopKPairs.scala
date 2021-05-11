package interlinkers.progressive

import model.entities.Entity
import model.{MBR, StaticComparisonPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Constants.Relation.Relation
import utils.Constants.WeightingFunction.WeightingFunction

case class TopKPairs(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                     thetaXY: (Double, Double), partitionBorders: Array[MBR],
                     mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction], budget: Int,
                     totalSourceEntities: Long, ws: Constants.WeightingScheme)
    extends ProgressiveInterlinkerT {

    /**
     * First we find the top-k comparisons of each geometry in source and target,
     * then we merge them in a common PQ and for each duplicate comparison
     * maintain the max weight.
     *
     * @param source partition of source dataset
     * @param target partition of target dataset
     * @param partition partition MBR
     * @param relation examining relation
     * @return prioritized comparisons in a PQ
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): StaticComparisonPQ = {
        val localBudget = math.ceil(budget*source.length.toDouble/totalSourceEntities.toDouble).toLong
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        // the budget is divided based on the number of entities
        val k = (math.ceil(localBudget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0
        val sourcePQ: Array[StaticComparisonPQ] = new Array(source.length)
        val targetPQ: StaticComparisonPQ = StaticComparisonPQ(k)
        val partitionPQ: StaticComparisonPQ = StaticComparisonPQ(localBudget)
        var counter = 0

        target.indices
            .foreach{ j =>
                val t = target(j)
                t.index(thetaXY, filterIndices)
                    .foreach{ block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(t, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val s = source(i)
                                val wp = getWeightedPair(counter, s, i, t, j)
                                counter += 1

                                // set top-K PQ for the examining target entity
                                targetPQ.enqueue(wp)

                                // update source entities' top-K
                                if (sourcePQ(i) == null)
                                    sourcePQ(i) = StaticComparisonPQ(k)
                                sourcePQ(i).enqueue(wp)
                            }
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

object TopKPairs{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)],
              thetaXY: (Double, Double), partitionBorders: Array[MBR], sourceCount: Long, wf: WeightingFunction,
              swf: Option[WeightingFunction] = None, budget: Int, partitioner: Partitioner,
              ws: Constants.WeightingScheme): TopKPairs ={

        val joinedRDD = source.cogroup(target, partitioner)
        TopKPairs(joinedRDD, thetaXY, partitionBorders,  wf, swf, budget, sourceCount, ws)
    }
}
