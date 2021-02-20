package geospatialInterlinking.progressive

import dataModel.{Entity, MBR, WeightedPair, WeightedPairsPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils

case class TopKPairs(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                     thetaXY: (Double, Double), mainWS: WeightingScheme, secondaryWS: Option[WeightingScheme],
                     budget: Int, sourceCount: Long)
    extends ProgressiveGeospatialInterlinkingT {

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
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): WeightedPairsPQ = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        // the budget is divided based on the number of entities
        val k = (math.ceil(budget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0
        val sourcePQ: Array[WeightedPairsPQ] = new Array(source.length)
        val targetPQ: WeightedPairsPQ = WeightedPairsPQ(k)
        val partitionPQ: WeightedPairsPQ = WeightedPairsPQ(budget)

        target.indices
            .foreach{ j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach{ block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val e1 = source(i)
                                val w = getMainWeight(e1, e2)
                                val secW = getSecondaryWeight(e1, e2)
                                val wp = WeightedPair(i, j, w, secW)

                                // set top-K PQ for the examining target entity
                                targetPQ.enqueue(wp)

                                // update source entities' top-K
                                if (sourcePQ(i) == null)
                                    sourcePQ(i) = WeightedPairsPQ(k)
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

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, sws: Option[WeightingScheme] = None,
              budget: Int, partitioner: Partitioner): TopKPairs ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        TopKPairs(joinedRDD, thetaXY, ws, sws, budget, sourceCount)
    }
}
