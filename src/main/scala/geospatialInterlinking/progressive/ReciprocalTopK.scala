package geospatialInterlinking.progressive

import dataModel.{Entity, MBR, WeightedPair, WeightedPairsPQ}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils



case class ReciprocalTopK(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))], thetaXY: (Double, Double),
                          mainWS: WeightingScheme, secondaryWS: Option[WeightingScheme], budget: Int)
    extends ProgressiveGeospatialInterlinkingT {

    /**
     * Find the top-K comparisons of target and source and keep only the comparison (i, j) that belongs to both
     * top-K comparisons of i and j.
     *
     * @param source source dataset
     * @param target target dataset
     * @param partition current partition
     * @param relation examining relation
     * @return prioritized comparisons as a PQ
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation):  WeightedPairsPQ = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        val sourceK = (math.ceil(budget / source.length).toInt + 1) * 2 // +1 to avoid k=0
        val targetK = (math.ceil(budget / target.length).toInt + 1) * 2 // +1 to avoid k=0

        val sourcePQ: Array[WeightedPairsPQ] = new Array(source.length)
        val targetPQ: WeightedPairsPQ = WeightedPairsPQ(targetK)
        val partitionPQ: WeightedPairsPQ = WeightedPairsPQ(budget)

        val targetSet: Array[Set[Int]] = new Array(target.length)
        target.indices
            .foreach{j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val e1 = source(i)
                                val w = getMainWeight(e1, e2)
                                val secW = getSecondaryWeight(e1, e2)
                                val wp = WeightedPair( i, j, w, secW)

                                // set top-K PQ for the examining target entity
                                targetPQ.enqueue(wp)

                                // update source entities' top-K
                                if (sourcePQ(i) == null)
                                    sourcePQ(i) = WeightedPairsPQ(sourceK)
                                sourcePQ(i).enqueue(wp)
                            }
                    }
                // add comparisons into corresponding HashSet
                targetSet(j) = targetPQ.iterator().map(_.entityId1).toSet
                targetPQ.clear()
            }

        // add comparison into PQ only if is contained by both top-K PQs
        sourcePQ
            .filter(_ != null)
            .foreach { pq =>
                pq.iterator()
                    .filter(wp => targetSet(wp.entityId2).contains(wp.entityId1))
                    .foreach(wp => partitionPQ.enqueue(wp))
            }
        partitionPQ
    }
}

object ReciprocalTopK{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, sws: Option[WeightingScheme] = None,
              budget: Int, partitioner: Partitioner): ReciprocalTopK ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        ReciprocalTopK(joinedRDD, thetaXY, ws, sws, budget)
    }
}
