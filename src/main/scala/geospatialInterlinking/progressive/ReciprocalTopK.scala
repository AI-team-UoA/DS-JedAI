package geospatialInterlinking.progressive

import dataModel.{ComparisonPQ, Entity, MBR}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightingScheme.WeightingScheme
import utils.Utils



case class ReciprocalTopK(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                          thetaXY: (Double, Double), ws: WeightingScheme, budget: Int, sourceCount: Long) extends ProgressiveGeospatialInterlinkingT {

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
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): ComparisonPQ[(Int, Int)] = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        val sourceK = (math.ceil(budget / source.length).toInt + 1) * 2 // +1 to avoid k=0
        val targetK = (math.ceil(budget / target.length).toInt + 1) * 2 // +1 to avoid k=0

        val sourcePQ: Array[ComparisonPQ[Int]] = new Array(source.length)
        val targetPQ: ComparisonPQ[Int] = ComparisonPQ[Int](targetK)
        val partitionPQ: ComparisonPQ[(Int, Int)] = ComparisonPQ[(Int, Int)](budget)

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
                                val w = getWeight(e1, e2)

                                // set top-K PQ for the examining target entity
                                targetPQ.enqueue(w, i)

                                // update source entities' top-K
                                if (sourcePQ(i) == null)
                                    sourcePQ(i) = ComparisonPQ[Int](sourceK)
                                sourcePQ(i).enqueue(w, j)
                            }
                    }
                // add comparisons into corresponding HashSet
                targetSet(j) = targetPQ.iterator().map(_._2).toSet
                targetPQ.clear()
            }

        // add comparison into PQ only if is contained by both top-K PQs
        sourcePQ
            .zipWithIndex
            .filter(_._1 != null)
            .foreach { case (pq, i) =>
                val w = Double.MaxValue
                while (pq.size > 0 && w > partitionPQ.minW) {
                    val (w, j) = pq.dequeueHead()
                    if (targetSet(j).contains(i))
                        partitionPQ.enqueue(w, (i, j))
                }
            }
        partitionPQ
    }
}

object ReciprocalTopK{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightingScheme, budget: Int, partitioner: Partitioner): ReciprocalTopK ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        ReciprocalTopK(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
