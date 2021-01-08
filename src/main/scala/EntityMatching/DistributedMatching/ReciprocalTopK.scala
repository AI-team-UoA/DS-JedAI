package EntityMatching.DistributedMatching

import DataStructures.{MBB, SpatialEntity}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.spark_project.guava.collect.MinMaxPriorityQueue
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable


case class ReciprocalTopK(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                     thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long) extends DMProgressiveTrait {


    def prioritize(source: Array[SpatialEntity], target: Array[SpatialEntity], partition: MBB, relation: Relation): MinMaxPriorityQueue[(Double, (Int, Int))] = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        val orderingInt = Ordering.by[(Double, Int), Double](_._1).reverse
        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse

        // initialize PQ and compute budget based on the n.o. intersecting targets
        // (avoid the entities that don't intersect, so we do not compute the top-k for those )
        val localBudget: Int = ((source.length*2 * budget) / sourceCount).toInt
        val k = (math.ceil(localBudget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0

        val sourceMinWeightPQ: Array[Double] = Array.fill(source.length)(0d)
        val sourcePQ: Array[MinMaxPriorityQueue[(Double, Int)]] = new Array(source.length)

        val targetPQ: mutable.PriorityQueue[(Double, Int)] = mutable.PriorityQueue()(Ordering.by[(Double, Int), Double](_._1).reverse)
        val targetSet: Array[mutable.HashSet[Int]] = Array.fill(target.length)(new mutable.HashSet[Int]())
        var minW = 0d

        val partitionPQ: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        var partitionMinWeight = 0d

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

                                // set top-K for each target entity
                                if (minW < w) {
                                    targetPQ.enqueue((w, i))
                                    if (targetPQ.size > localBudget)
                                        minW = targetPQ.dequeue()._1
                                }

                                // update source entities' top-K
                                if (sourceMinWeightPQ(i) == 0)
                                    sourcePQ(i) = MinMaxPriorityQueue.orderedBy(orderingInt).maximumSize(k + 1).create()
                                if (sourceMinWeightPQ(i) < w) {
                                    sourcePQ(i).add((w, j))
                                    if (sourcePQ(i).size > k)
                                        sourceMinWeightPQ(i) = sourcePQ(i).pollLast()._1
                                }
                            }
                    }

                while (targetPQ.nonEmpty) targetSet(j).add(targetPQ.dequeue()._2)
            }

        sourcePQ
            .zipWithIndex
            .filter(_._1 != null)
            .foreach { case (pq, i) =>
                val w = Double.MaxValue
                while (pq.size > 0 && w > partitionMinWeight) {
                    val (w, j) = pq.pollFirst()
                    if (targetSet(j).contains(i))
                        if (partitionMinWeight < w) {
                            partitionPQ.add(w, (i, j))
                            if (partitionPQ.size() > localBudget)
                                partitionMinWeight = partitionPQ.pollLast()._1

                        }
                }
            }
        partitionPQ
    }


}

object ReciprocalTopK{

    def apply(source:RDD[(Int, SpatialEntity)], target:RDD[(Int, SpatialEntity)], ws: WeightStrategy, budget: Long, partitioner: Partitioner): ReciprocalTopK ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        ReciprocalTopK(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
