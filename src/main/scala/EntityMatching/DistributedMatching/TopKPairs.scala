package EntityMatching.DistributedMatching

import DataStructures.{MBB, SpatialEntity}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.spark_project.guava.collect.MinMaxPriorityQueue
import utils.Constants.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable

case class TopKPairs(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                     thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long) extends DMProgressiveTrait {


    def prioritize(source: Array[SpatialEntity], target: Array[SpatialEntity], partition: MBB): MinMaxPriorityQueue[(Double, (Int, Int))] = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val filterRedundantComparisons = (i: Int, j: Int) => source(i).partitionRF(target(j).mbb, thetaXY, partition) &&
            source(i).testMBB(target(j), Relation.INTERSECTS)

        val orderingInt = Ordering.by[(Double, Int), Double](_._1).reverse
        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse

        // initialize PQ and compute budget based on the n.o. intersecting targets
        // (avoid the entities that don't intersect, so we do not compute the top-k for those )
        val localBudget: Int = ((source.length*2 * budget) / sourceCount).toInt
        val k = (math.ceil(localBudget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0

        val sourceMinWeightPQ: Array[Double] = Array.fill(source.length)(0d)
        val sourcePQ: Array[MinMaxPriorityQueue[(Double, Int)]] = new Array(source.length)

        val targetPQ: MinMaxPriorityQueue[(Double, Int)] = MinMaxPriorityQueue.orderedBy(orderingInt).maximumSize(k + 1).create()
        var minW = 0d

        val partitionPQ: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        var partitionMinWeight = 0d

        target.indices
                .foreach{j =>
                    val e2 = target(j)
                    e2.index(thetaXY, filterIndices)
                        .foreach{ c =>
                            sourceIndex.get(c)
                                .filter(i => filterRedundantComparisons(i, j))
                                .foreach { i =>
                                    val e1 = source(i)
                                    val w = getWeight(e1, e2)

                                    // set top-K for each target entity
                                    if (minW < w) {
                                        targetPQ.add((w, i))
                                        if (targetPQ.size > k)
                                            minW = targetPQ.pollLast()._1
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

                // add target's pairs in partition's PQ
                if (!targetPQ.isEmpty) {
                    val w = Double.MaxValue
                    while (targetPQ.size > 0 && w > partitionMinWeight) {
                        val (w, i) = targetPQ.pollFirst()
                        if (partitionMinWeight < w) {
                            partitionPQ.add((w, (i, j)))
                            if (partitionPQ.size() > localBudget)
                                partitionMinWeight = partitionPQ.pollLast()._1
                        }
                    }
                }
                targetPQ.clear()
                minW = 0d
            }

        // putting pairs in hasMap to avoid duplicates
        val partitionPairs: mutable.HashMap[(Int, Int), Double] = mutable.HashMap()
        partitionPQ.iterator().asScala.foreach{ case(w:Double, pair:(Int, Int)) => partitionPairs += (pair -> w) }

        // adding source entities' top-K in hashMap
        sourcePQ
            .zipWithIndex
            .filter(_._1 != null)
            .foreach { case (pq, i) =>
                val w = Double.MaxValue
                while (pq.size > 0 && w > partitionMinWeight) {
                    val (w, j) = pq.pollFirst()
                    if (partitionMinWeight < w) {
                        partitionPairs.get(i, j) match {
                            case Some(weight) if weight < w => partitionPairs.update((i, j), w) //if exist with smaller weight -> update
                            case None => partitionPairs += ((i, j) -> w)
                            case _ =>
                        }
                    }
                }
            }

        // keep partitions top comparisons
        partitionMinWeight = 0d
        partitionPairs.takeWhile(_._2 > partitionMinWeight).foreach{ case(pair, w) =>
            partitionPQ.add((w, pair))
            if (partitionPQ.size() > localBudget)
                partitionMinWeight = partitionPQ.pollLast()._1
        }
        partitionPQ
    }

}

object TopKPairs{

    def apply(source:RDD[(Int, SpatialEntity)], target:RDD[(Int, SpatialEntity)], ws: WeightStrategy, budget: Long, partitioner: Partitioner): TopKPairs ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        TopKPairs(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
