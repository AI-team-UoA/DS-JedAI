package EntityMatching.DistributedMatching

import DataStructures.{ComparisonPQ, Entity, MBR}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils


import scala.collection.mutable

case class TopKPairs(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                     thetaXY: (Double, Double), ws: WeightStrategy, budget: Int, sourceCount: Long) extends DMProgressiveTrait {


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
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBR, relation: Relation): ComparisonPQ[(Int, Int)] = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        // the budget is divided based on the number of entities
        val k = (math.ceil(budget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0
        val sourcePQ: Array[ComparisonPQ[Int]] = new Array(source.length)
        val targetPQ: ComparisonPQ[Int] = ComparisonPQ[Int](k)
        val partitionPQ: ComparisonPQ[(Int, Int)] = ComparisonPQ[(Int, Int)](budget)

        target.indices
                .foreach{ j =>
                    val e2 = target(j)
                    e2.index(thetaXY, filterIndices)
                        .foreach{ block =>
                            sourceIndex.get(block)
                                .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                                .foreach { i =>
                                    val e1 = source(i)
                                    val w = getWeight(e1, e2)

                                    // set top-K PQ for the examining target entity
                                    targetPQ.enqueue(w, i)

                                    // update source entities' top-K
                                    if (sourcePQ(i) == null)
                                        sourcePQ(i) = ComparisonPQ[Int](k)
                                    sourcePQ(i).enqueue(w, j)
                            }
                        }

                // add target's pairs in partition's PQ
                if (!targetPQ.isEmpty) {
                    val w = Double.MaxValue
                    while (targetPQ.size > 0 && w > partitionPQ.minW) {
                        val (w, i) = targetPQ.dequeueHead()
                        partitionPQ.enqueue(w, (i, j))
                    }
                }
                targetPQ.clear()
            }

        // putting target comparisons in a HasMap. Source entities will also be added in the HashMap
        // to update wights and avoid duplicate comparisons
        val partitionPairs: mutable.HashMap[(Int, Int), Double] = mutable.HashMap()
        partitionPQ.iterator().foreach{ case(w:Double, pair:(Int, Int)) => partitionPairs += (pair -> w) }

        // adding source entities' top-K in hashMap
        sourcePQ
            .zipWithIndex
            .filter(_._1 != null)
            .foreach { case (pq, i) =>
                val w = Double.MaxValue
                while (pq.size > 0 && w > partitionPQ.minW) {
                    val (w, j) = pq.dequeueHead()
                    if (partitionPQ.minW < w) {
                        partitionPairs.get(i, j) match {
                            case Some(weight) if weight < w => partitionPairs.update((i, j), w) //if exist with smaller weight -> update
                            case None => partitionPairs += ((i, j) -> w)
                            case _ =>
                        }
                    }
                }
                pq.clear()
            }

        // keep partition's top comparisons
        partitionPQ.clear()
        partitionPQ.enqueueAll(partitionPairs.toIterator)
        partitionPQ
    }
}

object TopKPairs{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightStrategy, budget: Int, partitioner: Partitioner): TopKPairs ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        TopKPairs(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
