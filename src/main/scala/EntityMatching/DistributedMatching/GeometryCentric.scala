package EntityMatching.DistributedMatching

import DataStructures.{MBB, Entity}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.spark_project.guava.collect.MinMaxPriorityQueue
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.JavaConverters._

case class GeometryCentric(joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))],
                           thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long)
   extends DMProgressiveTrait {


    /**
     * For each target entity we keep only the top K comparisons, according to a weighting scheme.
     * Then we assign to these top K comparisons, a common weight calculated based on the weights
     * of all the comparisons of the target entity. Based on this weight we prioritize their execution.
     *
     * @return  an RDD of Intersection Matrices
     */
    def prioritize(source: Array[Entity], target: Array[Entity], partition: MBB, relation: Relation): MinMaxPriorityQueue[(Double, (Int, Int))] = {
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)

        val orderingInt = Ordering.by[(Double, Int), Double](_._1).reverse
        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse

        val localBudget: Int = ((source.length * budget) / sourceCount).toInt
        val k = (math.ceil(localBudget / (source.length + target.length)).toInt + 1) * 2 // +1 to avoid k=0

        val targetPQ: MinMaxPriorityQueue[(Double, Int)] = MinMaxPriorityQueue.orderedBy(orderingInt).maximumSize(k + 1).create()
        var minW = 0d

        val partitionPQ: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        target
            .indices
            .foreach { j =>
                var wSum = 0d
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .foreach { block =>
                        sourceIndex.get(block)
                            .filter(i => source(i).filter(e2, relation, block, thetaXY, Some(partition)))
                            .foreach { i =>
                                val e1 = source(i)
                                val w = getWeight(e1, e2)
                                wSum += w

                                // set top-K for each target entity
                                if (minW < w) {
                                    targetPQ.add((w, i))
                                    if (targetPQ.size > k)
                                        minW = targetPQ.pollLast()._1
                                }
                            }
                    }
                if (! targetPQ.isEmpty) {
                    val weight = wSum / targetPQ.size()
                    val topK = targetPQ.iterator().asScala.map(_._2)
                    partitionPQ.addAll(topK.map(i => (weight, (i, j))).toList.reverse.asJava)
                    targetPQ.clear()
                }
            }
        partitionPQ
    }
}


object GeometryCentric{

    def apply(source:RDD[(Int, Entity)], target:RDD[(Int, Entity)], ws: WeightStrategy, budget: Long, partitioner: Partitioner)
    : GeometryCentric ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val joinedRDD = source.cogroup(target, partitioner)
        GeometryCentric(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}