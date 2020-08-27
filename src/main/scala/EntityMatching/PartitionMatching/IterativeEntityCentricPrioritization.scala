package EntityMatching.PartitionMatching

import java.util

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils
import utils.Readers.SpatialReader

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class IterativeEntityCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                           thetaXY: (Double, Double), ws: WeightStrategy, targetCount: Long, budget: Long) extends ProgressiveTrait {


    /**
     * Similar to the ComparisonCentric, but instead of executing all the comparisons of target,
     * it just select the top-k which k is based on the input budget.
     * Then gives a weight to the entities of the target, based on the weights of their comparisons,
     * and sorts based on that.
     *
     * @param relation the examining relation
     * @return  an RDD of pair of IDs and boolean that indicate if the relation holds
     */
    def getComparisons(relation: Relation): RDD[((String, String), Boolean)] = {
        // WARNING: we take K from each target, what if a se has fewer than K candidates?
        val k = budget / targetCount
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val sourceSize = source.length
                val frequencies = new Array[Int](sourceSize)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val pq = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)

                target
                    .map {e2 =>
                        val sIndices:Array[((Int, Int), ArrayBuffer[Int])] = e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))
                        util.Arrays.fill(frequencies, 0)
                        sIndices.flatMap(_._2)foreach(i => frequencies(i) += 1)

                        var wSum = 0d
                        sIndices
                            .flatMap{ case (c, indices) => indices.filter(i => source(i).referencePointFiltering(e2, c, thetaXY, Some(partition)))}
                            .foreach { i =>
                                val e1 = source(i)
                                val f = frequencies(i)
                                val w = getWeight(f, e1, e2)
                                wSum += w
                                pq.enqueue((w, i))
                            }
                        val weight = if (pq.nonEmpty) wSum / pq.length else -1d
                        val sz = if (k < pq.length) k.toInt else pq.length

                        val weightedComparisons = for (_ <- 0 until sz) yield pq.dequeue()
                        pq.clear()
                        val topComparisons = weightedComparisons.map(_._2).map(i => (source(i), e2))
                        (weight, topComparisons.toIterator)
                    }
                    .filter(_._1 >= 0d)
            }
            // sort the comparisons based on their mean weight
            .sortByKey(ascending = false)
            .map(p => p._2)
            .mapPartitions { comparisonsIter: Iterator[Iterator[(SpatialEntity, SpatialEntity)]] =>
                var pairs: List[((String, String), Boolean)] = List()

                var converge: Boolean = false
                while (!converge) {
                    converge = true
                    for (comparisonIter <- comparisonsIter) {
                        if (comparisonIter.hasNext) {
                            converge = false
                            val (e1, e2) = comparisonIter.next()
                            val isMatch = e1.testMBB(e2, relation) && e1.relate(e2, relation)
                            pairs = pairs :+ ((e1.originalID, e2.originalID), isMatch)
                        }
                    }
                }
                pairs.toIterator
            }
    }

    def getDE9IM: RDD[IM] = {

        val k = budget / targetCount
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val sourceSize = source.length
                val frequencies = new Array[Int](sourceSize)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val pq = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)

                target
                    .map {e2 =>
                        val sIndices:Array[((Int, Int), ArrayBuffer[Int])] = e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))
                        util.Arrays.fill(frequencies, 0)
                        sIndices.flatMap(_._2)foreach(i => frequencies(i) += 1)

                        var wSum = 0d
                        sIndices
                            .flatMap{ case (c, indices) => indices.filter(i => source(i).referencePointFiltering(e2, c, thetaXY, Some(partition)))}
                            .foreach { i =>
                                val e1 = source(i)
                                val f = frequencies(i)
                                val w = getWeight(f, e1, e2)
                                wSum += w
                                pq.enqueue((w, i))
                            }
                        val weight = if (pq.nonEmpty) wSum / pq.length else -1d
                        val sz = if (k < pq.length) k.toInt else pq.length

                        val weightedComparisons = for (_ <- 0 until sz) yield pq.dequeue()
                        pq.clear()
                        val topIM = weightedComparisons
                            .map(_._2)
                            .filter(i => source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES))
                            .map(i => IM(source(i), e2))
                        (weight, topIM.toIterator)
                    }
                    .filter(_._1 >= 0d)
            }
            // sort the comparisons based on their mean weight
            .sortByKey(ascending = false)
            .map(p => p._2)
            .mapPartitions { imIterIter: Iterator[Iterator[IM]] =>
                var pairs: List[IM] = List()

                var converge: Boolean = false
                while (!converge) {
                    converge = true
                    for (imIter <- imIterIter) {
                        if (imIter.hasNext) {
                            converge = false
                            val im = imIter.next()
                            pairs = pairs :+ im
                        }
                    }
                }
                pairs.toIterator
            }
    }

}

object IterativeEntityCentricPrioritization{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption,
              ws: WeightStrategy, budget: Long): IterativeEntityCentricPrioritization ={
        val thetaXY = Utils.getTheta
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val targetCount = Utils.targetCount
        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        IterativeEntityCentricPrioritization(joinedRDD, thetaXY, ws, targetCount, budget)
    }

}
