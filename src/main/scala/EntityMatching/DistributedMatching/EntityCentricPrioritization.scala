package EntityMatching.DistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import utils.Constants.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class EntityCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                       thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long)
   extends DMProgressiveTrait {


    /**
     * For each target entity we keep only the top K comparisons, according to a weighting scheme.
     * Then we assign to these top K comparisons, a common weight calculated based on the weights
     * of all the comparisons of the target entity. Based on this weight we prioritize their execution.
     *
     * @return  an RDD of Intersection Matrices
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val sourceSize = source.length
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val entityPQ = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)
                val partitionPQ = mutable.PriorityQueue[(Double, (Iterator[Int], SpatialEntity))]()(Ordering.by[(Double, (Iterator[Int], SpatialEntity)), Double](_._1))

                val localBudget: Int = ((sourceSize*budget)/sourceCount).toInt
                val k = localBudget / p._2._2.size
                var minW = 10000d

                target
                    .foreach {e2 =>
                        val frequencies = e2.index(thetaXY, filteringFunction)
                            .flatMap(c => sourceIndex.get(c))
                            .groupBy(identity)
                            .mapValues(_.length)
                        var wSum = 0d
                        frequencies
                            .filter{ case(i, _) => source(i).partitionRF(e2.mbb, thetaXY, partition) && source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES) }
                            .foreach{ case(i, f) =>
                                val e1 = source(i)
                                val w = getWeight(f, e1, e2)
                                wSum += w
                                // keep the top-K for each target entity
                                if (entityPQ.size < k) {
                                    if(w < minW) minW = w
                                    entityPQ.enqueue((w, i))
                                }
                                else if(w > minW) {
                                    entityPQ.dequeue()
                                    entityPQ.enqueue((w, i))
                                    minW = entityPQ.head._1
                                }
                            }
                        if (entityPQ.nonEmpty) {
                            val weight = wSum / entityPQ.length
                            val topK = entityPQ.dequeueAll.map(_._2).reverse.toIterator
                            partitionPQ.enqueue((weight, (topK, e2)))
                            entityPQ.clear()
                        }
                    }

                partitionPQ.dequeueAll.map(_._2).flatMap{ case(sIndices, e2) => sIndices.map(i => IM(source(i), e2))}
            }
        }


    def getWeightedDE9IM: RDD[(Double, IM)] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val sourceSize = source.length
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val innerPQ = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)
                val partitionPQ = mutable.PriorityQueue[(Double, (Iterator[Int], SpatialEntity))]()(Ordering.by[(Double, (Iterator[Int], SpatialEntity)), Double](_._1))

                val localBudget: Int = ((sourceSize*budget)/sourceCount).toInt
                val k = localBudget / p._2._2.size
                var minW = 10000d
                target
                    .foreach {e2 =>
                        val sIndices:IndexedSeq[((Int, Int), ListBuffer[Int])] = e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))
                        val frequencies = sIndices.flatMap(_._2).groupBy(identity).mapValues(_.length)
                        var wSum = 0d
                        frequencies
                            .filter{ case(i, _) => source(i).partitionRF(e2.mbb, thetaXY, partition) && source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES) }
                            .foreach{ case(i, f) =>
                                val e1 = source(i)
                                val w = getWeight(f, e1, e2)
                                wSum += w
                                // keep the top-K for each target entity
                                if (innerPQ.size < k) {
                                    if(w < minW) minW = w
                                    innerPQ.enqueue((w, i))
                                }
                                else if(w > minW) {
                                    innerPQ.dequeue()
                                    innerPQ.enqueue((w, i))
                                    minW = innerPQ.head._1
                                }
                            }
                          if (innerPQ.nonEmpty) {
                            val weight = wSum / innerPQ.length
                            val topK = innerPQ.dequeueAll.map(_._2).reverse.toIterator
                            partitionPQ.enqueue((weight, (topK, e2)))
                            innerPQ.clear()
                        }
                    }

                partitionPQ.dequeueAll.flatMap{ case(w, (sIndices, e2)) => sIndices.map(i => (w, IM(source(i), e2)))}
            }
    }
}


object EntityCentricPrioritization{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], ws: WeightStrategy, budget: Long, partitioner: SpatialPartitioner)
    : EntityCentricPrioritization ={

        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val sourcePartitions = source.mapPartitions(seIter => Iterator((TaskContext.getPartitionId(), seIter.toIterable)))
        val targetPartitions = target.mapPartitions(seIter => Iterator((TaskContext.getPartitionId(), seIter.toIterable)))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, partitioner).map(p => (p._1, (p._2._1.flatten, p._2._2.flatten)))
        EntityCentricPrioritization(joinedRDD, thetaXY, ws, budget, sourceCount)
    }

}