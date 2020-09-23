package EntityMatching.PartitionMatching


import java.util

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils
import utils.Readers.SpatialReader

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class ComparisonCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                           thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long) extends ProgressiveTrait {

    /**
     * First index source and then for each entity of target, find its comparisons from source's index.
     * Weight the comparisons according to the weighting scheme and sort them using a PQ. Calculate the
     * DE9IM in a descending order.
     * From each partition we calculate just a portion of the total comparisons, based on the budget and the size
     * of the partition.
     *
     * @return  an RDD of Intersection Matrices
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

                val localBudget: Int = ((source.length*budget)/sourceCount).toInt
                val pq = mutable.PriorityQueue()(Ordering.by[(Double, (Int, SpatialEntity)), Double](_._1).reverse)
                var minW = 10000d

                // weight and put the comparisons in a PQ
                target
                    .map(e2 => (e2, e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))))
                    .filter(_._2.length > 0)
                    .foreach { case(e2: SpatialEntity, sIndices:  Array[((Int, Int), ListBuffer[Int])]) =>

                        val frequencies = sIndices.flatMap(_._2).groupBy(identity).mapValues(_.length)
                        frequencies
                            .filter{ case(i, _) => source(i).partitionRF(e2.mbb, thetaXY, partition) && source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES) }
                            .foreach{ case(i, f) =>
                                val e1 = source(i)
                                val w =  getWeight(f, e1, e2)
                                if(pq.size < localBudget) {
                                    if (w < minW) minW = w
                                    pq.enqueue((w, (i, e2)))
                                } else if (w > minW) {
                                    pq.dequeue()
                                    pq.enqueue((w, (i, e2)))
                                    minW = pq.head._1
                                }
                            }
                        }
               pq.dequeueAll.map{case (_, (i, e2)) => IM(source(i), e2)}.reverse
            }
    }


    def getWeightedDE9IM: RDD[(Double, IM)] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

                val localBudget: Int = ((source.length*budget)/sourceCount).toInt
                val pq = mutable.PriorityQueue()(Ordering.by[(Double, (Int, SpatialEntity)), Double](_._1).reverse)
                var minW = 10000d

                // weight and put the comparisons in a PQ
                target
                    .map(e2 => (e2, e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))))
                    .filter(_._2.length > 0)
                    .foreach { case(e2: SpatialEntity, sIndices:  Array[((Int, Int), ListBuffer[Int])]) =>

                        val frequencies = sIndices.flatMap(_._2).groupBy(identity).mapValues(_.length)
                        frequencies
                            .filter{ case(i, _) => source(i).partitionRF(e2.mbb, thetaXY, partition) && source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES) }
                            .foreach{ case(i, f) =>
                                val e1 = source(i)
                                val w =  getWeight(f, e1, e2)
                                if(pq.size < localBudget) {
                                    if (w < minW) minW = w
                                    pq.enqueue((w, (i, e2)))
                                } else if (w > minW) {
                                    pq.dequeue()
                                    pq.enqueue((w, (i, e2)))
                                    minW = pq.head._1
                                }
                            }
                    }

                pq.dequeueAll.map{case (w, (i, e2)) => (w, IM(source(i), e2))}.reverse
            }
    }
}



/**
 * auxiliary constructor
 */
object ComparisonCentricPrioritization {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption, ws: WeightStrategy,
              budget: Long): ComparisonCentricPrioritization ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        ComparisonCentricPrioritization(joinedRDD, thetaXY, ws, budget, sourceCount)
    }

}
