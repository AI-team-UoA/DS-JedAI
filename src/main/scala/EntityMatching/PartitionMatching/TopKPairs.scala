package EntityMatching.PartitionMatching

import DataStructures.{IM, SpatialEntity}
import com.google.common.collect.MinMaxPriorityQueue
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils
import utils.Readers.SpatialReader

import scala.collection.mutable
import scala.collection.JavaConverters._

case class TopKPairs(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                     thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long) extends ProgressiveTrait {


    def getDE9IM: RDD[IM] = joinedRDD
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
        .flatMap { p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source: Array[SpatialEntity] = p._2._1.toArray
            val target: Array[SpatialEntity] = p._2._2.toArray
            val sourceIndex = index(source)
            val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

            val localBudget: Int = ((source.length*budget)/sourceCount).toInt
            val k = math.ceil(localBudget / (source.length + target.length)).toInt*2

            val orderingInt = Ordering.by[(Double, Int), Double](_._1).reverse
            val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse
            val sourceMinWeightPQ: Array[Double] = Array.fill(source.length)(1000d)
            val sourcePQ: Array[MinMaxPriorityQueue[(Double, Int)]] = Array.fill(source.length)(MinMaxPriorityQueue.orderedBy(orderingInt).maximumSize(k+1).create())
            val targetPQ: MinMaxPriorityQueue[(Double, Int)] = MinMaxPriorityQueue.orderedBy(orderingInt).maximumSize(k+1).create()
            var minW = 0d

            val partitionPQ: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget+1).create()
            var partitionMinWeight = 0d
            target
                .zipWithIndex
                .foreach { case (e2, j) =>
                    val frequencies = e2.index(thetaXY, filteringFunction)
                        .flatMap(c => sourceIndex.get(c))
                        .groupBy(identity)
                        .mapValues(_.length)

                    frequencies
                        .filter { case (i, _) => source(i).partitionRF(e2.mbb, thetaXY, partition) && source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES) }
                        .foreach { case (i, f) =>
                            val e1 = source(i)
                            val w = getWeight(f, e1, e2)

                            // set top-K for each target entity
                            if(minW < w) {
                                targetPQ.add((w, i))
                                if (targetPQ.size > k)
                                    minW = targetPQ.pollLast()._1
                            }

                            // update source entities' top-K
                            if(sourceMinWeightPQ(i) < w) {
                                targetPQ.add((w, i))
                                if (sourcePQ(i).size > k)
                                    sourceMinWeightPQ(i) = targetPQ.pollLast()._1
                            }
                        }

                    // add target's pairs in partition's PQ
                    if (!targetPQ.isEmpty) {
                        val w = Double.MaxValue
                        while (targetPQ.size > 0 && w > partitionMinWeight) {
                            val (w, i) = targetPQ.pollFirst()
                            if (partitionMinWeight < w){
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
                .filter(!_._1.isEmpty)
                .foreach { case (pq, i) =>
                    val w = Double.MaxValue
                    while (pq.size > 0 && w > partitionMinWeight) {
                        val (w, j) = pq.pollFirst()
                        if (partitionMinWeight < w) {
                            partitionPairs.get(i, j) match {
                                case Some(weight) if weight < w => partitionPairs.update((i, j), w)
                                case None => partitionPairs += ((i, j) -> w)
                            }
                        }
                   }
                }

            partitionMinWeight = 0d
            partitionPairs.takeWhile(_._2 > partitionMinWeight).foreach{ case(pair, w) =>
                partitionPQ.add((w, pair))
                if (partitionPQ.size() > localBudget)
                    partitionMinWeight = partitionPQ.pollLast()._1
            }
            partitionPQ.iterator().asScala.map{ case(_, (i, j)) =>  IM(source(i), target(j))}
        }

    def getWeightedDE9IM: RDD[(Double, IM)] = null
}

object TopKPairs{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption,
              ws: WeightStrategy, budget: Long): TopKPairs ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        TopKPairs(joinedRDD, thetaXY, ws, budget, sourceCount)
    }
}
