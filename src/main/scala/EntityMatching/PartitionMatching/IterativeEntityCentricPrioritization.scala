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

case class IterativeEntityCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                           thetaXY: (Double, Double), ws: WeightStrategy, targetCount: Long, budget: Long) extends ProgressiveTrait {


    /**
     * Similar to the EntityCentric, but in the end it iterates over the selected comparisons
     * of all target entities.
     *
     * @return  an RDD of pair of IDs and boolean that indicate if the relation holds
     */
    def getDE9IM: RDD[IM] ={
        val totalComparisons: Long = joinedRDD.map(p => p._2._1.size * p._2._2.size).sum().toLong
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterable[SpatialEntity] = p._2._2
                val sourceIndex = index(source)
                val sourceSize = source.length
                val frequencies = new Array[Int](sourceSize)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val innerPQ = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)
                val partitionPQ = mutable.PriorityQueue[(Double, (Iterator[Int], SpatialEntity))]()(Ordering.by[(Double, (Iterator[Int], SpatialEntity)), Double](_._1))

                val localCartesian = target.size*source.length
                val localBudget: Int = ((localCartesian*budget)/totalComparisons).toInt
                val k = localBudget / p._2._2.size
                var minW = 10000d
                target
                    .toIterator
                    .foreach {e2 =>
                        val sIndices:Array[((Int, Int), ListBuffer[Int])] = e2.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))
                        util.Arrays.fill(frequencies, 0)
                        sIndices.flatMap(_._2)foreach(i => frequencies(i) += 1)

                        var wSum = 0d
                        sIndices
                            .flatMap{ case (c, indices) => indices.filter(i => source(i).referencePointFiltering(e2, c, thetaXY, Some(partition)) &&
                                source(i).testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES))}
                            .foreach { i =>
                                val e1 = source(i)
                                val f = frequencies(i)
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

                val comparisons = partitionPQ.dequeueAll.map(_._2).toArray
                var converge = false
                val imList: ListBuffer[IM] = ListBuffer[IM]()
                while(!converge){
                    converge = true
                    for ((sIndices, e2) <- comparisons) {
                        if (sIndices.hasNext) {
                            converge = false
                            val e1 = source(sIndices.next())
                            val im = IM(e1, e2)
                            imList.append(im)
                        }
                    }
                }
                imList.toIterator
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
