package EntityMatching.DistributedMatching


import DataStructures.{IM, MBB, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.spark_project.guava.collect.MinMaxPriorityQueue
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable


case class ProgressiveGIAnt(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                            thetaXY: (Double, Double), ws: WeightStrategy, budget: Long, sourceCount: Long) extends DMProgressiveTrait {


    /**
     * First index source and then for each entity of target, find its comparisons using source's index.
     * Weight the comparisons according to the weighting scheme and sort them using a PQ. Calculate the
     * DE9IM in a descending order.
     * From each partition we calculate just a portion of the total comparisons, based on the budget and the size
     * of the partition.
     *
     * @param partition the MBB of the partition
     * @param source source
     * @param target target
     * @param relations the examined relations
     * @return a PQ with the top comparisons
     */
    private def compute(partition: MBB, source: Array[SpatialEntity], target: Array[SpatialEntity], relations: Relation*): MinMaxPriorityQueue[(Double, (Int, Int))] ={
        val sourceIndex = index(source)
        val filterIndices = (b: (Int, Int)) => sourceIndex.contains(b)
        val filterRedundantComparisons = (i: Int, j: Int) => source(i).partitionRF(target(j).mbb, thetaXY, partition) &&
            source(i).testMBB(target(j), Relation.INTERSECTS, Relation.TOUCHES)

        val localBudget: Int = math.ceil((source.length*budget)/sourceCount).toInt
        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse
        val pq: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        var minW = 0d

        // to store #common blocks of a target entity with all the entities of Source
        val frequencies: mutable.HashMap[Int, Int] = mutable.HashMap()
        // the rejected blocks - useful for avoiding re-examining entities
        val rejected: mutable.HashSet[Int] = mutable.HashSet()
        // weight and put the comparisons in a PQ
        target
            .indices
            .foreach {j =>
                val e2 = target(j)
                e2.index(thetaXY, filterIndices)
                    .view
                    .flatMap(c => sourceIndex.get(c))
                    .foreach{ i =>
                        if (frequencies.contains(i)) frequencies(i) += 1
                        else{
                            if (!rejected.contains(i))
                                if (filterRedundantComparisons(i, j)) frequencies += i -> 1
                                else rejected += i
                        }
                    }
                rejected.clear()
                frequencies
                    .foreach{ case(i, f) =>
                        val e1 = source(i)
                        val w =  getWeight(f, e1, e2)
                        if (minW < w) {
                            pq.add((w, (i, j)))
                            if (pq.size > localBudget)
                                minW = pq.pollLast()._1
                        }
                    }
                frequencies.clear()
            }
        pq
    }

    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting strategy
     * @return an RDD of IM
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD.flatMap{ p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source = p._2._1.toArray
            val target = p._2._2.toArray

            val pq = compute(partition, source, target, Relation.INTERSECTS, Relation.TOUCHES)
            if (!pq.isEmpty)
                Iterator.continually {
                    val (i, j) = pq.removeFirst()._2
                    val e1 = source(i)
                    val e2 = target(j)
                    IM(e1, e2)
                }.takeWhile(_ => !pq.isEmpty)
            else Iterator()
        }
    }


    /**
     * Get the DE-9IM of the top most related entities based
     * on the input budget and the Weighting strategy
     *
     * @return RDD of weighted IM
     */
    def getWeightedDE9IM: RDD[(Double, IM)] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source = p._2._1.toArray
                val target = p._2._2.toArray

                val pq = compute(partition, source, target, Relation.INTERSECTS, Relation.TOUCHES)
                if (!pq.isEmpty)
                    Iterator.continually {
                        val (w, (i, j)) = pq.removeFirst()
                        val e1 = source(i)
                        val e2 = target(j)
                        (w, IM(e1, e2))
                    }.takeWhile(_ => !pq.isEmpty)
                else Iterator()
            }
    }
}



/**
 * auxiliary constructor
 */
object ProgressiveGIAnt {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], ws: WeightStrategy, budget: Long, partitioner: SpatialPartitioner): ProgressiveGIAnt ={
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.getSourceCount
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, partitioner)
        ProgressiveGIAnt(joinedRDD, thetaXY, ws, budget, sourceCount)
    }

}
