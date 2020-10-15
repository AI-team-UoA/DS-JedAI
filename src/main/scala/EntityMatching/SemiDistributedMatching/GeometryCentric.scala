package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, SpatialEntity}
import com.google.common.collect.MinMaxPriorityQueue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.{Relation, WeightStrategy}
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer



case class GeometryCentric(source: RDD[SpatialEntity], target: Array[SpatialEntity], thetaXY: (Double, Double),
                           sourceCount: Long, ws: WeightStrategy, budget: Long) extends SDMTrait {

    /**
     * Find the top-K comparisons of Source's geometries, and execute them
     * in a Descending way.
     *
     * For each geometry of Source corresponds K comparisons based on the local budget of the partition
     * The local budget is defined by the size of the partition regarding the overall size of the dataset.
     * We use a PQ to find the top-K comparisons of a geometry, and a different PQ to sort the
     * comparisons of the overall partition.
     * WARNING: Some geometries have less than K comparisons, and hence the actual budget is fewer than the
     *  requested one!
     *
     * @param source Partition's Source entities
     * @param target All Target Entities
     * @param targetIndex target Index
     * @param relations examined relation
     * @return a PQ with the comparisons of the overall partition
     */
    def compute(source: Array[SpatialEntity], target: Array[SpatialEntity], targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]],
                relations: Relation*): MinMaxPriorityQueue[(Double, (Int, Int))]={

        // estimate local budget and K
        val totalBlocks = targetIndex.keySet.size
        val localBudget: Int = ((source.length * budget) / sourceCount).toInt
        val k = (math.ceil(localBudget / source.length).toInt + 1) * 2 // +1 to avoid k=0

        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse

        // where top-K will be stored
        val pq: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(k + 1).create()
        var minW = 0d

        // Partitions top comparisons
        val partitionPQ: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        var partitionMinWeight = 0d

        // to avoid redundant comparisons
        val candidates = mutable.HashSet[Int]()

        // for each source find its top-K comparisons
        source.indices
            .foreach { i =>
                val e1 = source(i)
                val coords = e1.index(thetaXY, targetIndex.contains)
                coords
                    .foreach { c =>
                        val targetEntities = targetIndex(c).filter(j => !candidates.contains(j))
                        candidates ++= targetEntities

                        targetEntities
                            .filter(j => relations.forall(r => e1.testMBB(target(j), r)))
                            .foreach { j =>
                                val e2 = target(j)
                                val e2Blocks = e2.index(thetaXY)
                                val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                if (minW < w) {
                                    pq.add((w, (i, j)))
                                    if (pq.size > k)
                                        minW = pq.pollLast()._1
                                }
                            }
                    }

                // add comparisons in partition's PQ
                if (!pq.isEmpty) {
                    val w = Double.MaxValue
                    while (pq.size > 0 && w > partitionMinWeight) {
                        val (w, (i, j)) = pq.pollFirst()
                        if (partitionMinWeight < w) {
                            partitionPQ.add((w, (i, j)))
                            if (partitionPQ.size() > localBudget)
                                partitionMinWeight = partitionPQ.pollLast()._1
                        }
                    }
                }
                candidates.clear()
                pq.clear()
                minW = 0d
            }
        partitionPQ
    }


    /**
     * Find the pairs which the relation holds
     * @param relation the examined relation
     * @param targetIndex target index
     * @return the matching pairs
     */
    def matchTarget(relation: Relation, targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {

        val sc = SparkContext.getOrCreate()
        val targetIndexBD = sc.broadcast(targetIndex)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>

            val source = sourceIter.toArray
            val target = targetBD.value
            val pq = compute(source, target, targetIndexBD.value, relation)

            Iterator.continually{pq.removeFirst()._2}
                .filter{case (i,j) => source(i).relate(target(j), relation)}
                .map{ case(i, j) => (source(i).originalID, target(j).originalID)}
                .takeWhile(_ => !pq.isEmpty)
        }
    }


    /**
     * Find De-9IM
     * @param targetIndex targets index
     * @return an RDD of IM
     */
    def getDE9IM(targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM] = {

        val sc = SparkContext.getOrCreate()
        val targetIndexBD = sc.broadcast(targetIndex)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val source = sourceIter.toArray
            val pq = compute(source, targetBD.value, targetIndexBD.value, Relation.INTERSECTS, Relation.TOUCHES)

            Iterator.continually{
                val (i, j) = pq.removeFirst()._2
                val e1 = source(i)
                val e2 = targetBD.value(j)
                IM(e1, e2)
            }.takeWhile(_ => !pq.isEmpty)
        }
    }

}


object GeometryCentric {
    /**
     * Constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], ws: WeightStrategy = WeightStrategy.CBS,  budget: Long): GeometryCentric = {
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.sourceCount
        GeometryCentric(source, target.collect(), thetaXY, sourceCount, ws, budget)
    }
}
