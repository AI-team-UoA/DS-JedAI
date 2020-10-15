package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, SpatialEntity}
import com.google.common.collect.MinMaxPriorityQueue
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants._
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class ProgressiveLightRADON(source: RDD[SpatialEntity], target: Array[SpatialEntity], thetaXY: (Double, Double),
                                 sourceCount: Long, ws: WeightStrategy,  budget: Long) extends SDMTrait{

    /**
     * Prioritize the execution of comparisons in the partition based on their weight.
     * For each partition corresponds a fraction of the initial budget based on the number of geometries
     *
     * @param source Partition's Source
     * @param target Target (collected)
     * @param targetIndex targets index
     * @param relations the relation the MBBs must hold
     * @return a PQ of weighted comparisons
     */
    def compute(source: Array[SpatialEntity], target: Array[SpatialEntity], targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]],
                relations: Relation*): MinMaxPriorityQueue[(Double, (Int, Int))] ={

        // to avoid redundant comparisons
        val candidates = mutable.HashSet[Int]()
        val totalBlocks = targetIndex.keySet.size
        val localBudget: Int = math.ceil((source.length*budget)/sourceCount).toInt

        val orderingPair = Ordering.by[(Double, (Int, Int)), Double](_._1).reverse
        val pq: MinMaxPriorityQueue[(Double, (Int, Int))] = MinMaxPriorityQueue.orderedBy(orderingPair).maximumSize(localBudget + 1).create()
        var minW = 0d
        source.indices
            .foreach { i =>
                val coords = source(i).index(thetaXY)
                coords
                    .filter(targetIndex.contains)
                    .foreach { c =>
                        val targetEntities = targetIndex(c).filter(j => !candidates.contains(j))
                        candidates ++= targetEntities

                        targetEntities.foreach { j =>
                            val e2 = target(j)
                            if (relations.forall(r => source(i).testMBB(e2, r))) {
                                val e2Blocks = e2.index(thetaXY) // easier to re-index than to retrieve from targetIndexBD
                                val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                if (minW < w) {
                                    pq.add((w, (i, j)))
                                    if (pq.size > localBudget)
                                        minW = pq.pollLast()._1
                                }
                            }
                        }
                    }
                candidates.clear()
            }
        pq

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

            val pq = compute(source, target, targetIndexBD.value)
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
    def getDE9IM(targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM]= {
        val sc = SparkContext.getOrCreate()
        val targetIndexBD = sc.broadcast(targetIndex)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val source = sourceIter.toArray
            val target = targetBD.value

            val pq = compute(source, target, targetIndexBD.value)
            Iterator.continually {
                val (i, j) = pq.removeFirst()._2
                val e1 = source(i)
                val e2 = targetBD.value(j)
                IM(e1, e2)
            }.takeWhile(_ => !pq.isEmpty)
        }
    }

}



object ProgressiveLightRADON {
    /**
     * Auxiliary constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], ws: WeightStrategy = WeightStrategy.CBS, budget: Long): ProgressiveLightRADON = {
        val thetaXY = Utils.getTheta
        val count = Utils.sourceCount
        ProgressiveLightRADON(source, target.collect(), thetaXY, count, ws, budget)
    }
}
