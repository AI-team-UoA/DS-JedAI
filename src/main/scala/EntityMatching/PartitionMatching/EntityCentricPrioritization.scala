package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}
import utils.Readers.SpatialReader

import scala.collection.mutable

case class EntityCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                       thetaXY: (Double, Double), weightingScheme: String, targetCount: Long)
    extends PartitionMatchingTrait{


    def apply(relation: String): RDD[(String, String)] = {
        val comparisons = takeBudget(relation, Utils.targetCount*Utils.sourceCount)
        comparisons.filter(_._2).map(_._1)
    }

    /**
     * Get the first budget comparisons, and finds the marches in them.
     *
     * @param relation examined relation
     * @param budget the no comparisons will be implemented
     * @return the number of matches the relation holds
     */
    override def applyWithBudget(relation: String, budget: Int): Long = {
        val comparisons = takeBudget(relation, Utils.targetCount*Utils.sourceCount)
        comparisons.take(budget).count(_._2)
    }


    /**
     * Similar to the ComparisonCentric, but instead of executing all the comparisons of target,
     * it select the top-k, where k is based on the input budget.
     *      *
     * @param relation the examining relation
     * @param budget the number of comparisons to implement
     * @return  an RDD of pair of IDs and boolean that indicate if the relation holds
     */
    def takeBudget(relation: String, budget: Long): RDD[((String, String), Boolean)] ={
        val k = budget / targetCount
        joinedRDD
            .flatMap { p =>
                val partitionId = p._1
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Array[SpatialEntity] = p._2._2.toArray  //WARNING: this will result to OOM
                val sourceIndex = index(source, partitionId)
                val sourceSize = source.length
                val targetsCoords = target.zipWithIndex.map { case (e2, i) =>
                    val coords = e2.index(thetaXY, (b:(Int, Int)) => zoneCheck(partitionId)(b) && sourceIndex.contains(b))
                    (i, coords)
                }

                val comparisons = for ((j, coords) <- targetsCoords.filter(_._2.nonEmpty)) yield {
                    val e2: SpatialEntity = target(j)

                    val sIndicesList = coords.map(c => sourceIndex.get(c))
                    val frequency = Array.fill(sourceSize)(0)
                    sIndicesList.flatten.foreach(i => frequency(i) += 1)

                    var wSum = 0d
                    val pq = mutable.PriorityQueue[(Double, Int)]()(Ordering.by[(Double, Int), Double](_._1).reverse)
                    for (k <- sIndicesList.indices) {
                        val c = coords(k)
                        val sIndices = sIndicesList(k).filter(i => source(i).mbb.referencePointFiltering(e2.mbb, c, thetaXY))
                        for (i <- sIndices) {
                            val e1 = source(i)
                            val f = frequency(i)
                            val w = getWeight(f, e1, e2)
                            wSum += w
                            pq.enqueue((w, i))
                        }
                    }
                    val weight = if (pq.nonEmpty) wSum / pq.length else -1d
                    val sz = if (k < pq.length) k.toInt else pq.length
                    val weightedComparisons = for (_ <- 0 until sz) yield pq.dequeue()
                    val topComparisons = weightedComparisons.map(_._2).map(i => (source(i), e2))
                    (weight, topComparisons)
                }
                comparisons.filter(_._1 >= 0d)
            }

            // sort the comparisons based on their mean weight
            .sortByKey(ascending = false)
            .flatMap(_._2)
            .map(c => ((c._1.originalID, c._2.originalID), c._1.mbb.testMBB(c._2.mbb, relation) && c._1.relate(c._2, relation)))
    }


}

object EntityCentricPrioritization{


    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String = Constants.CBS)
    : EntityCentricPrioritization ={

        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val targetCount = Utils.targetCount
        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        EntityCentricPrioritization(joinedRDD, thetaXY, weightingScheme, targetCount)
    }

}