package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}
import utils.Readers.SpatialReader

import scala.collection.mutable

case class EntityCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                       thetaXY: (Double, Double), weightingScheme: String,  budget: Int, targetSize: Long) extends PartitionMatchingTrait{


    def apply(relation: String): RDD[(String, String)] = {
        val tSize = joinedRDD.flatMap(_._2._2).count()
        apply(relation, 10000, tSize)
    }


    /**
     * Similar to the ComparisonCentric, but instead of executing all the comparisons of target,
     * it just select the top-k which k is based on the input budget.
     * Then gives a weight to the entities of the target, based on the weights of their comparisons,
     * and sorts based on that.
     *
     * @param relation the examining relation
     * @param budget the total comparisons we want to implement
     * @param targetSize no entities in target
     * @return an RDD containing the matching pairs
     */
    def apply(relation: String, budget: Int, targetSize: Long): RDD[(String, String)] ={
        val k = budget / targetSize
        init()
        joinedRDD
            .flatMap { p =>
                val partitionId = p._1
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Array[SpatialEntity] = p._2._2.toArray  //WARNING: this will result to OOM
                val sourceIndex = index(source, partitionId)
                val sourceSize = source.length
                val targetsCoords = target.zipWithIndex.map { case (e2, i) =>
                    val coords = indexSpatialEntity(e2, partitionId).filter(c => sourceIndex.contains(c))
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
                        val sIndices = sIndicesList(k).filter(i => source(i).mbb.testMBB(e2.mbb, relation) &&
                            source(i).mbb.referencePointFiltering(e2.mbb, c, thetaXY))
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
            .filter(c => relate(c._1.geometry, c._2.geometry, relation))
            .map(c => (c._1.originalID, c._2.originalID))
    }


}

object EntityCentricPrioritization{


    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String,
              weightingScheme: String = Constants.NO_USE, budget: Int, tcount: Long): EntityCentricPrioritization ={
        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        EntityCentricPrioritization(joinedRDD, thetaXY, weightingScheme, budget, tcount)
    }

}