package EntityMatching.PartitionMatching

import DataStructures.SpatialEntity
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants
import utils.Readers.SpatialReader

import scala.collection.mutable

case class IterativeEntityCentricPrioritization(joinedRDD: RDD[(Int, (Array[SpatialEntity], Array[SpatialEntity]))],
                                           thetaXY: (Double, Double), weightingScheme: String, budget: Int, targetSize: Long) extends PartitionMatchingTrait {

    val orderByWeight: Ordering[(Double, (SpatialEntity, SpatialEntity))] = Ordering.by[(Double, (SpatialEntity, SpatialEntity)), Double](_._1).reverse


    /**
     * Similar to the ComparisonCentric, but instead of executing all the comparisons of target,
     * it just select the top-k which k is based on the input budget.
     * Then gives a weight to the entities of the target, based on the weights of their comparisons,
     * and sorts based on that.
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: String): RDD[(String, String)] = {
        val k = budget / targetSize
        init()
        joinedRDD
            .flatMap { p =>
                val partitionId = p._1
                val source: Array[SpatialEntity] = p._2._1
                val target: Array[SpatialEntity] = p._2._2
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
                            source(i).mbb.referencePointFiltering(e2.mbb, c))
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
                    (weight, topComparisons.toIterator)
                }
                comparisons.filter(_._1 >= 0d)
            }

            // sort the comparisons based on their mean weight
            .sortByKey(ascending = false)
            .map(_._2)
            .mapPartitions { comparisonsIter =>
                val comparisonsArIter = comparisonsIter.toArray
                var matches: List[(String, String)] = List()

                var converge: Boolean = false
                while (!converge) {
                    converge = true
                    for (comparisonIter <- comparisonsArIter) {
                        if (comparisonIter.hasNext) {
                            converge = false
                            val (e1, e2) = comparisonIter.next()
                            if (relate(e1.geometry, e2.geometry, relation))
                                matches = matches :+ (e1.originalID, e2.originalID)
                        }
                    }
                }
                matches.toIterator
            }
    }
}

object IterativeEntityCentricPrioritization{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String, weightingScheme: String, budget: Int, tcount: Long):
    IterativeEntityCentricPrioritization ={
        val thetaXY = initTheta(source, target, thetaMsrSTR)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), Array(se))).reduceByKey(SpatialReader.spatialPartitioner, _ ++ _)

        val joinedRDD = sourcePartitions.join(targetPartitions, SpatialReader.spatialPartitioner)
        IterativeEntityCentricPrioritization(joinedRDD, thetaXY, weightingScheme, budget, tcount)
    }


    /**
     * initialize theta based on theta measure
     */
    def initTheta(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaMsrSTR: String): (Double, Double) ={
        val thetaMsr: RDD[(Double, Double)] = source
            .union(target)
            .map {
                sp =>
                    val env = sp.geometry.getEnvelopeInternal
                    (env.getHeight, env.getWidth)
            }
            .setName("thetaMsr")
            .cache()

        var thetaX = 1d
        var thetaY = 1d
        thetaMsrSTR match {
            case Constants.MIN =>
                // filtering because there are cases that the geometries are perpendicular to the axes
                // and have width or height equals to 0.0
                thetaX = thetaMsr.map(_._1).filter(_ != 0.0d).min
                thetaY = thetaMsr.map(_._2).filter(_ != 0.0d).min
            case Constants.MAX =>
                thetaX = thetaMsr.map(_._1).max
                thetaY = thetaMsr.map(_._2).max
            case Constants.AVG =>
                val length = thetaMsr.count
                thetaX = thetaMsr.map(_._1).sum() / length
                thetaY = thetaMsr.map(_._2).sum() / length
            case _ =>
        }
        (thetaX, thetaY)
    }
}
