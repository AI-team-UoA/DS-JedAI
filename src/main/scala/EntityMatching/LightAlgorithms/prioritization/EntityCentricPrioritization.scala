package EntityMatching.LightAlgorithms.prioritization

import DataStructures.SpatialEntity
import EntityMatching.LightAlgorithms.LightMatchingTrait
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class EntityCentricPrioritization(source: RDD[SpatialEntity], target: ArrayBuffer[SpatialEntity], thetaXY: (Double, Double), weightingStrategy: String) extends LightMatchingTrait {

    def matchTargetData(relation: String, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(Int, Int)] = {

        val sc = SparkContext.getOrCreate()
        val targetBlocksMapBD = sc.broadcast(targetBlocksMap)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val totalBlocks = targetBlocksMapBD.value.keySet.size
            val sourceAr = sourceIter.toArray
            sourceAr
                .zipWithIndex
                .map { case (e1, e1ID) =>
                    val candidates = mutable.HashSet[Int]()
                    val coords = indexSpatialEntity(e1)
                    val weightedComparisons = coords
                        .filter(targetBlocksMapBD.value.contains)
                        .flatMap { c =>
                            val targetEntities = targetBlocksMapBD.value(c).filter(e2 => !candidates.contains(e2))
                            candidates ++= targetEntities

                            targetEntities.map { e2ID =>
                                val e2Blocks = indexSpatialEntity(targetBD.value(e2ID - idStart))
                                val w = getWeight(totalBlocks, coords, e2Blocks, weightingStrategy)
                                (w, e2ID)
                            }
                        }
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.size
                    ((e1ID, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .flatMap { case ((e1ID, w), weightedComparisons) =>
                    val e1 = sourceAr(e1ID)
                    weightedComparisons
                        .sortBy(_._1)
                        .map(p => targetBD.value(p._2 - idStart))
                        .filter(e2 => testMBB(e1.mbb, e2.mbb, relation))
                        .filter(e2 => relate(e1.geometry, e2.geometry, relation))
                        .map(e2 => (e1.id, e2.id))
                }
                .toIterator
        }
    }


    def iterativeExecution(relation: String, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(Int, Int)] = {

        val sc = SparkContext.getOrCreate()
        val targetBlocksMapBD = sc.broadcast(targetBlocksMap)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val totalBlocks = targetBlocksMapBD.value.keySet.size
            val sourceAr = sourceIter.toArray
            val comparisons = sourceAr
                .zipWithIndex
                .map { case (e1, e1ID) =>
                    val candidates = mutable.HashSet[Int]()
                    val coords = indexSpatialEntity(e1)
                    val weightedComparisons = coords
                        .filter(targetBlocksMapBD.value.contains)
                        .flatMap { c =>
                            val targetEntities = targetBlocksMapBD.value(c).filter(e2 => !candidates.contains(e2))
                            candidates ++= targetEntities

                            targetEntities.map { e2ID =>
                                val e2Blocks = indexSpatialEntity(targetBD.value(e2ID - idStart))
                                val w = getWeight(totalBlocks, coords, e2Blocks, weightingStrategy)
                                (w, e2ID)
                            }
                        }
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.size
                    ((e1ID, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .map{case (k, weightedComparisons) => (sourceAr(k._1), weightedComparisons
                                                                            .sortBy(_._1)(Ordering.Double.reverse)
                                                                            .map(p => targetBD.value(p._2 - idStart))
                                                                            .toIterator)}

            val matches = ArrayBuffer[(Int, Int)]()
            var converged = false
            while (!converged) {
                converged = true
                for (c <- comparisons) {
                    if (c._2.hasNext) {
                        converged = false
                        val (e1, e2) = (c._1, c._2.next())
                        if (testMBB(e1.mbb, e2.mbb, relation))
                            if (relate(e1.geometry, e2.geometry, relation))
                                matches.append((e1.id, e2.id))
                    }
                }
            }
            matches.toIterator
        }
    }
}


object EntityCentricPrioritization {
    /**
     * Constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @param thetaMsrSTR theta measure
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String = Constants.NO_USE, weightingStrategy: String = Constants.CBS): EntityCentricPrioritization = {
        val thetaXY = initTheta(source, target, thetaMsrSTR)
        EntityCentricPrioritization(source, target.sortBy(_.id).collect().to[ArrayBuffer], thetaXY, weightingStrategy)
    }

    /**
     * initialize theta based on theta measure
     */
    def initTheta(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String): (Double, Double) = {
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
            // WARNING: small or big values of theta may affect negatively the indexing procedure
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
