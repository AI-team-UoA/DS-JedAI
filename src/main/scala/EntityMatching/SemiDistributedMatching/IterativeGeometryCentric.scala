package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.{Relation, WeightStrategy}
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class IterativeGeometryCentric(source: RDD[SpatialEntity], target: Array[SpatialEntity], thetaXY: (Double, Double),
                                    sourceCount: Long, ws: WeightStrategy, budget: Long) extends SDMTrait {

    def matchTarget(relation: Relation, targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {

        val sc = SparkContext.getOrCreate()
        val targetIndexBD = sc.broadcast(targetIndex)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val totalBlocks = targetIndexBD.value.keySet.size
            val source = sourceIter.toArray
            val candidates = mutable.HashSet[Int]()
            val comparisons = source.indices
                .map { i =>
                    val e1 = source(i)
                    val coords = e1.index(thetaXY, targetIndexBD.value.contains)
                    val weightedComparisons = coords
                        .flatMap { c =>
                            val targetEntities = targetIndexBD.value(c).filter(j => !candidates.contains(j))
                            candidates ++= targetEntities

                            targetEntities
                                .filter(j => e1.testMBB(targetBD.value(j), relation))
                                .map { j =>
                                    val e2 = targetBD.value(j)
                                    val e2Blocks = e2.index(thetaXY)
                                    val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                    (w, j)
                                }
                        }
                    candidates.clear()
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.length
                    ((i, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .map{ case (k, weightedComparisons) =>
                    (k._1, weightedComparisons.sortBy(_._1)(Ordering.Double.reverse).map(_._2).toIterator)}

            val matches = ListBuffer[(String, String)]()
            var converged = false
            while (!converged) {
                converged = true
                for ((i, jIter) <- comparisons) {
                    if (jIter.hasNext) {
                        converged = false
                        val (e1, e2) = (source(i), targetBD.value(jIter.next()))
                            if (e1.relate(e2, relation))
                                matches.append((e1.originalID, e2.originalID))
                    }
                }
            }
            matches.toIterator
        }
    }

    def getDE9IM(targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM] = {
        val sc = SparkContext.getOrCreate()
        val targetIndexBD = sc.broadcast(targetIndex)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val totalBlocks = targetIndexBD.value.keySet.size
            val source = sourceIter.toArray
            val candidates = mutable.HashSet[Int]()
            val comparisons = source.indices
                .map { i =>
                    val e1 = source(i)
                    val coords = e1.index(thetaXY, targetIndexBD.value.contains)
                    val weightedComparisons = coords
                        .flatMap { c =>
                            val targetEntities = targetIndexBD.value(c).filter(j => !candidates.contains(j))
                            candidates ++= targetEntities

                            targetEntities
                                .filter(j => e1.testMBB(targetBD.value(j), Relation.INTERSECTS, Relation.TOUCHES))
                                .map { j =>
                                    val e2 = targetBD.value(j)
                                    val e2Blocks = e2.index(thetaXY)
                                    val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                    (w, j)
                                }
                        }
                    candidates.clear()
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.length
                    ((i, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .map{ case (k, weightedComparisons) =>
                    (k._1, weightedComparisons.sortBy(_._1)(Ordering.Double.reverse).map(_._2).toIterator)}

            val IMs = ListBuffer[IM]()
            var converged = false
            while (!converged) {
                converged = true
                for ((i, jIter) <- comparisons) {
                    if (jIter.hasNext) {
                        converged = false
                        val (e1, e2) = (source(i), targetBD.value(jIter.next()))
                        IMs.append(IM(e1, e2))
                    }
                }
            }
            IMs.toIterator
        }
    }
}

object IterativeGeometryCentric {
    /**
     * Constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], ws: WeightStrategy = WeightStrategy.CBS,  budget: Long): IterativeGeometryCentric = {
        val thetaXY = Utils.getTheta
        val sourceCount = Utils.sourceCount
        IterativeGeometryCentric(source, target.collect(), thetaXY, sourceCount, ws, budget)
    }
}