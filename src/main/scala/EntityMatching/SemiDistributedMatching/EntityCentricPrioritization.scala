package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.{Relation, ThetaOption, WeightStrategy}
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class EntityCentricPrioritization(source: RDD[SpatialEntity], target: ArrayBuffer[SpatialEntity], thetaXY: (Double, Double), ws: WeightStrategy) extends SDMTrait {

    def matchTargetData(relation: Relation, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {

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
                    val coords = e1.index(thetaXY, targetBlocksMapBD.value.contains)
                    val weightedComparisons = coords
                        .flatMap { c =>
                            val targetEntities = targetBlocksMapBD.value(c).filter(e2 => !candidates.contains(e2))
                            candidates ++= targetEntities

                            targetEntities.map { e2ID =>
                                val e2 = targetBD.value(e2ID - idStart)
                                val e2Blocks = e2.index(thetaXY)
                                val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                (w, e2ID)
                            }
                        }
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.length
                    ((e1ID, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .flatMap { case ((e1ID, _), weightedComparisons) =>
                    val e1 = sourceAr(e1ID)
                    weightedComparisons
                        .sortBy(_._1)(Ordering.Double.reverse)
                        .map(p => targetBD.value(p._2 - idStart))
                        .filter(e2 => e1.testMBB(e2, relation))
                        .filter(e2 => e1.relate(e2, relation))
                        .map(e2 => (e1.originalID, e2.originalID))
                }
                .toIterator
        }
    }


    def getDE9IM(idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM] = {

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
                    val coords = e1.index(thetaXY, targetBlocksMapBD.value.contains)
                    val weightedComparisons = coords
                        .flatMap { c =>
                            val targetEntities = targetBlocksMapBD.value(c).filter(e2 => !candidates.contains(e2))
                            candidates ++= targetEntities

                            targetEntities.map { e2ID =>
                                val e2 = targetBD.value(e2ID - idStart)
                                val e2Blocks = e2.index(thetaXY)
                                val w = getWeight(totalBlocks, coords, e2Blocks, ws)
                                (w, e2ID)
                            }
                        }
                    val weights = weightedComparisons.map(_._1)
                    val e1Weight = weights.sum / weights.length
                    ((e1ID, e1Weight), weightedComparisons)
                }
                .sortBy(_._1._2)(Ordering.Double.reverse)
                .flatMap { case ((e1ID, _), weightedComparisons) =>
                    val e1 = sourceAr(e1ID)
                    weightedComparisons
                        .sortBy(_._1)(Ordering.Double.reverse)
                        .map(p => targetBD.value(p._2 - idStart))
                        .filter(e2 => e1.testMBB(e2, Relation.INTERSECTS, Relation.TOUCHES))
                        .map(e2 => IM(e1, e2))
                }
                .toIterator
        }
    }




}


object EntityCentricPrioritization {
    /**
     * Constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @param thetaOption theta measure
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaOption: ThetaOption = ThetaOption.NO_USE, ws: WeightStrategy = WeightStrategy.CBS): EntityCentricPrioritization = {
        val thetaXY = Utils.getTheta
        EntityCentricPrioritization(source, target.sortBy(_.id).collect().to[ArrayBuffer], thetaXY, ws)
    }
}
