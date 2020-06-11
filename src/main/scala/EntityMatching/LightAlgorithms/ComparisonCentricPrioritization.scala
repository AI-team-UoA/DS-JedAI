package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


case class ComparisonCentricPrioritization(source: RDD[SpatialEntity], target: ArrayBuffer[SpatialEntity], thetaXY: (Double, Double), weightingStrategy: String) extends LightMatchingTrait{


    def matchTargetData(relation: String, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {

        val sc = SparkContext.getOrCreate()
        val targetBlocksMapBD = sc.broadcast(targetBlocksMap)
        val targetBD = sc.broadcast(target)

        source.mapPartitions { sourceIter =>
            val totalBlocks = targetBlocksMapBD.value.keySet.size
            val sourceAr = sourceIter.toArray
            sourceAr
                .zipWithIndex
                .flatMap { case (e1, e1ID) =>
                    val candidates = mutable.HashSet[Int]()
                    val coords = e1.index(thetaXY)
                    coords
                        .filter(targetBlocksMapBD.value.contains)
                        .flatMap { c =>
                            val targetEntities = targetBlocksMapBD.value(c).filter(e2 => !candidates.contains(e2))
                            candidates ++= targetEntities

                            targetEntities.map { e2ID =>
                                val e2 = targetBD.value(e2ID - idStart)
                                val e2Blocks = e2.index(thetaXY)
                                val w = getWeight(totalBlocks, coords, e2Blocks, weightingStrategy)
                                (w, (e1ID, e2ID))
                            }
                        }
                }
                .sortBy(_._1)(Ordering.Double.reverse)
                .map(p => (sourceAr(p._2._1), targetBD.value(p._2._2 - idStart)))
                .filter(c => c._1.mbb.testMBB(c._2.mbb, relation))
                .filter(c => c._1.relate(c._2, relation))
                .map(c => (c._1.originalID, c._2.originalID))
                .toIterator
        }
    }

}



object ComparisonCentricPrioritization {
    /**
     * Constructor based on RDDs
     *
     * @param source      source RDD
     * @param target      target RDD which will be collected
     * @param thetaMsrSTR theta measure
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String = Constants.NO_USE, weightingStrategy: String = Constants.CBS): ComparisonCentricPrioritization = {
        val thetaXY = Utils.initTheta(source, target, thetaMsrSTR)
        ComparisonCentricPrioritization(source, target.sortBy(_.id).collect().to[ArrayBuffer], thetaXY, weightingStrategy)
    }
}
