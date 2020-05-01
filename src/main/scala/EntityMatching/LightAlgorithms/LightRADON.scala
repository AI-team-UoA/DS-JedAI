package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import EntityMatching.DistributedAlgorithms.SpatialMatching
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
 * A blocking and Matching algorithm. Similar to RADON but the target dataset is
 * collected and brodcasted during the matching procedure.
 *
 * @param source the distributed dataset (source)
 * @param target the collected dataset
 * @param thetaXY theta values
 */
case class LightRADON(source: RDD[SpatialEntity], target: ArrayBuffer[SpatialEntity], thetaXY: (Double, Double) ) extends LightAlgorithmsTrait {



       /**
     * Get the matching pairs of the datasets. Broadcast the blocks hashmap and the collected dataset.
     * Then index the distributed dataset and after obtaining their blocks, get the respective
     * entities of the broadcasted dataset of the same block and perform the comparisons.
     * First test their MBBs and then their geometries
     *
     * @param relation the examined relation
     * @param idStart target ids starting value
     * @param blocksMap HashMap of targets blocks
     * @return an RDD of matches
     */
   def matchTargetData(relation: String, idStart: Int, blocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(Int, Int)] ={
       val sc = SparkContext.getOrCreate()
       val blocksMapBD = sc.broadcast(blocksMap)
       val collectedBD = sc.broadcast(target)

       source
           .map(se => (se, indexSpatialEntity(se)))
           .flatMap { case (se, blocksArray) =>
               val compared = mutable.HashSet[Int]()
               val blocksMap = blocksMapBD.value
               blocksArray
                   .filter(blocksMap.contains)
                   .flatMap{ block =>
                       val entitiesIDs = blocksMap(block).filter(id => !compared.contains(id))
                       compared ++= entitiesIDs
                       entitiesIDs
                           .map(id => collectedBD.value(id - idStart))
                           .filter(tse => testMBB(se.mbb, tse.mbb, relation))
                           .filter(tse => relate(se.geometry, tse.geometry, relation))
                           .map(tse => (se.id, tse.id))
               }
           }
   }

}


object LightRADON {
    /**
     * Constructor based on RDDs
     *
     * @param source source RDD
     * @param target target RDD which will be collected
     * @param thetaMsrSTR theta measure
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaMsrSTR: String = Constants.NO_USE): LightRADON ={
        val thetaXY = initTheta(source, target, thetaMsrSTR)
        LightRADON(source, target.sortBy(_.id).collect().to[ArrayBuffer], thetaXY)
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
