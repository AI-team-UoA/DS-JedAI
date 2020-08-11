package EntityMatching.LightAlgorithms

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption
import utils.Constants.ThetaOption.ThetaOption
import utils.{Constants, Utils}

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
case class LightRADON(source: RDD[SpatialEntity], target: ArrayBuffer[SpatialEntity], thetaXY: (Double, Double) ) extends LightMatchingTrait {



       /**
     * Get the matching pairs of the datasets. Broadcast the blocks hashmap and the collected dataset.
     * Then index the distributed dataset and after obtaining their blocks, get the respective
     * entities of the broadcasted dataset of the same block and perform the comparisons.
     * First test their MBBs and then their geometries
     *
     * @param relation the examined relation
     * @param idStart target ids starting value
     * @param targetBlocksMap HashMap of targets blocks
     * @return an RDD of matches
     */
   def matchTargetData(relation: Relation, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {
       val sc = SparkContext.getOrCreate()
       val blocksMapBD = sc.broadcast(targetBlocksMap)
       val collectedBD = sc.broadcast(target)

       source
           .map(se => (se, se.index(thetaXY)))
           .flatMap { case (se, blocksArray) =>
               val compared = mutable.HashSet[Int]()
               val blocksMap = blocksMapBD.value
               blocksArray
                   .filter(blocksMap.contains)
                   .flatMap { block =>
                       val entitiesIDs = blocksMap(block).filter(id => !compared.contains(id))
                       compared ++= entitiesIDs
                       entitiesIDs
                           .map(id => collectedBD.value(id - idStart))
                           .filter(tse => se.testMBB(tse, relation))
                           .filter(tse => se.relate(tse, relation))
                           .map(tse => (se.originalID, tse.originalID))
                   }
           }
   }

   def getDE9IM(idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM] = {
       val sc = SparkContext.getOrCreate()
       val blocksMapBD = sc.broadcast(targetBlocksMap)
       val collectedBD = sc.broadcast(target)

       source
           .map(se => (se, se.index(thetaXY)))
           .flatMap { case (se, blocksArray) =>
               val compared = mutable.HashSet[Int]()
               val blocksMap = blocksMapBD.value
               blocksArray
                   .filter(blocksMap.contains)
                   .flatMap { block =>
                       val entitiesIDs = blocksMap(block).filter(id => !compared.contains(id))
                       compared ++= entitiesIDs
                       entitiesIDs
                           .map(id => collectedBD.value(id - idStart))
                           .filter(tse => se.testMBB(tse, Constants.Relation.INTERSECTS))
                           .map(tse => IM(se, tse))
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
     * @param thetaOption theta measure
     * @return LightRADON instance
     */
    def apply(source: RDD[SpatialEntity], target: RDD[SpatialEntity], thetaOption: ThetaOption = ThetaOption.NO_USE): LightRADON ={
        val thetaXY = Utils.initTheta(source, target, thetaOption)
        LightRADON(source, target.sortBy(_.id).collect().to[ArrayBuffer], thetaXY)
    }

}
