package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, Entity}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.{Constants, Utils}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * A Matching algorithm. Similar to RADON but the target dataset is
 * collected and brodcasted during the matching procedure.
 *
 * @param source the distributed dataset (source)
 * @param target the collected dataset
 * @param thetaXY theta values
 */
case class LightRADON(source: RDD[Entity], target: Array[Entity], thetaXY: (Double, Double), budget: Long) extends SDMTrait {

       /**
        * Get the matching pairs which the relation holds.
        * Broadcast target's index and the collected dataset.
        * Then index the distributed dataset and compare the entities with common blocks
        * and intersecting MBBs. To avoid duplicate comparisons maintain a Set with the
        * compared entities.
        *
        * @param relation the testing relation
        * @param targetIndex HashMap of targets blocks
        * @return an RDD of matches
     */
   def matchTarget(relation: Relation, targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)] = {
       val sc = SparkContext.getOrCreate()
       val targetIndexBD = sc.broadcast(targetIndex)
       val targetBD = sc.broadcast(target)

       source
           .flatMap { e1 =>
               val sourceIndices = e1.index(thetaXY)
               val compared = mutable.HashSet[Int]()
               val index = targetIndexBD.value
               sourceIndices
                   .filter(index.contains)
                   .flatMap { block =>
                       val entitiesIDs = index(block).filter(i => !compared.contains(i))
                       compared ++= entitiesIDs
                       entitiesIDs
                           .map(i => targetBD.value(i))
                           .filter(e2 => e1.testMBB(e2, relation) && e1.relate(e2, relation))
                           .map(e2 => (e1.originalID, e2.originalID))
                   }
           }
   }

    /**
     * Similar to matchTarget, but returns the intersection matrices (DE-9IM)
     * @param targetIndex HashMap of targets blocks
     * @return an RDD of Intersection Matrices
     */
   def getDE9IM(targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM] = {
       val sc = SparkContext.getOrCreate()
       val targetIndexBD = sc.broadcast(targetIndex)
       val targetBD = sc.broadcast(target)

       source
           .flatMap { e1 =>
               val sourceIndices = e1.index(thetaXY)
               val compared = mutable.HashSet[Int]()
               val index = targetIndexBD.value
               sourceIndices
                   .filter(index.contains)
                   .flatMap { block =>
                       val entitiesIDs = index(block).filter(i => !compared.contains(i))
                       compared ++= entitiesIDs
                       entitiesIDs
                           .map(i => targetBD.value(i))
                           .filter(e2 => e1.testMBB(e2, Constants.Relation.INTERSECTS, Constants.Relation.TOUCHES))
                           .map(e2 => IM(e1, e2))
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
     * @return LightRADON instance
     */
    def apply(source: RDD[Entity], target: RDD[Entity], budget: Long): LightRADON ={
        val thetaXY = Utils.getTheta
        LightRADON(source, target.collect(), thetaXY, budget)
    }

}
