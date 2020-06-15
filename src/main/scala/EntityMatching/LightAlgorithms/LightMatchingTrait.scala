package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.log10

trait LightMatchingTrait {

    val source: RDD[SpatialEntity]
    val target: ArrayBuffer[SpatialEntity]
    val thetaXY: (Double, Double)

    def matchTargetData(relation: String, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)]

    /**
     * start LightRADON algorithm
     * @param idStart target's id starting value
     * @param relation the examined relation
     * @return an RDD of matches
     */
    def apply(idStart: Int, relation: String): RDD[(String, String)] = {
        val blocksMap = indexTarget()
        matchTargetData(relation, idStart, blocksMap)
    }

    /**
     * Index the collected dataset
     *
     * @return a HashMap containing the block coordinates as keys and
     *         lists of Spatial Entities ids as values
     */
    def indexTarget():mutable.HashMap[(Int, Int), ListBuffer[Int]] = {
        var blocksMap = mutable.HashMap[(Int, Int), ListBuffer[Int]]()
        for (se <- target) {
            val seID = se.id
            val blocksIter = se.index(thetaXY)
            blocksIter.foreach {
                blockCoords =>
                    if (blocksMap.contains(blockCoords))
                        blocksMap(blockCoords).append(seID)
                    else blocksMap += (blockCoords -> ListBuffer(seID))
            }
        }
        blocksMap
    }




    def getWeight(totalBlocks: Int, e1Blocks: Array[(Int, Int)], e2Blocks: Array[(Int, Int)], weightingStrategy: String = Constants.CBS): Double ={
        val commonBlocks = e1Blocks.intersect(e2Blocks).length
        weightingStrategy match {
            case Constants.ECBS =>
                commonBlocks * log10(totalBlocks / e1Blocks.length) * log10(totalBlocks / e2Blocks.length)
            case Constants.JS =>
                commonBlocks / (e1Blocks.length + e2Blocks.length - commonBlocks)
            case Constants.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](commonBlocks, e2Blocks.length - commonBlocks)
                val v2: Array[Long] = Array[Long](e1Blocks.length - commonBlocks, totalBlocks - (v1(0) + v1(1) +(e1Blocks.length - commonBlocks)) )

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))
            case Constants.CBS | _ =>
                commonBlocks
        }
    }



}
