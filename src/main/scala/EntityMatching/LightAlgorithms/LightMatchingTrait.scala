package EntityMatching.LightAlgorithms

import DataStructures.SpatialEntity
import EntityMatching.MatchingTrait
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.log10

trait LightMatchingTrait extends MatchingTrait {
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
     * find the blocks of a Spatial Entity
     * @param se input SpatialEntity
     * @return and array of Block coordinates
     */
    def indexSpatialEntity(se: SpatialEntity): ArrayBuffer[(Int, Int)] ={
        val (thetaX, thetaY) = thetaXY
        // TODO: crossing meridian
        val maxX = math.ceil(se.mbb.maxX / thetaX).toInt
        val minX = math.floor(se.mbb.minX / thetaX).toInt
        val maxY = math.ceil(se.mbb.maxY / thetaY).toInt
        val minY = math.floor(se.mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY) yield (x, y)).to[ArrayBuffer]
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
            val blocksIter = indexSpatialEntity(se)
            blocksIter.foreach {
                blockCoords =>
                    if (blocksMap.contains(blockCoords))
                        blocksMap(blockCoords).append(seID)
                    else blocksMap += (blockCoords -> ListBuffer(seID))
            }
        }
        blocksMap
    }




    def getWeight(totalBlocks: Int, e1Blocks: ArrayBuffer[(Int, Int)], e2Blocks: ArrayBuffer[(Int, Int)], weightingStrategy: String = Constants.CBS): Double ={
        val commonBlocks = e1Blocks.intersect(e2Blocks).size
        weightingStrategy match {
            case Constants.ECBS =>
                commonBlocks * log10(totalBlocks / e1Blocks.size) * log10(totalBlocks / e2Blocks.size)
            case Constants.JS =>
                commonBlocks / (e1Blocks.size + e2Blocks.size - commonBlocks)
            case Constants.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](commonBlocks, e2Blocks.size - commonBlocks)
                val v2: Array[Long] = Array[Long](e1Blocks.size - commonBlocks, totalBlocks - (v1(0) + v1(1) +(e1Blocks.size - commonBlocks)) )

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))
            case Constants.CBS | _ =>
                commonBlocks
        }
    }



}
