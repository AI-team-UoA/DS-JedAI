package EntityMatching.LightAlgorithms

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.log10

trait LightMatchingTrait extends Serializable {
    val source: RDD[SpatialEntity]
    val target: ArrayBuffer[SpatialEntity]
    val thetaXY: (Double, Double)

    def matchTargetData(relation: String, idStart: Int, targetBlocksMap: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(Int, Int)]

    /**
     * start LightRADON algorithm
     * @param idStart target's id starting value
     * @param relation the examined relation
     * @return an RDD of matches
     */
    def apply(idStart: Int, relation: String): RDD[(Int, Int)] = {
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


    /**
     * check the relation between two geometries
     *
     * @param sourceGeom geometry from source set
     * @param targetGeometry geometry from target set
     * @param relation requested relation
     * @return whether the relation is true
     */
    def relate(sourceGeom: Geometry, targetGeometry: Geometry, relation: String): Boolean ={
        relation match {
            case Constants.CONTAINS => sourceGeom.contains(targetGeometry)
            case Constants.INTERSECTS => sourceGeom.intersects(targetGeometry)
            case Constants.CROSSES => sourceGeom.crosses(targetGeometry)
            case Constants.COVERS => sourceGeom.covers(targetGeometry)
            case Constants.COVEREDBY => sourceGeom.coveredBy(targetGeometry)
            case Constants.OVERLAPS => sourceGeom.overlaps(targetGeometry)
            case Constants.TOUCHES => sourceGeom.touches(targetGeometry)
            case Constants.DISJOINT => sourceGeom.disjoint(targetGeometry)
            case Constants.EQUALS => sourceGeom.equals(targetGeometry)
            case Constants.WITHIN => sourceGeom.within(targetGeometry)
            case _ => false
        }
    }


    /**
     *  check relation among MBBs
     *
     * @param s MBB from source
     * @param t MBB form target
     * @param relation requested relation
     * @return whether the relation is true
     */
    def testMBB(s:MBB, t:MBB, relation: String): Boolean ={
        relation match {
            case Constants.CONTAINS | Constants.COVERS =>
                s.contains(t)
            case Constants.WITHIN | Constants.COVEREDBY =>
                s.within(t)
            case Constants.INTERSECTS | Constants.CROSSES | Constants.OVERLAPS =>
                s.intersects(t)
            case Constants.TOUCHES => s.touches(t)
            case Constants.DISJOINT => s.disjoint(t)
            case Constants.EQUALS => s.equals(t)
            case _ => false
        }
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


    def normalizeWeight(weight: Double, entity1: Geometry, entity2:Geometry): Double ={
        val area1 = entity1.getArea
        val area2 = entity2.getArea
        if (area1 == 0 || area2 == 0 ) weight
        else weight/(entity1.getArea * entity2.getArea)
    }



}
