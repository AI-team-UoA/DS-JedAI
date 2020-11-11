package EntityMatching.SemiDistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy
import utils.Constants.WeightStrategy.WeightStrategy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.log10

trait SDMTrait {
    val budget: Long
    val source: RDD[SpatialEntity]
    val target: Array[SpatialEntity]
    val thetaXY: (Double, Double)

    /**
     * start LightRADON algorithm
     * @param relation the examined relation
     * @return an RDD of matches
     */
    def apply(relation: Relation): RDD[(String, String)] = matchTarget(relation, indexTarget())

    def applyDE9IM: RDD[IM] = getDE9IM(indexTarget())

    def matchTarget(relation: Relation, targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[(String, String)]

    def getDE9IM(targetIndex: mutable.HashMap[(Int, Int), ListBuffer[Int]]): RDD[IM]


    /**
     * Index the collected dataset
     *
     * @return a HashMap containing the block coordinates as keys and
     *         lists of Spatial Entities ids as values
     */
    def indexTarget():mutable.HashMap[(Int, Int), ListBuffer[Int]] = {
        val index = mutable.HashMap[(Int, Int), ListBuffer[Int]]()
        for ((se, i) <- target.zipWithIndex) {
            val blocksIter = se.index(thetaXY)
            blocksIter.foreach {
                blockCoords =>
                    if (index.contains(blockCoords))
                        index(blockCoords).append(i)
                    else index += (blockCoords -> ListBuffer(i))
            }
        }
        index
    }


    def getWeight(totalBlocks: Int, e1Blocks: Seq[(Int, Int)], e2Blocks: Seq[(Int, Int)], ws: WeightStrategy = WeightStrategy.CBS): Double ={
        val commonBlocks = e1Blocks.intersect(e2Blocks).length
        ws match {
            case WeightStrategy.ECBS =>
                commonBlocks * log10(totalBlocks / e1Blocks.length) * log10(totalBlocks / e2Blocks.length)
            case WeightStrategy.JS =>
                commonBlocks / (e1Blocks.length + e2Blocks.length - commonBlocks)
            case WeightStrategy.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](commonBlocks, e2Blocks.length - commonBlocks)
                val v2: Array[Long] = Array[Long](e1Blocks.length - commonBlocks, totalBlocks - (v1(0) + v1(1) +(e1Blocks.length - commonBlocks)) )

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))
            case WeightStrategy.CBS | _ =>
                commonBlocks
        }
    }


    implicit class TuppleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
        def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
            (p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
    }

    def countRelations: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
        applyDE9IM
            .mapPartitions { imIterator =>
                var totalContains: Int = 0
                var totalCoveredBy = 0
                var totalCovers = 0
                var totalCrosses = 0
                var totalEquals = 0
                var totalIntersects = 0
                var totalOverlaps = 0
                var totalTouches = 0
                var totalWithin = 0
                var intersectingPairs = 0
                var interlinkedGeometries = 0
                imIterator.foreach { im =>
                    intersectingPairs += 1
                    if (im.relate) {
                        interlinkedGeometries += 1
                        if (im.isContains) totalContains += 1
                        if (im.isCoveredBy) totalCoveredBy += 1
                        if (im.isCovers) totalCovers += 1
                        if (im.isCrosses) totalCrosses += 1
                        if (im.isEquals) totalEquals += 1
                        if (im.isIntersects) totalIntersects += 1
                        if (im.isOverlaps) totalOverlaps += 1
                        if (im.isTouches) totalTouches += 1
                        if (im.isWithin) totalWithin += 1
                    }
                }

                Iterator((totalContains, totalCoveredBy, totalCovers,
                    totalCrosses, totalEquals, totalIntersects,
                    totalOverlaps, totalTouches, totalWithin,
                    intersectingPairs, interlinkedGeometries))
            }
            .treeReduce({ case (im1, im2) => im1 + im2}, 4)
    }


}
