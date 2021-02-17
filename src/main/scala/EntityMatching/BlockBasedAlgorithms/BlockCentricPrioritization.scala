package EntityMatching.BlockBasedAlgorithms

import DataStructures.{Block, Entity}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils

import scala.math.{ceil, floor, max, min}


/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

case class BlockCentricPrioritization(blocks: RDD[Block], d: (Int, Int), totalBlocks: Long,
                                      ws: WeightStrategy) extends BlockMatchingTrait  {

    val thetaXY: (Double, Double) = Utils.getTheta

    /**
     * Weight a comparison
     *
     * @param e1        Spatial entity
     * @param e2        Spatial entity
     * @return weight
     */
    def getWeight(e1: Entity, e2: Entity): Double = {
        val e1Blocks = (ceil(e1.mbr.maxX/thetaXY._1).toInt - floor(e1.mbr.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbr.maxY/thetaXY._2).toInt - floor(e1.mbr.minY/thetaXY._2).toInt + 1).toDouble
        val e2Blocks = (ceil(e2.mbr.maxX/thetaXY._1).toInt - floor(e2.mbr.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbr.maxY/thetaXY._2).toInt - floor(e2.mbr.minY/thetaXY._2).toInt + 1).toDouble
        val cb = (min(ceil(e1.mbr.maxX/thetaXY._1), ceil(e2.mbr.maxX/thetaXY._1)).toInt - max(floor(e1.mbr.minX/thetaXY._1), floor(e2.mbr.minX/thetaXY._1)).toInt + 1) *
            (min(ceil(e1.mbr.maxY/thetaXY._2), ceil(e2.mbr.maxY/thetaXY._2)).toInt - max(floor(e1.mbr.minY/thetaXY._2), floor(e2.mbr.minY/thetaXY._2)).toInt + 1)

        ws match {
            case WeightStrategy.ECBS =>
                cb * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case WeightStrategy.JS =>
                cb / (e1Blocks + e2Blocks - cb)

            case WeightStrategy.PEARSON_X2 =>
                val v1: Array[Long] = Array[Long](cb, (e2Blocks - cb).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - cb))).toLong)
                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case WeightStrategy.CBS | _ =>
                cb.toDouble
        }
    }

    /**
     * Get blocks' comparisons and filter the unnecessary ones. Then weight the comparisons
     * based on an block centric way and the weighting scheme. Then sort and execute the
     * comparisons.
     *
     * @return an RDD containing the IDs of the matches
     */
    def apply(relation: Relation): RDD[(String, String)] ={
        blocks.flatMap(b => b.getFilteredComparisons(relation))
            .map(c => (getWeight(c._1, c._2), c))
            .sortByKey(ascending = false)
            .map(_._2)
            .filter(c => c._1.relate(c._2, relation))
            .map(c => (c._1.originalID, c._2.originalID))
    }
}

object BlockCentricPrioritization {

    // auxiliary constructors
    def apply(blocks: RDD[Block], d: (Int, Int), ws : WeightStrategy): BlockCentricPrioritization ={
        BlockCentricPrioritization(blocks, d, blocks.count(), ws)
    }

    def apply(blocks: RDD[Block], ws : WeightStrategy): BlockCentricPrioritization ={
        val d1 = blocks.map(b => b.getSourceIDs.toSet).reduce(_ ++ _).size
        val d2 = blocks.map(b => b.getTargetIDs.toSet).reduce(_ ++ _).size
        BlockCentricPrioritization(blocks, (d1, d2), blocks.count(), ws)
    }
}
