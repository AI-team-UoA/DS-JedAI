package EntityMatching.DistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.rdd.RDD
import utils.Constants.{Relation, WeightStrategy}
import utils.Constants.Relation.Relation

import scala.math.{ceil, floor}

trait DMProgressiveTrait extends DMTrait{
    val budget: Long

    def apply(relation: Relation): RDD[(String, String)] = {
        val imRDD = getDE9IM
        relation match {
            case Relation.CONTAINS => imRDD.filter(_.isContains).map(_.idPair)
            case Relation.COVEREDBY => imRDD.filter(_.isCoveredBy).map(_.idPair)
            case Relation.COVERS => imRDD.filter(_.isCovers).map(_.idPair)
            case Relation.CROSSES => imRDD.filter(_.isCrosses).map(_.idPair)
            case Relation.INTERSECTS => imRDD.filter(_.isIntersects).map(_.idPair)
            case Relation.TOUCHES => imRDD.filter(_.isTouches).map(_.idPair)
            case Relation.EQUALS => imRDD.filter(_.isEquals).map(_.idPair)
            case Relation.OVERLAPS => imRDD.filter(_.isOverlaps).map(_.idPair)
            case Relation.WITHIN => imRDD.filter(_.isWithin).map(_.idPair)
        }
    }

    /**
     * Weight a comparison
     *
     * @param frequency common blocks of e1 and e2
     * @param e1        Spatial entity
     * @param e2        Spatial entity
     * @return weight
     */
    def getWeight(frequency: Int, e1: SpatialEntity, e2: SpatialEntity): Double = {
        ws match {
            case WeightStrategy.ECBS =>
                val e1Blocks = (ceil(e1.mbb.maxX/thetaXY._1).toInt - floor(e1.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbb.maxY/thetaXY._2).toInt - floor(e1.mbb.minY/thetaXY._2).toInt + 1).toDouble
                val e2Blocks = (ceil(e2.mbb.maxX/thetaXY._1).toInt - floor(e2.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbb.maxY/thetaXY._2).toInt - floor(e2.mbb.minY/thetaXY._2).toInt + 1).toDouble
                frequency * math.log10(totalBlocks / e1Blocks) * math.log10(totalBlocks / e2Blocks)

            case WeightStrategy.JS =>
                val e1Blocks = (ceil(e1.mbb.maxX/thetaXY._1).toInt - floor(e1.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbb.maxY/thetaXY._2).toInt - floor(e1.mbb.minY/thetaXY._2).toInt + 1).toDouble
                val e2Blocks = (ceil(e2.mbb.maxX/thetaXY._1).toInt - floor(e2.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbb.maxY/thetaXY._2).toInt - floor(e2.mbb.minY/thetaXY._2).toInt + 1).toDouble
                frequency / (e1Blocks + e2Blocks - frequency)

            case WeightStrategy.PEARSON_X2 =>
                val e1Blocks = (ceil(e1.mbb.maxX/thetaXY._1).toInt - floor(e1.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e1.mbb.maxY/thetaXY._2).toInt - floor(e1.mbb.minY/thetaXY._2).toInt + 1).toDouble
                val e2Blocks = (ceil(e2.mbb.maxX/thetaXY._1).toInt - floor(e2.mbb.minX/thetaXY._1).toInt + 1) * (ceil(e2.mbb.maxY/thetaXY._2).toInt - floor(e2.mbb.minY/thetaXY._2).toInt + 1).toDouble

                val v1: Array[Long] = Array[Long](frequency, (e2Blocks - frequency).toLong)
                val v2: Array[Long] = Array[Long]((e1Blocks - frequency).toLong, (totalBlocks - (v1(0) + v1(1) + (e1Blocks - frequency))).toLong)

                val chiTest = new ChiSquareTest()
                chiTest.chiSquare(Array(v1, v2))

            case WeightStrategy.CBS | _ =>
                frequency.toDouble
        }
    }

    def getDE9IM: RDD[IM]

    def getWeightedDE9IM: RDD[(Double, IM)]
}
