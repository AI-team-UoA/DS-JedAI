package EntityMatching.DistributedAlgorithms.prioritization

import EntityMatching.DistributedAlgorithms.DistributedMatchingTrait
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

trait DistributedProgressiveMatching extends DistributedMatchingTrait{

    val totalBlocks: Long
    val weightingScheme: String

    /**
     * Weight the comparisons of blocks and clean the duplicate comparisons.
     * The accepted weighing strategies are CBS(default), ECBS, JS and PEARSON_X
     *
     * @return the weighted comparisons of each block
     */
    def getWeights(): RDD[((Long, Double), ArrayBuffer[Long])] ={
        val sc = SparkContext.getOrCreate()
        val totalBlocksBD = sc.broadcast(totalBlocks)

        val entitiesBlockMapBD =
            if (weightingScheme != Constants.CBS ){
                val ce1:RDD[(Int, Long)] = blocks.flatMap(b => b.getSourceIDs.map(id => (id, b.id)))
                val ce2:RDD[(Int, Long)] = blocks.flatMap(b => b.getTargetIDs.map(id => (id, b.id)))
                val ce = ce1.union(ce2)
                    .map(c => (c._1, ArrayBuffer(c._2)))
                    .reduceByKey(_ ++ _)
                    .map(c => (c._1, c._2.toSet))
                    .collectAsMap()
                sc.broadcast(ce)
            }
            else null

        weightingScheme match {
            case Constants.ECBS =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c=>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        val weight = commonBlocks.size * math.log10(totalBlocksBD.value/e1Blocks.size) * math.log10(totalBlocksBD.value/e2Blocks.size)
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.JS =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map { c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        val denominator = e1Blocks.size + e2Blocks.size - commonBlocks.size
                        val weight = commonBlocks.size / denominator
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.PEARSON_X2 =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)

                        val v1: Array[Long] = Array[Long](commonBlocks.size, e2Blocks.size - commonBlocks.size)
                        val v2: Array[Long] = Array[Long](e1Blocks.size - commonBlocks.size, totalBlocksBD.value - (v1(0) + v1(1) +(e1Blocks.size - commonBlocks.size)) )

                        val chiTest = new ChiSquareTest()
                        val weight = chiTest.chiSquare(Array(v1, v2))
                        ((comparisonID, weight), blockIDs)
                    }
            case Constants.CBS| _ =>
                blocks
                    .map(b => (b.id, b.getComparisonsPairs))
                    .flatMap(b => b._2.map(pair => (pair, ArrayBuffer(b._1))))
                    .reduceByKey(_ ++ _)
                    .map{ c =>
                        val (sourceID, targetID) = c._1
                        val blockIDs = c._2
                        val comparisonID = Utils.bijectivePairing(sourceID, targetID)
                        val e1Blocks = entitiesBlockMapBD.value(sourceID)
                        val e2Blocks = entitiesBlockMapBD.value(targetID)
                        val commonBlocks = e1Blocks.intersect(e2Blocks)
                        ((comparisonID, commonBlocks.size.toDouble), blockIDs)
                    }
        }
    }

}