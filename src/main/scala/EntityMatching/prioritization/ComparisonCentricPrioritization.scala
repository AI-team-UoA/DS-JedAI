package EntityMatching.prioritization

import Blocking.BlockUtils.clean
import DataStructures.{Block, TBlock}
import EntityMatching.Matching.{relate, testMBB}
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */


case class ComparisonCentricPrioritization(totalBlocks: Long, weightingStrategy: String) extends  PrioritizationTrait  {


    /**
     * First for each block compute the weight of each comparison and to which block
     * it will be assigned to (clean). Then in each partition, order its comparisons
     * according to their weights and then execute them to find matches.
     *
     * During the matching, first test the relation to geometries's MBBs and then
     * performs the relation to the geometries
     *
     * @param blocks the input Blocks
     * @param relation the examined relation
     * @param cleaningStrategy  the cleaning strategy
     * @return an RDD containing the IDs of the matches
     */
    def apply(blocks: RDD[Block], relation: String, cleaningStrategy: String = Constants.RANDOM):
    RDD[(Int,Int)] = {

        val weightedComparisonsPerBlock = getWeights(blocks.asInstanceOf[RDD[TBlock]])
            .asInstanceOf[RDD[(Any, ArrayBuffer[Long])]]

        val cleanWeightedComparisonsPerBlock = clean(weightedComparisonsPerBlock, cleaningStrategy)
            .asInstanceOf[RDD[(Long, ArrayBuffer[(Long, Double)])]]
            .filter(_._2.nonEmpty)

        val blocksComparisons = blocks.map(b => (b.id, b))
        cleanWeightedComparisonsPerBlock
            .leftOuterJoin(blocksComparisons)
            .flatMap {
                b =>
                    val comparisonsWeightsMap = b._2._1.toMap
                    val comparisons = b._2._2.get.getComparisons
                    comparisons
                        .filter(c => comparisonsWeightsMap.contains(c.id))
                        .map { c =>
                            val weight = comparisonsWeightsMap(c.id)
                            val env1 = c.entity1.geometry.getEnvelope
                            val env2 = c.entity2.geometry.getEnvelope
                            val normalizedWeight = normalizeWeight(weight, env1, env2)
                            (normalizedWeight, c)
                        }
            }
            .mapPartitions { comparisonsIter =>
                comparisonsIter
                    .toArray
                    .sortBy(_._1)
                    .map(_._2)
                    .filter(c => testMBB(c.entity1.mbb, c.entity2.mbb, relation))
                    .filter(c => relate(c.entity1.geometry, c.entity2.geometry, relation))
                    .map(c => (c.entity1.id, c.entity2.id))
                    .toIterator
            }
    }


    /**
     * Weight the comparisons of blocks and clean the duplicate comparisons.
     * The accepted weighing strategies are CBS(default), ECBS, JS and PEARSON_X
     *
     * @param blocks blocks RDD
     * @return the weighted comparisons of each block
     */
    def getWeights(blocks: RDD[TBlock]): RDD[((Long, Double), ArrayBuffer[Long])] ={
        val sc = SparkContext.getOrCreate()
        val totalBlocksBD = sc.broadcast(totalBlocks)

        val entitiesBlockMapBD =
            if (weightingStrategy != Constants.CBS ){
                val ce1:RDD[(Int, Long)] = blocks.flatMap(b => b.getSourceIDs.map(id => (id, b.id)))
                val ce2:RDD[(Int, Long)] = blocks.flatMap(b => b.getTargetIDs.map(id => (id, b.id)))
                val ce = ce1.union(ce2)
                    .map(c => (c._1, ArrayBuffer(c._2)))
                    .reduceByKey(_ ++ _)
                    .sortByKey()
                    .map(c => (c._1, c._2.toSet))
                    .collectAsMap()
                sc.broadcast(ce)
            }
            else null

        weightingStrategy match {
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
