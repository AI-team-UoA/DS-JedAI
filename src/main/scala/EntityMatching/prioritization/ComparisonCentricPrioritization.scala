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
    RDD[(Long,Long)] = {

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
        val sourceFrequenciesMap = blocks
            .flatMap(b => b.getSourceIDs.map(id => (id, b.getTargetSize())))
            .reduceByKey(_ + _)
            .collectAsMap()
        val sourceFrequenciesMapBD = sc.broadcast(sourceFrequenciesMap)

        val entitiesBlockMapBD =
            if (weightingStrategy != Constants.CBS ){
                val ce1:RDD[(Long, Long)] = blocks.flatMap(b => b.getSourceIDs.map(id => (id, b.id)))
                val ce2:RDD[(Long, Long)] = blocks.flatMap(b => b.getTargetIDs.map(id => (id, b.id)))
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
                        val entity1Frequency = sourceFrequenciesMapBD.value(sourceID)
                        val noOfBlocks1 = entitiesBlockMapBD.value(sourceID).size
                        val noOfBlocks2 = entitiesBlockMapBD.value(targetID).size
                        val weight = entity1Frequency * math.log10(totalBlocksBD.value/noOfBlocks1) * math.log10(totalBlocksBD.value/noOfBlocks2)
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
                        val entity1Frequency = sourceFrequenciesMapBD.value(sourceID)
                        val noOfBlocks1 = entitiesBlockMapBD.value(sourceID).size
                        val noOfBlocks2 = entitiesBlockMapBD.value(targetID).size
                        val denominator = (noOfBlocks1 + noOfBlocks2 - entity1Frequency) // WARNING: how do we ensure the denominator is non zero
                        val weight = entity1Frequency / denominator
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
                        val entity1Frequency = sourceFrequenciesMapBD.value(sourceID)
                        val noOfBlocks1 = entitiesBlockMapBD.value(sourceID).size
                        val noOfBlocks2 = entitiesBlockMapBD.value(targetID).size

                        val v1: Array[Long] = Array[Long](entity1Frequency, noOfBlocks2 - entity1Frequency)
                        val v2: Array[Long] = Array[Long](noOfBlocks1 - entity1Frequency, totalBlocksBD.value - (v1(0) + v1(1) +(noOfBlocks1 - entity1Frequency)) )

                        val chiTest = new ChiSquareTest()
                        val weight = chiTest.chiSquare(Array(v1, v2)) //WARNING NoPositiveNumber Exception
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
                        val entity1Frequency = sourceFrequenciesMapBD.value(sourceID)
                        ((comparisonID, entity1Frequency.toDouble), blockIDs)
                    }
        }
    }


}
