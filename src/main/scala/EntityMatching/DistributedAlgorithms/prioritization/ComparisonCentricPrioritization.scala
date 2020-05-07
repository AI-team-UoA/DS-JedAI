package EntityMatching.DistributedAlgorithms.prioritization

import Blocking.BlockUtils.clean
import DataStructures.{Block, TBlock}
import EntityMatching.DistributedAlgorithms.DistributedMatchingTrait
import org.apache.spark.rdd.RDD
import utils.Constants

import scala.collection.mutable.ArrayBuffer

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */


case class ComparisonCentricPrioritization(totalBlocks: Long, weightingStrategy: String) extends DistributedMatchingTrait  {


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
    RDD[(String,String)] = {

        val weightedComparisonsPerBlock = getWeights(blocks.asInstanceOf[RDD[TBlock]])
            .asInstanceOf[RDD[(Any, ArrayBuffer[Long])]]

        val cleanWeightedComparisonsPerBlock = clean(weightedComparisonsPerBlock, cleaningStrategy)
            .asInstanceOf[RDD[(Long, ArrayBuffer[(Long, Double)])]]
            .filter(_._2.nonEmpty)

        val blocksComparisons = blocks.map(b => (b.id, b))
        blocksComparisons.leftOuterJoin(cleanWeightedComparisonsPerBlock)
            .filter(_._2._2.isDefined)
            .flatMap {
                b =>
                    val comparisonsWeightsMap = b._2._2.get.toMap
                    val comparisons = b._2._1.getComparisons
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
                    .sortBy(_._1)(Ordering.Double.reverse)
                    .map(_._2)
                    .filter(c => testMBB(c.entity1.mbb, c.entity2.mbb, relation))
                    .filter(c => relate(c.entity1.geometry, c.entity2.geometry, relation))
                    .map(c => (c.entity1.originalID, c.entity2.originalID))
                    .toIterator
            }
    }

}
