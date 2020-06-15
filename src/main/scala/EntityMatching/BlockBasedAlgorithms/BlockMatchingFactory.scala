package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import EntityMatching.PartitionMatching.PartitionMatchingFactory.log
import utils.Constants.{BlockingAlgorithm, MatchingAlgorithm}
//, ComparisonCentricPrioritization, EntityCentricPrioritization}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object BlockMatchingFactory {

    def getMatchingAlgorithm(conf: Configuration, blocks: RDD[Block], d: (Int, Int), totalBlocks: Long = -1): BlockMatchingTrait = {
        val algorithm = conf.getMatchingAlgorithm
        val wc = conf.getWeightingScheme
        algorithm match {
            case MatchingAlgorithm.BLOCK_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.BLOCK_CENTRIC)
                BlockCentricPrioritization(blocks, d, totalBlocks, wc)
            case _=>
                log.info("Matching Algorithm: " + BlockingAlgorithm.RADON)
                BlockMatching(blocks)
        }
    }

}
