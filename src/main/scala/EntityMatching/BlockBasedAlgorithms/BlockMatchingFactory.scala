package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import EntityMatching.PartitionMatching.PartitionMatchingFactory.log
//, ComparisonCentricPrioritization, EntityCentricPrioritization}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object BlockMatchingFactory {

    def getMatchingAlgorithm(conf: Configuration, blocks: RDD[Block], d: (Int, Int), totalBlocks: Long = -1): BlockMatchingTrait = {
        val algorithm = conf.getMatchingAlgorithm
        val weightingScheme = conf.getWeightingScheme
        algorithm match {
            case Constants.BLOCK_CENTRIC =>
                log.info("Matching Algorithm: " + Constants.BLOCK_CENTRIC)
                BlockCentricPrioritization(blocks, d, totalBlocks, weightingScheme)
            case _=>
                log.info("Matching Algorithm: " + Constants.RADON)
                BlockMatching(blocks)
        }
    }

}
