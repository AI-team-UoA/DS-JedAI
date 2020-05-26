package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
//, ComparisonCentricPrioritization, EntityCentricPrioritization}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object BlockMatchingFactory {

    def getMatchingAlgorithm(conf: Configuration, blocks: RDD[Block], d: (Int, Int),
                             totalBlocks: Long = -1): BlockMatchingTrait = {
        val algorithm = conf.configurations.getOrElse(Constants.CONF_MATCHING_ALG, Constants.BLOCK_CENTRIC)
        val weightingScheme = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)
        algorithm match {
            case Constants.BLOCK_CENTRIC =>
                BlockCentricPrioritization(blocks, d, totalBlocks, weightingScheme)
            case _=>
                BlockMatching(blocks)
        }
    }

}
