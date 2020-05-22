package EntityMatching.DistributedAlgorithms

import DataStructures.Block
import EntityMatching.DistributedAlgorithms.prioritization.{BlockCentricPrioritization}//, ComparisonCentricPrioritization, EntityCentricPrioritization}
import org.apache.spark.rdd.RDD
import utils.{Configuration, Constants}

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

object DistributedMatchingFactory {

    def getMatchingAlgorithm(conf: Configuration, blocks: RDD[Block], relation: String, d: (Int, Int),
                             totalBlocks: Long = -1): DistributedMatchingTrait = {
        val algorithm = conf.configurations.getOrElse(Constants.CONF_MATCHING_ALG, Constants.BLOCK_CENTRIC)
        val weightingScheme = conf.configurations.getOrElse(Constants.CONF_WEIGHTING_STRG, Constants.CBS)
        algorithm match {
         /*   case Constants.COMPARISON_CENTRIC =>
                ComparisonCentricPrioritization(totalBlocks, weightingStrategy)
            case Constants.ΕΝΤΙΤΥ_CENTRIC =>
                EntityCentricPrioritization(totalBlocks, weightingStrategy)
           */ case Constants.BLOCK_CENTRIC =>
                BlockCentricPrioritization(blocks, relation, d, totalBlocks, weightingScheme)
            case _=>
                SpatialMatching(blocks, relation)
        }
    }

}
