package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import EntityMatching.MatchingTrait
import org.apache.spark.rdd.RDD

trait BlockMatchingTrait extends MatchingTrait{
    val blocks: RDD[Block]

    def apply(relation: String): RDD[(String, String)]
}
