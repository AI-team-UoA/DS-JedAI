package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import org.apache.spark.rdd.RDD

trait BlockMatchingTrait {
    val blocks: RDD[Block]

    def apply(relation: String): RDD[(String, String)]
}
