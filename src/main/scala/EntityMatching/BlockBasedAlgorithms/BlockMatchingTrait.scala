package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation

trait BlockMatchingTrait {
    val blocks: RDD[Block]

    def apply(relation: Relation): RDD[(String, String)]
}
