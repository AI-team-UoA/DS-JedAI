package EntityMatching.BlockBasedAlgorithms

import DataStructures.Block
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation


/**
 *  Link discovery
 */
case class BlockMatching(blocks:RDD[Block]) extends BlockMatchingTrait {


	/**
	 * Perform the only the necessary comparisons
	 * @return the original ids of the matches
	 */
	def apply(relation: Relation): RDD[(String, String)] ={
		blocks.flatMap(b => b.getFilteredComparisons(relation))
			.filter(c => c._1.relate(c._2, relation))
			.map(c => (c._1.originalID, c._2.originalID))
	}

}
