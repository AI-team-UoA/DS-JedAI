package EntityMatching.DistributedAlgorithms

import DataStructures.Block
import EntityMatching.MatchingTrait
import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.{Constants, Utils}

import scala.collection.mutable.ArrayBuffer

trait DistributedMatchingTrait extends MatchingTrait{
    val blocks: RDD[Block]
    val relation: String

    def apply(): RDD[(String, String)]

}
