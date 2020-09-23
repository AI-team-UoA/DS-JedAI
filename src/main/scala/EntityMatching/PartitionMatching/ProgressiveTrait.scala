package EntityMatching.PartitionMatching

import DataStructures.IM
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation

trait ProgressiveTrait extends PartitionMatchingTrait{
    val budget: Long

    def apply(relation: Relation): RDD[(String, String)] = {
        val imRDD = getDE9IM
        relation match {
            case Relation.CONTAINS => imRDD.filter(_.isContains).map(_.idPair)
            case Relation.COVEREDBY => imRDD.filter(_.isCoveredBy).map(_.idPair)
            case Relation.COVERS => imRDD.filter(_.isCovers).map(_.idPair)
            case Relation.CROSSES => imRDD.filter(_.isCrosses).map(_.idPair)
            case Relation.INTERSECTS => imRDD.filter(_.isIntersects).map(_.idPair)
            case Relation.TOUCHES => imRDD.filter(_.isTouches).map(_.idPair)
            case Relation.EQUALS => imRDD.filter(_.isEquals).map(_.idPair)
            case Relation.OVERLAPS => imRDD.filter(_.isOverlaps).map(_.idPair)
            case Relation.WITHIN => imRDD.filter(_.isWithin).map(_.idPair)
        }
    }

    def getDE9IM: RDD[IM]

    def getWeightedDE9IM: RDD[(Double, IM)]
}
