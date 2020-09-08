package EntityMatching.PartitionMatching

import DataStructures.IM
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation

trait ProgressiveTrait extends PartitionMatchingTrait{

    val budget: Long
    def getComparisons(relation: Relation): RDD[((String, String), Boolean)]

    /**
     * Get the first budget comparisons, and finds the marches in them.
     *
     * @param relation examined relation
     * @return the number of matches the relation holds
     */
    def applyWithBudget(relation: Relation): Long = getComparisons(relation).take(budget.toInt).count(_._2)

    def apply(relation: Relation): RDD[(String, String)] = getComparisons(relation).filter(_._2).map(_._1)

    def getDE9IMBudget: Iterator[IM] = getDE9IM.toLocalIterator
}
