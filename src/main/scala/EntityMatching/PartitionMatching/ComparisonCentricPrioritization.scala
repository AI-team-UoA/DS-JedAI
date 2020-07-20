package EntityMatching.PartitionMatching


import java.util

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils
import utils.Readers.SpatialReader

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class ComparisonCentricPrioritization(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                                           thetaXY: (Double, Double), ws: WeightStrategy) extends PartitionMatchingTrait {



    def apply(relation: Relation): RDD[(String, String)] = {
        val comparisons = takeBudget(relation, Utils.targetCount*Utils.sourceCount)
        comparisons.filter(_._2).map(_._1)
    }

    /**
     * Get the first budget comparisons, and finds the marches in them.
     *
     * @param relation examined relation
     * @param budget the no comparisons will be implemented
     * @return the number of matches the relation holds
     */
    override def applyWithBudget(relation: Relation, budget: Int): Long = {
        val comparisons = takeBudget(relation, Utils.targetCount*Utils.sourceCount)
        comparisons.take(budget).count(_._2)
    }

    /**
     * First index source and then for each entity of target, find its comparisons from source's index.
     * Weight the comparisons based the weighting scheme and then implement them in a ascending way.
     *
     * The target is most of the matches will be  in the first comparisons.
     *
     * @param relation the examining relation
     * @param budget the number of comparisons to implement
     * @return  an RDD of pair of IDs and boolean that indicate if the relation holds
     */
    def takeBudget(relation: Relation, budget: Long): RDD[((String, String), Boolean)] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val partitionId = p._1
                val source = p._2._1.toArray
                val target = p._2._2.toIterator
                val sourceIndex = index(source, partitionId)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b) && zoneCheck(partitionId)(b)

                target.map(targetSE => (targetSE, targetSE.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))))
                    .filter(_._2.length > 0)
                    .flatMap { case(targetSE: SpatialEntity, sIndices:  Array[((Int, Int), ArrayBuffer[Int])]) =>

                    val frequency = Array.fill(source.length)(0)
                    sIndices.flatMap(_._2).foreach(i => frequency(i) += 1)

                    sIndices.flatMap { case (c, indices) =>
                        indices.map(i => (source(i), frequency(i)))
                            .filter { case (e1, _) => e1.mbb.referencePointFiltering(targetSE.mbb, c, thetaXY) }
                            .map { case (e1, f) => (getWeight(f, e1, targetSE), (e1, targetSE)) }
                    }
                }
            }
            .sortByKey(ascending = false)
            .map(_._2)
            .map(c => ((c._1.originalID, c._2.originalID), c._1.mbb.testMBB(c._2.mbb, relation) && c._1.relate(c._2, relation)))
    }



    def getDE9IM: RDD[IM] ={
        joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val partitionId = p._1
                val source = p._2._1.toArray
                val target = p._2._2.toIterator
                val sourceIndex = index(source, partitionId)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b) && zoneCheck(partitionId)(b)
                val frequencies = new Array[Int](source.length)

                target
                    .map(targetSE => (targetSE, targetSE.index(thetaXY, filteringFunction).map(c => (c, sourceIndex.get(c)))))
                    .filter(_._2.length > 0)
                    .flatMap { case(targetSE: SpatialEntity, sIndices:  Array[((Int, Int), ArrayBuffer[Int])]) =>

                        util.Arrays.fill(frequencies, 0)
                        sIndices.flatMap(_._2).foreach(i => frequencies(i) += 1)
                        sIndices.flatMap { case (c, indices) =>
                            indices.map(i => (source(i), frequencies(i)))
                                .filter { case (e1, _) => e1.mbb.referencePointFiltering(targetSE.mbb, c, thetaXY) && e1.mbb.testMBB(targetSE.mbb, Relation.INTERSECTS )}
                                .map { case (e1, f) => (getWeight(f, e1, targetSE), (e1, targetSE)) }
                        }
                    }
            }
            .map{case (w, c) => (w, IM(c._1, c._2))}
            .sortByKey(ascending = false)
            .map(_._2)
    }

}





/**
 * auxiliary constructor
 */
object ComparisonCentricPrioritization {

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption,
              ws: WeightStrategy): ComparisonCentricPrioritization ={
        val thetaXY = Utils.initTheta(source, target, thetaOption)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)
        ComparisonCentricPrioritization(joinedRDD, thetaXY, ws)
    }

}
