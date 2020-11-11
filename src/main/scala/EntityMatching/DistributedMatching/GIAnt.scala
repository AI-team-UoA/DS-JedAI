package EntityMatching.DistributedMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils


case class GIAnt(joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))],
                 thetaXY: (Double, Double), ws: WeightStrategy) extends DMTrait {

    /**
     * First index the Source and then use the index to find the comparisons with target's entities.
     * Filter the redundant comparisons using testMBB and RF
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: Relation): RDD[(String, String)] = joinedRDD
        .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty )
        .flatMap { p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source: Array[SpatialEntity] = p._2._1.toArray
            val target: Iterator[SpatialEntity] = p._2._2.toIterator
            val sourceIndex = index(source)
            val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

            target.flatMap{ targetSE =>
                targetSE
                    .index(thetaXY, filteringFunction)
                    .view
                    .flatMap(c => sourceIndex.get(c).map(i => (c, i)))
                    .filter{ case(c, i) => source(i).testMBB(targetSE, relation) &&
                                            source(i).referencePointFiltering(targetSE, c, thetaXY, Some(partition))}
                    .filter{ case (_, i) => source(i).relate(targetSE, relation)}
                    .map(_._2)
                    .map(i => (source(i).originalID, targetSE.originalID))
                    .force
            }
        }


    /**
     * compute the Intersection Matrix of the input datasets
     * @return an RDD of intersection matrix
     */
    def getDE9IM: RDD[IM] ={
        joinedRDD.flatMap { p =>
            val pid = p._1
            val partition = partitionsZones(pid)
            val source: Array[SpatialEntity] = p._2._1.toArray
            val target: Iterable[SpatialEntity] = p._2._2
            val sourceIndex = index(source)
            val filteringFunction = (b:(Int, Int)) => sourceIndex.contains(b)

           target.flatMap { targetSE =>
                targetSE
                    .index(thetaXY, filteringFunction)
                    .view
                    .flatMap(c => sourceIndex.get(c).map(i => (c, i)))
                    .filter{case(c, i) => source(i).testMBB(targetSE, Relation.INTERSECTS) &&
                                          source(i).referencePointFiltering(targetSE, c, thetaXY, Some(partition))}
                    .map(_._2)
                    .map(i => IM(source(i), targetSE))
                    .filter(_.relate)
                    .force
            }
        }
    }


    def getSampleDE9IM(frac: Double): RDD[(SpatialEntity, SpatialEntity)] ={
        joinedRDD
            .flatMap { p =>
                val pid = p._1
                val partition = partitionsZones(pid)
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b:(Int, Int)) => sourceIndex.contains(b)

                target.flatMap { targetSE =>
                    targetSE
                        .index(thetaXY, filteringFunction)
                        .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                        .filter { case (c, se) => se.testMBB(targetSE, Relation.INTERSECTS) &&
                                                se.referencePointFiltering(targetSE, c, thetaXY, Some(partition)) }
                        .map(se => (se._2, targetSE))
                }
            }
            .sample(withReplacement = false, frac)
    }
}

/**
 * auxiliary constructor
 */
object GIAnt{

    def apply(source:RDD[(Int, SpatialEntity)], target:RDD[(Int, SpatialEntity)], partitioner: Partitioner): GIAnt ={
        val thetaXY = Utils.getTheta
        val joinedRDD = source.cogroup(target, partitioner)
        GIAnt(joinedRDD, thetaXY, null)
    }
}
