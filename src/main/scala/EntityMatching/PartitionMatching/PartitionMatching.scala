package EntityMatching.PartitionMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils
import utils.Readers.SpatialReader


case class PartitionMatching(joinedRDD: RDD[(Int, (Iterable[SpatialEntity],  Iterable[SpatialEntity]))],
                             thetaXY: (Double, Double), ws: WeightStrategy)  extends  PartitionMatchingTrait {

    /**
     * First index the source and then use the index to find the comparisons with target's entities.
     *
     * @param relation the examining relation
     * @return an RDD containing the matching pairs
     */
    def apply(relation: Relation): RDD[(String, String)] ={
        joinedRDD.filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty )
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
                    .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                    .filter{case(c, se) => se.testMBB(targetSE, relation) && se.referencePointFiltering(targetSE, c, thetaXY, Some(partition))}
                    .map(_._2)
                    .filter(se => se.relate(targetSE, relation))
                    .map(se => (se.originalID, targetSE.originalID))
            }
        }
    }


    def getDE9IM: RDD[IM] ={
        joinedRDD.flatMap { p =>
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
                    .filter { case (c, se) => se.testMBB(targetSE, Relation.INTERSECTS, Relation.TOUCHES) && se.referencePointFiltering(targetSE, c, thetaXY, Some(partition)) }
                    .map(_._2)
                    .map(se => IM(se, targetSE))
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
                        .filter { case (c, se) => se.testMBB(targetSE, Relation.INTERSECTS, Relation.TOUCHES) && se.referencePointFiltering(targetSE, c, thetaXY, Some(partition)) }
                        .map(se => (se._2, targetSE))
                }
            }
            .sample(withReplacement = false, frac)

    }
}

/**
 * auxiliary constructor
 */
object PartitionMatching{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption): PartitionMatching ={
        val thetaXY = Utils.getTheta
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)

        // Utils.printPartition(joinedRDD)
        PartitionMatching(joinedRDD, thetaXY, null)
    }
}
