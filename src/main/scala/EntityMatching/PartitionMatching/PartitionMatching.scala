package EntityMatching.PartitionMatching

import DataStructures.{IM, SpatialEntity}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
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
            val partitionId = p._1
            val source: Array[SpatialEntity] = p._2._1.toArray
            val target: Iterator[SpatialEntity] = p._2._2.toIterator
            val sourceIndex = index(source, partitionId)
            val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b) && zoneCheck(partitionId)(b)

            target.flatMap{ targetSE =>
                targetSE
                    .index(thetaXY, filteringFunction)
                    .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                    .filter{case(c, se) => se.mbb.testMBB(targetSE.mbb, relation)  && se.mbb.referencePointFiltering(targetSE.mbb, c, thetaXY)}
                    .map(_._2)
                    .filter(se => se.relate(targetSE, relation))
                    .map(se => (se.originalID, targetSE.originalID))
            }
        }
    }

    def getDE9IM: RDD[IM] ={
        joinedRDD.flatMap { p =>
            val partitionId = p._1
            val source: Array[SpatialEntity] = p._2._1.toArray
            val target: Iterator[SpatialEntity] = p._2._2.toIterator
            val sourceIndex = index(source, partitionId)
            val filteringFunction = (b:(Int, Int)) => sourceIndex.contains(b) && zoneCheck(partitionId)(b)

            target.flatMap { targetSE =>
                targetSE
                    .index(thetaXY, filteringFunction)
                    .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                    .filter { case (c, se) => se.mbb.testMBB(targetSE.mbb, Relation.INTERSECTS) && se.mbb.referencePointFiltering(targetSE.mbb, c, thetaXY) }
                    .map(_._2)
                    .map(se => IM(se, targetSE))
            }
        }
    }

    def printSpaceInfo(): Unit ={
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        // todo adjust on theta
        val mbbRDD = joinedRDD.flatMap(p => p._2._1.map(se => se.mbb) ++ p._2._2.map(se => se.mbb)).persist(StorageLevel.MEMORY_AND_DISK)
        val mbbMinX = math.floor(mbbRDD.map(mbb => mbb.minX).min())
        val mbbMinY = math.floor(mbbRDD.map(mbb => mbb.minY).min())
        val mbbMaxX = math.ceil(mbbRDD.map(mbb => mbb.maxX).max())
        val mbbMaxY = math.ceil(mbbRDD.map(mbb => mbb.maxY).max())
        val totalTiles = (mbbMaxX - mbbMinX)*(mbbMaxY - mbbMinY)
        log.info("Total Tiles: " + totalTiles)
        mbbRDD.unpersist()
    
        val allComparisonsRDD: RDD[((Int, Int), (SpatialEntity, SpatialEntity))] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val partitionId = p._1
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source, partitionId)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b) && zoneCheck(partitionId)(b)

                target.flatMap { targetSE =>
                    targetSE
                        .index(thetaXY, filteringFunction)
                        .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                        .map(p => (p._1, (p._2, targetSE)))
                }
            }

        val pairsTiles = allComparisonsRDD.count()
        val uniquePairs = allComparisonsRDD.filter{case (c, (sSE, tSE)) => sSE.mbb.referencePointFiltering(tSE.mbb, c, thetaXY)}.count()
        val intersectingPairs = allComparisonsRDD.filter{case (c, (sSE, tSE)) => sSE.mbb.testMBB(tSE.mbb, Relation.INTERSECTS)}.count()
        val truePairs = allComparisonsRDD
            .filter{case (c, (sSE, tSE)) => sSE.mbb.referencePointFiltering(tSE.mbb, c, thetaXY) && sSE.mbb.testMBB(tSE.mbb, Relation.INTERSECTS)}
            .filter{case (c, (sSE, tSE)) => IM(sSE, tSE).relate}
            .count()
        log.info("Pairs Tiles: " + pairsTiles)
        log.info("Unique Pairs: " + uniquePairs)
        log.info("Intersecting Pairs: " + intersectingPairs)
        log.info("True Pairs: " + truePairs)
    }
}

/**
 * auxiliary constructor
 */
object PartitionMatching{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption): PartitionMatching ={
        val thetaXY = Utils.initTheta(source, target, thetaOption)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)

        //Utils.printPartition(joinedRDD)
        PartitionMatching(joinedRDD, thetaXY, null)
    }
}
