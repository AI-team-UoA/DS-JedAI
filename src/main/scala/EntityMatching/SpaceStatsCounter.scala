package EntityMatching

import DataStructures.{IM, MBB, SpatialEntity, SpatialIndex}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import utils.Constants.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Readers.SpatialReader
import utils.Utils

case class SpaceStatsCounter(joinedRDD: RDD[(Int, (Iterable[SpatialEntity],  Iterable[SpatialEntity]))], thetaXY: (Double, Double)){

    val partitionsZones: Array[MBB] = Utils.getZones
    val spaceEdges: MBB = Utils.getSpaceEdges

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
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)

                target.flatMap { targetSE =>
                    targetSE
                        .index(thetaXY, filteringFunction)
                        .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                        .map(p => (p._2.originalID+p._1._1 +"-"+ targetSE.originalID+p._1._2, (p._1, (p._2, targetSE))))
                }
            }
            .reduceByKey((_, b) => b)
            .map(_._2)
            .persist(StorageLevel.MEMORY_AND_DISK)

        val pairsTiles = allComparisonsRDD.count()
        val uniquePairs = allComparisonsRDD.filter{case (c, (sSE, tSE)) => sSE.referencePointFiltering(tSE, c, thetaXY, Some(partitionsZones(TaskContext.getPartitionId())))}.count()
        val intersectingPairs = allComparisonsRDD.filter{case (_, (sSE, tSE)) => sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}.count()
        val truePairs = allComparisonsRDD
            .filter{case (c, (sSE, tSE)) => sSE.referencePointFiltering(tSE, c, thetaXY, Some(partitionsZones(TaskContext.getPartitionId()))) && sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}
            .filter{case (_, (sSE, tSE)) => IM(sSE, tSE).relate}
            .map { case (c, (sSE, tSE)) => (c, sSE.originalID, tSE.originalID)}
            .distinct()
            .count()
        log.info("Pairs Tiles: " + pairsTiles)
        log.info("Unique Pairs: " + uniquePairs)
        log.info("Intersecting Pairs: " + intersectingPairs)
        log.info("True Pairs: " + truePairs)
    }

    /**
     * index a list of spatial entities
     *
     * @param entities list of spatial entities
     * @return a SpatialIndex
     */
    def index(entities: Array[SpatialEntity]): SpatialIndex = {
        val spatialIndex = new SpatialIndex()
        entities.zipWithIndex.foreach { case (se, index) =>
            val indices: Array[(Int, Int)] = se.index(thetaXY)
            indices.foreach(i => spatialIndex.insert(i, index))
        }
        spatialIndex
    }


}
object SpaceStatsCounter{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption): SpaceStatsCounter ={
        val thetaXY = Utils.initTheta(source, target, thetaOption)
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)

        SpaceStatsCounter(joinedRDD, thetaXY)
    }
}

