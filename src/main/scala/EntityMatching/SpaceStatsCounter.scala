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

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


case class SpaceStatsCounter(joinedRDD: RDD[(Int, (Iterable[SpatialEntity],  Iterable[SpatialEntity]))], thetaXY: (Double, Double)){

    val partitionsZones: Array[MBB] = Utils.getZones
    val spaceEdges: MBB = Utils.getSpaceEdges

    def printSpaceInfo(): Unit ={
        val log = LogManager.getRootLogger
        log.setLevel(Level.INFO)

        val source: RDD[SpatialEntity] = joinedRDD.flatMap(_._2._1.map(se => (se.originalID, se))).distinct().map(_._2).setName("Source").cache()
        val target: RDD[SpatialEntity] = joinedRDD.flatMap(_._2._2.map(se => (se.originalID, se))).distinct().map(_._2).setName("target").cache()

        val sourceTiles: RDD[(Int, Int)] = source.flatMap(se => se.index(thetaXY)).setName("SourceTiles").cache()
        val targetTiles: RDD[(Int, Int)] = target.flatMap(se => se.index(thetaXY)).setName("TargetTiles").cache()

        val ssePerTile: RDD[((Int, Int), Int)] = sourceTiles.map((_,1)).reduceByKey(_ + _)
        val tsePerTile: RDD[((Int, Int), Int)] = targetTiles.map((_,1)).reduceByKey(_ + _)

        val commonTiles = ssePerTile.join(tsePerTile).setName("CommonTiles").cache()
        val tiles = commonTiles.map{ case(_, (n1, n2)) => n2}.sum()
        log.info("Tiles: " + tiles)

        val pairTiles = commonTiles.map{ case(c, (n1, n2)) => n1*n2}.sum()
        log.info("Pairs Tiles: " + pairTiles)

        sourceTiles.unpersist()
        targetTiles.unpersist()
        commonTiles.unpersist()

        val tilesSE = source.flatMap(se => se.index(thetaXY).map(c => (c, ListBuffer(se.originalID)))).reduceByKey(_ ++ _)
        val tilesTE = target.flatMap(se => se.index(thetaXY).map(c => (c, ListBuffer(se.originalID)))).reduceByKey(_ ++ _)
        val joinedTiles = tilesSE.rightOuterJoin(tilesTE).filter(_._2._1.isDefined).setName("Joined").cache()
        val uniquePairs = joinedTiles.flatMap{case(_, (sse, tse)) => tse.map(se => (se, sse.get))}.reduceByKey(_ ++ _).map(_._2.distinct.size).sum
        log.info("Unique Tiles: " + uniquePairs)

        joinedTiles.unpersist()
        source.unpersist()
        target.unpersist()

        val comparisonsRDD: RDD[(SpatialEntity, SpatialEntity)] = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                val pid = p._1
                val partition = partitionsZones(pid)

                target.flatMap { targetSE =>
                    targetSE
                        .index(thetaXY, filteringFunction)
                        .flatMap(c => sourceIndex.get(c).map(j => (c, source(j))))
                        .filter{case(c, se) => se.referencePointFiltering(targetSE, c, thetaXY, Some(partition))}
                        .map {case(_, se) => (se, targetSE)}
                }
            }.setName("ComparisonsRDD")

        val intersectingTiles = comparisonsRDD.filter{ case (sSE, tSE) => sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}
        val truePairs = comparisonsRDD.filter{ case (sSE, tSE) => sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}.filter{case (sSE, tSE) => IM(sSE, tSE).relate}

        log.info("Intersecting Pairs: " + intersectingTiles.count())
        log.info("True Pairs: " + truePairs.count())
        log.info("")
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
            val indices: Seq[(Int, Int)] = se.index(thetaXY)
            indices.foreach(i => spatialIndex.insert(i, index))
        }
        spatialIndex
    }


}
object SpaceStatsCounter{

    def apply(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaOption: ThetaOption): SpaceStatsCounter ={
        val thetaXY = Utils.getTheta
        val sourcePartitions = source.map(se => (TaskContext.getPartitionId(), se))
        val targetPartitions = target.map(se => (TaskContext.getPartitionId(), se))

        val joinedRDD = sourcePartitions.cogroup(targetPartitions, SpatialReader.spatialPartitioner)

        SpaceStatsCounter(joinedRDD, thetaXY)
    }
}

