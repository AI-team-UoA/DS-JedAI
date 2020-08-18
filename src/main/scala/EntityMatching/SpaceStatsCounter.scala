package EntityMatching

import DataStructures.{IM, MBB, SpatialEntity, SpatialIndex}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.ThetaOption.ThetaOption
import utils.Readers.SpatialReader
import utils.Utils

import scala.collection.mutable.ArrayBuffer

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
        val tiles = commonTiles.map{ case(c, (n1, n2)) => n1 }.sum()
        log.info("Tiles: " + tiles)

        //val pairTiles = commonTiles.map{ case(c, (n1, n2)) => n1*n2}.sum()
        //log.info("Pairs Tiles: " + pairTiles)

        sourceTiles.unpersist()
        targetTiles.unpersist()
        commonTiles.unpersist()

        // val tilesSE = source.flatMap(se => se.index(thetaXY).map(c => (c, ArrayBuffer(se.originalID)))).reduceByKey(_ ++ _)
        // val tilesTE = target.flatMap(se => se.index(thetaXY).map(c => (c, ArrayBuffer(se.originalID)))).reduceByKey(_ ++ _)
        // val joinedTiles = tilesSE.leftOuterJoin(tilesTE).filter(_._2._2.isDefined).setName("Joined").cache()
        //val uniqueTiles = joinedTiles.flatMap{case(c, (sse, tse)) => sse.map(se => (se, tse.get)).groupBy(_._1).mapValues(p => p.flatMap(_._2.toSet))}.reduceByKey(_ ++ _).map(_._2.toSet.size).sum

        val pairTilesRDD = joinedRDD
            .filter(p => p._2._1.nonEmpty && p._2._2.nonEmpty)
            .flatMap { p =>
                val source: Array[SpatialEntity] = p._2._1.toArray
                val target: Iterator[SpatialEntity] = p._2._2.toIterator
                val sourceIndex = index(source)
                val filteringFunction = (b: (Int, Int)) => sourceIndex.contains(b)
                target.flatMap { targetSE =>
                    targetSE.index(thetaXY, filteringFunction).map(c => (targetSE.originalID, sourceIndex.get(c).map(j => source(j).originalID)))
                }
            }.setName("pairTilesRDD").cache()

        // not the exact number but an approach
        log.info("Pair Tiles: "+ pairTilesRDD.map( p => (p._1, p._2.size)).reduceByKey(_ + _).map(_._2).sum())
        log.info("Unique Tiles: " + pairTilesRDD.reduceByKey(_.distinct ++ _.distinct).map(_._2.distinct.size).sum)

        source.unpersist()
        target.unpersist()
        pairTilesRDD.unpersist()

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
            }.setName("ComparisonsRDD").cache()

        val intersectingTiles = comparisonsRDD.filter{ case (sSE, tSE) => sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}
        val truePairs = comparisonsRDD.filter{ case (sSE, tSE) => sSE.testMBB(tSE, Relation.INTERSECTS, Relation.TOUCHES)}.filter{case (sSE, tSE) => IM(sSE, tSE).relate}

        log.info("Intersecting Pairs: " + intersectingTiles.count())
        log.info("True Pairs: " + truePairs.count())
        log.info("")

        comparisonsRDD.unpersist()

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

