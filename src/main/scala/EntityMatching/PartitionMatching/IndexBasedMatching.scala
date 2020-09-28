package EntityMatching.PartitionMatching

import DataStructures.{IM, MBB, SpatialEntity}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Utils

import scala.collection.mutable.ListBuffer

case class IndexBasedMatching(source:RDD[SpatialEntity], target:RDD[SpatialEntity], thetaXY: (Double, Double))  {

    val partitionsZones: Array[MBB] = Utils.getZones
    val filteringFunction: ((SpatialEntity, Int), (SpatialEntity, Int), (Int, Int)) => Boolean =
        (e1: (SpatialEntity, Int), e2: (SpatialEntity, Int), c: (Int, Int)) =>
        e1._2 == e2._2 && e1._1.testMBB(e2._1, Relation.INTERSECTS, Relation.TOUCHES) &&
            e1._1.referencePointFiltering(e2._1, c, thetaXY, Some(partitionsZones(e1._2)))

    implicit class TuppleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
        def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
            (p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
    }

    def getDE9IM: RDD[IM] = {
        val indexedSource = source
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(_ ++ _)
        val partitioner = indexedSource.partitioner.get

        val indexedTarget = target
            .map(se => (se.index(thetaXY), (se, TaskContext.getPartitionId())))
            .flatMap{case (indices, (se, pid)) => indices.map(i => (i, ListBuffer((se, pid))))}
            .reduceByKey(partitioner, _ ++ _)

        indexedSource.leftOuterJoin(indexedTarget, partitioner)
            .filter(_._2._2.isDefined)
            .flatMap { case (c: (Int, Int), (s: ListBuffer[(SpatialEntity, Int)], optT: Option[ListBuffer[(SpatialEntity, Int)]])) =>
                for (e1 <- s; e2 <- optT.get; if filteringFunction(e1, e2, c)) yield IM(e1._1, e2._1)
            }
    }

    def countRelationsBlocking: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
        getDE9IM
            .mapPartitions { imIterator =>
                var totalContains = 0
                var totalCoveredBy = 0
                var totalCovers = 0
                var totalCrosses = 0
                var totalEquals = 0
                var totalIntersects = 0
                var totalOverlaps = 0
                var totalTouches = 0
                var totalWithin = 0
                var intersectingPairs = 0
                var interlinkedGeometries = 0
                imIterator.foreach { im =>
                    intersectingPairs += 1
                    if (im.relate) {
                        interlinkedGeometries += 1
                        if (im.isContains) totalContains += 1
                        if (im.isCoveredBy) totalCoveredBy += 1
                        if (im.isCovers) totalCovers += 1
                        if (im.isCrosses) totalCrosses += 1
                        if (im.isEquals) totalEquals += 1
                        if (im.isIntersects) totalIntersects += 1
                        if (im.isOverlaps) totalOverlaps += 1
                        if (im.isTouches) totalTouches += 1
                        if (im.isWithin) totalWithin += 1
                    }
                }

                Iterator((totalContains, totalCoveredBy, totalCovers,
                    totalCrosses, totalEquals, totalIntersects,
                    totalOverlaps, totalTouches, totalWithin,
                    intersectingPairs, interlinkedGeometries))
            }
            .treeReduce { case(t1, t2) => t1 + t2}
    }


}
