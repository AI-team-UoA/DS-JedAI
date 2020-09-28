package EntityMatching.PartitionMatching

import DataStructures.{IM, MBB, SpatialEntity, SpatialIndex}
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Constants.WeightStrategy
import utils.Constants.WeightStrategy.WeightStrategy
import utils.Utils


trait PartitionMatchingTrait {

    val orderByWeight: Ordering[(Double, (SpatialEntity, SpatialEntity))] = Ordering.by[(Double, (SpatialEntity, SpatialEntity)), Double](_._1).reverse

    val joinedRDD: RDD[(Int, (Iterable[SpatialEntity], Iterable[SpatialEntity]))]
    val thetaXY: (Double, Double)
    val ws: WeightStrategy

    val partitionsZones: Array[MBB] = Utils.getZones
    val spaceEdges: MBB = Utils.getSpaceEdges

    val totalBlocks: Double = if (ws == WeightStrategy.ECBS || ws == WeightStrategy.PEARSON_X2){
        val globalMinX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minX/thetaXY._1)).min()
        val globalMaxX = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxX/thetaXY._1)).max()
        val globalMinY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.minY/thetaXY._2)).min()
        val globalMaxY = joinedRDD.flatMap(p => p._2._1.map(_.mbb.maxY/thetaXY._2)).max()
        (globalMaxX - globalMinX + 1) * (globalMaxY - globalMinY + 1)
    } else -1


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

    implicit class TuppleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
        def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
            (p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
    }

    def countRelations: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
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
            .treeReduce { case (im1, im2) => im1 + im2}
    }

    def apply(relation: Relation): RDD[(String, String)]

    def getDE9IM: RDD[IM]
}


