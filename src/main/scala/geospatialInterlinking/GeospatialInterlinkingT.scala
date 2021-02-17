package geospatialInterlinking

import dataModel.{Entity, IM, MBR, SpatialIndex}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.Constants.Relation.Relation
import utils.Utils

trait GeospatialInterlinkingT {

    val orderByWeight: Ordering[(Double, (Entity, Entity))] = Ordering.by[(Double, (Entity, Entity)), Double](_._1).reverse

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))]
    val thetaXY: (Double, Double)

    val partitionsZones: Array[MBR] = SparkContext.getOrCreate().broadcast(Utils.getZones).value
    val spaceEdges: MBR = SparkContext.getOrCreate().broadcast(Utils.getSpaceEdges).value


    /**
     * index a list of spatial entities
     *
     * @param entities list of spatial entities
     * @return a SpatialIndex
     */
    def index(entities: Array[Entity]): SpatialIndex = {
        val spatialIndex = new SpatialIndex()
        entities.zipWithIndex.foreach { case (se, i) =>
            val indices: Seq[(Int, Int)] = se.index(thetaXY)
            indices.foreach(c => spatialIndex.insert(c, i))
        }
        spatialIndex
    }

    implicit class TupleAdd(t: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)) {
        def +(p: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
            (p._1 + t._1, p._2 + t._2, p._3 +t._3, p._4+t._4, p._5+t._5, p._6+t._6, p._7+t._7, p._8+t._8, p._9+t._9, p._10+t._10, p._11+t._11)
    }

    def accumulate(imIterator: Iterator[IM]): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) ={
        var totalContains: Int = 0
        var totalCoveredBy: Int = 0
        var totalCovers: Int = 0
        var totalCrosses: Int = 0
        var totalEquals: Int = 0
        var totalIntersects: Int = 0
        var totalOverlaps: Int = 0
        var totalTouches: Int = 0
        var totalWithin: Int = 0
        var verifications: Int = 0
        var qualifiedPairs: Int = 0
        imIterator.foreach { im =>
            verifications += 1
            if (im.relate) {
                qualifiedPairs += 1
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
        (totalContains, totalCoveredBy, totalCovers, totalCrosses, totalEquals, totalIntersects,
            totalOverlaps, totalTouches, totalWithin, verifications, qualifiedPairs)

    }

    def countAllRelations: (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        getDE9IM
            .mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
            .treeReduce({ case (im1, im2) => im1 + im2}, 4)

    def take(budget: Int): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        getDE9IM
            .mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
            .take(budget).reduceLeft(_ + _)

    def countRelation(relation: Relation): Long = relate(relation).count()

    def relate(relation: Relation): RDD[(String, String)]

    def getDE9IM: RDD[IM]
}
