package interlinkers

import model.entities.Entity
import model.{IM, MBR, SpatialIndex, TileGranularities}
import org.apache.spark.rdd.RDD
import utils.Constants.Relation
import utils.Constants.Relation.Relation

trait InterlinkerT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))]
    val tileGranularities: TileGranularities
    val partitionBorders: Array[MBR]

    val weightOrdering: Ordering[(Double, (Entity, Entity))] = Ordering.by[(Double, (Entity, Entity)), Double](_._1).reverse


    /**
     * filter redundant verifications based on spatial criteria
     *
     * @param s source spatial entity
     * @param t source spatial entity
     * @param relation examining relation
     * @param block block the comparison belongs to
     * @param partition the partition the comparisons belong to
     * @return true if comparison is necessary
     */
    def filterVerifications(s: Entity, t: Entity, relation: Relation, block: (Int, Int), partition: MBR): Boolean =
        s.intersectingMBR(t, relation) && s.referencePointFiltering(t, block, tileGranularities, partition)

    /**
     * count all the necessary verifications
     * @return number of verifications
     */
    def countVerification: Long =
        joinedRDD.filter(j => j._2._1.nonEmpty && j._2._2.nonEmpty)
            .flatMap { p =>
                val pid = p._1
                val partition = partitionBorders(pid)
                val source: Array[Entity] = p._2._1.toArray
                val target: Iterable[Entity] = p._2._2
                val sourceIndex = SpatialIndex(source, tileGranularities)

                target.flatMap(t => getAllCandidates(t, sourceIndex, partition, Relation.DE9IM))
            }.count()


    /**
     *  Given a spatial index, retrieve all candidate geometries and filter based on
     *  spatial criteria
     *
     * @param se target Spatial entity
     * @param index spatial index
     * @param partition current partition
     * @param relation examining relation
     * @return all candidate geometries of se
     */
    def getAllCandidates(se: Entity, index: SpatialIndex, partition: MBR, relation: Relation): Seq[Entity] ={
        index.indexEntity(se)
            .flatMap { block =>
                val blockCandidatesOpt = index.get(block)
                blockCandidatesOpt match {
                    case Some(blockCandidates) =>
                        val filteredBlockCandidates = blockCandidates.filter(e => filterVerifications(e, se, relation, block, partition))
                        Some(filteredBlockCandidates)
                    case _ => None
                }
            }.flatten
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
