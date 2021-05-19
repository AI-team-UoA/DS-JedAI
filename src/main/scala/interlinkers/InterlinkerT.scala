package interlinkers

import model.entities.Entity
import model.{IM, SpatialIndex, TileGranularities}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.Constants.Relation
import utils.Constants.Relation.Relation

import scala.math.{max, min}
import cats.implicits._

trait InterlinkerT {

    val joinedRDD: RDD[(Int, (Iterable[Entity], Iterable[Entity]))]
    val tileGranularities: TileGranularities
    val partitionBorders: Array[Envelope]

    val weightOrdering: Ordering[(Double, (Entity, Entity))] = Ordering.by[(Double, (Entity, Entity)), Double](_._1).reverse

    /**
     * Return true if the reference point is inside the block and inside the partition
     * The reference point is the upper left point of their intersection
     *
     * @param s source entity
     * @param t target entity
     * @param b block
     * @param partition current partition
     * @return true if the reference point is in the block and in partition
     */
    def referencePointFiltering(s: Entity, t: Entity, b:(Int, Int), partition: Envelope): Boolean ={

        val env1 = s.env
        val env2 = t.env
        val epsilon = 1e-8

        val minX1 = env1.getMinX /tileGranularities.x
        val minX2 = env2.getMinX /tileGranularities.x
        val maxY1 = env1.getMaxY /tileGranularities.y
        val maxY2 = env2.getMaxY /tileGranularities.y

        val rfX: Double = max(minX1, minX2)+epsilon
        val rfY: Double = min(maxY1, maxY2)+epsilon

        val blockContainsRF: Boolean =  b._1 <= rfX && b._1+1 >= rfX && b._2 <= rfY && b._2+1 >= rfY
        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX >= rfX && partition.getMinY <= rfY && partition.getMaxY >= rfY
        blockContainsRF && partitionContainsRF
    }


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
    def filterVerifications(s: Entity, t: Entity, relation: Relation, block: (Int, Int), partition: Envelope): Boolean =
        s.intersectingMBR(t, relation) && referencePointFiltering(s, t, block, partition)

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
    def getAllCandidates(se: Entity, index: SpatialIndex, partition: Envelope, relation: Relation): Seq[Entity] ={
        index.indexEntity(se)
            .flatMap { block =>
                val blockCandidates = index.get(block)
                blockCandidates.filter(candidate => filterVerifications(candidate, se, relation, block, partition))
            }
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
            .treeReduce({ case (im1, im2) => im1 |+| im2}, 4)

    def take(budget: Int): (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) =
        getDE9IM
            .mapPartitions { imIterator => Iterator(accumulate(imIterator)) }
            .take(budget).reduceLeft(_ |+| _)

    def countRelation(relation: Relation): Long = relate(relation).count()

    def relate(relation: Relation): RDD[(String, String)]

    def getDE9IM: RDD[IM]
}
