package linkers

import model.{IM, SpatialIndex, TileGranularities}
import model.entities.Entity
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

trait LinkerT {

    val source: Array[Entity]
    val target: Iterable[Entity]
    val tileGranularities: TileGranularities
    val partitionBorder: Envelope

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
        val (rfX, rfY) = EnvelopeOp.getReferencePoint(env1, env2, tileGranularities)

        val blockContainsRF: Boolean =  b._1 <= rfX && b._1+1 >= rfX && b._2 <= rfY && b._2+1 >= rfY
        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX >= rfX && partition.getMinY <= rfY && partition.getMaxY >= rfY
        blockContainsRF && partitionContainsRF
    }

    def referencePointFiltering(s: Entity, t: Entity, partition: Envelope): Boolean ={
        val env1 = s.env
        val env2 = t.env
        val (rfX, rfY) = EnvelopeOp.getReferencePoint(env1, env2, tileGranularities)

        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX >= rfX && partition.getMinY <= rfY && partition.getMaxY >= rfY
        partitionContainsRF
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
    def countVerification: Long = {
        val sourceIndex = SpatialIndex(source, tileGranularities)
        target.flatMap(t => getAllCandidates(t, sourceIndex, partitionBorder, Relation.DE9IM)).size
    }

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
    def getAllCandidates(se: Entity, index: SpatialIndex[Entity], partition: Envelope, relation: Relation): Seq[Entity] ={
        index.index(se)
            .flatMap { block =>
                val blockCandidates = index.get(block)
                blockCandidates.filter(candidate => `filterVerifications`(candidate, se, relation, block, partition))
            }
    }

    def relate(relation: Relation): Iterator[(String, String)]

    def getDE9IM: Iterator[IM]
}
