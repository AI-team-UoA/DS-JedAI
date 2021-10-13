package linkers

import model.entities.EntityT
import model.structures.SpatialIndex
import model.{IM, TileGranularities, structures}
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation.Relation

trait LinkerT {

    val source: Array[EntityT]
    val target: Iterable[EntityT]
    val tileGranularities: TileGranularities
    val partitionBorder: Envelope

    val weightOrdering: Ordering[(Double, (EntityT, EntityT))] = Ordering.by[(Double, (EntityT, EntityT)), Double](_._1).reverse

    val sourceIndex: SpatialIndex[EntityT] = structures.SpatialIndex(source, tileGranularities)

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
    def referencePointFiltering(s: EntityT, t: EntityT, b:(Int, Int), partition: Envelope): Boolean ={
        val (rfX, rfY) = s.approximation.getReferencePoint(t.approximation, tileGranularities)
        val blockContainsRF: Boolean =  b._1 <= rfX && b._1+1 > rfX && b._2 <= rfY && b._2+1 > rfY
        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX > rfX && partition.getMinY <= rfY && partition.getMaxY > rfY
        blockContainsRF && partitionContainsRF
    }

    def referencePointFiltering(s: EntityT, t: EntityT, partition: Envelope): Boolean ={
        val (rfX, rfY) = s.approximation.getReferencePoint(t.approximation, tileGranularities)
        val partitionContainsRF: Boolean = partition.getMinX <= rfX && partition.getMaxX > rfX && partition.getMinY <= rfY && partition.getMaxY > rfY
        partitionContainsRF
    }

    /**
     * filter redundant verifications based on spatial criteria
     *
     * @param s source spatial entity
     * @param t source spatial entity
     * @param block block the comparison belongs to
     * @param partition the partition the comparisons belong to
     * @return true if comparison is necessary
     */
    def filterVerifications(s: EntityT, t: EntityT, block: (Int, Int), partition: Envelope): Boolean = {
        // in RF technique target is given first as it might have a more fine-grained approximation
        s.approximateIntersection(t) && referencePointFiltering(t, s, block, partition)
    }

    /**
     * get all the non-redundant verifications
     * head is always the target entity and the tail is the entities we need to compare it with
     * @return the verifications of each target geometry
     */
    def getVerifications: Iterable[List[EntityT]] = target.map(t => t :: getAllCandidates(t, sourceIndex, partitionBorder).toList)

    /**
     * count all the non-redundant verifications
     * @return number of verifications
     */
    def countVerification: Long = getVerifications.map(v => v.size-1).sum

    /**
     *  Given a spatial index, retrieve all candidate geometries and filter based on
     *  spatial criteria
     *
     * @param se target Spatial entity
     * @param index spatial index
     * @param partition current partition
     * @return all candidate geometries of se
     */
    def getAllCandidates(se: EntityT, index: SpatialIndex[EntityT], partition: Envelope): Seq[EntityT] ={
        index.index(se).flatMap { block =>
            val blockCandidates = index.get(block)
            blockCandidates.filter(candidate => filterVerifications(candidate, se, block, partition))
        }
    }

    def relate(relation: Relation): Iterator[(String, String)]

    def getDE9IM: Iterator[IM]
}
