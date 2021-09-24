package linkers.progressive

import linkers.LinkerT
import model.IM
import model.entities.EntityT
import model.structures.{ComparisonPQ, SpatialIndex}
import model.weightedPairs.WeightedPairFactory
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.configuration.Constants.WeightingFunction.WeightingFunction

trait ProgressiveLinkerT extends LinkerT{
    val budget: Int
    val mainWF: WeightingFunction
    val secondaryWF: Option[WeightingFunction]
    val ws: Constants.WeightingScheme
    val totalSourceEntities: Long
    val totalBlocks: Double
    val weightedPairFactory: WeightedPairFactory = WeightedPairFactory(mainWF, secondaryWF, ws, tileGranularities, totalBlocks)

    val targetAr: Array[EntityT] = target.toArray

    /**
     *  Given a spatial index, retrieve all candidate geometries and filter based on
     *  spatial criteria
     *
     * @param se target Spatial entity
     * @param index spatial index
     * @param partition current partition
     * @return all candidate geometries of se
     */
    def getAllCandidatesWithIndex(se: EntityT, index: SpatialIndex[EntityT], partition: Envelope): Seq[(Int, EntityT)] ={
        index.index(se)
            .flatMap { block =>
                val blockCandidates = index.getWithIndex(block)
                blockCandidates.filter(candidate => filterVerifications(candidate._2, se, block, partition))
            }
    }

    /**
     * Compute the  9-IM of the entities of a PQ
     * @param pq a Priority Queue
     * @return an iterator of  IM
     */
    def computeDE9IM(pq: ComparisonPQ): Iterator[IM] = {
        if (!pq.isEmpty)
            pq.dequeueAll.map{ wp =>
                val s = source(wp.entityId1)
                val t = targetAr(wp.entityId2)
                s.getIntersectionMatrix(t)
            }.takeWhile(_ => !pq.isEmpty)
        else Iterator()
    }


    /**
     *  Get the DE-9IM of the top most related entities based
     *  on the input budget and the Weighting Function
     * @return an RDD of IM
     */
    def getDE9IM: Iterator[IM] = computeDE9IM( prioritize(Relation.DE9IM))


    /**
     *  Examine the Relation of the top most related entities based
     *  on the input budget and the Weighting Function
     *  @param relation the relation to examine
     *  @return an RDD of pair of IDs
     */
    def relate(relation: Relation): Iterator[(String, String)] = {
        val targetAr = target.toArray
        val pq = prioritize(relation)
        if (!pq.isEmpty)
            pq.dequeueAll.map{ wp =>
                val s = source(wp.entityId1)
                val t = targetAr(wp.entityId2)
                (s.relate(t, relation), (s.originalID, t.originalID))
            }.filter(_._1).map(_._2)
        else Iterator()
    }

    def prioritize(relation: Relation): ComparisonPQ

}
