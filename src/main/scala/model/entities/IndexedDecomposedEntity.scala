package model.entities

import model.{IM, SpatialIndex, TileGranularities}
import org.locationtech.jts.geom.{Geometry, IntersectionMatrix}
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes.EnvelopeIntersectionTypes

import scala.annotation.tailrec

case class IndexedDecomposedEntity(originalID: String, geometry: Geometry, segments: Array[Geometry], index: SpatialIndex[Geometry]) extends Entity {

    def getTileIndices: Set[(Int, Int)] = index.indices

    def getSegmentsFromTile(tile: (Int, Int)): Seq[Geometry] = index.get(tile)

    def getSegmentsIndexFromTile(tile: (Int, Int)): Seq[Int] = index.getIndices(tile)

    override def intersectingMBR(e: Entity, relation: Relation): Boolean = {
        lazy val segmentsIntersection: Boolean = e match {
            case fe: IndexedDecomposedEntity =>
                index.indices.intersect(fe.getTileIndices).nonEmpty
            case fe: DecomposedEntity =>
                segments.exists { fg1 => fe.segments.exists(fg2 => EnvelopeOp.checkIntersection(fg1.getEnvelopeInternal, fg2.getEnvelopeInternal, relation)) }
            case _ =>
                segments.exists { fg => EnvelopeOp.checkIntersection(fg.getEnvelopeInternal, e.env, relation)}
        }
        val envIntersection: Boolean = EnvelopeOp.checkIntersection(env, e.env, relation)
        envIntersection && segmentsIntersection
    }


    @tailrec
    private def ruleBasedVerification(pairs: List[(EnvelopeIntersectionTypes, (Geometry, Geometry))], im: IntersectionMatrix): IntersectionMatrix = {
        pairs match {
            case Nil => im
            case head :: tail =>
                val (envelopeIntersectionType, (g1, g2)) = head
                if(envelopeIntersectionType == EnvelopeIntersectionTypes.RANK0)
                    im
                else {
                    val newIm = g1.relate(g2)
                    if (newIm.isIntersects)
                        im.add(newIm)
                    if (im.isEquals(g1.getDimension, g2.getDimension) &&
                        (im.isCrosses(g1.getDimension, g2.getDimension) || im.isOverlaps(g1.getDimension, g2.getDimension)))
                        im
                    else
                        ruleBasedVerification(tail, im)
                }
        }
    }


    // WARNING: the extra points introduced by the blades affect the results,
    //  - consider to remove it
    //  - or add the same point in fragment
    def segmentedVerification(fe: IndexedDecomposedEntity, commonTiles: Seq[(Int, Int)]): IntersectionMatrix ={
        val verifications: Seq[(Geometry, Geometry)] =

            if (fe.segments.length == 1){
//                         val intersectingFragments = intersectingTiles.flatMap{ t => getFragmentsFromTile(t)}
//                         val unifiedFragmentsTry = Try(UnaryUnionOp.union(intersectingFragments.asJava))
//                         unifiedFragmentsTry match {
//                             case Failure(_)                => intersectingFragments.map(g => (g, fe.geometry))
//                             case Success(unifiedFragments) => Seq((unifiedFragments, fe.geometry))
//                         }
                Seq((geometry, fe.geometry))
            }
            else if (segments.length == 1){
//                         val intersectingFragments = intersectingTiles.flatMap{ t => fe.getFragmentsFromTile(t)}
//                         val unifiedFragmentsTry = Try(UnaryUnionOp.union(intersectingFragments.asJava))
//                         unifiedFragmentsTry match {
//                             case Failure(_)                => intersectingFragments.map(g => (geometry, g))
//                             case Success(unifiedFragments) => Seq((geometry, unifiedFragments))
//                         }
                Seq((geometry, fe.geometry))
            }
            else {
                commonTiles
                    .map(t => (getSegmentsIndexFromTile(t), fe.getSegmentsIndexFromTile(t)))
                    .flatMap { case (indices1, indices2) => for (i <- indices1; j <- indices2) yield (i, j) }
                    .map{ case (i, j) => (segments(i), fe.segments(j))}
            }

        val typedVerifications: List[(EnvelopeIntersectionTypes, (Geometry, Geometry))] =
            verifications.map { case (g1, g2) =>
                val envelopeIntersectionType = EnvelopeOp.getIntersectingEnvelopesType(g1.getEnvelopeInternal, g2.getEnvelopeInternal)
                (envelopeIntersectionType, (g1, g2))
            }.sortBy(_._1).toList

        val emptyIM = new IntersectionMatrix("FFFFFFFFF")
        ruleBasedVerification(typedVerifications, emptyIM)
    }


     override def getIntersectionMatrix(e: Entity): IM = {
         e match {

             case fe: IndexedDecomposedEntity =>
                 val commonTiles = index.indices.intersect(fe.getTileIndices).toSeq
                 val im = if (commonTiles.length < 10) segmentedVerification(fe, commonTiles) else geometry.relate(fe.geometry)
                 IM(this, fe, im)

             case e: Entity => super.getIntersectionMatrix(e)
         }
     }
}


object IndexedDecomposedEntity{
    def apply(e: Entity, theta: TileGranularities, decompose: Geometry => Seq[Geometry]): IndexedDecomposedEntity ={
        val segments = decompose(e.geometry).toArray
        val index = SpatialIndex(segments, theta)
        IndexedDecomposedEntity(e.originalID, e.geometry, segments, index)
    }

    def apply(id: String, geometry: Geometry, theta: TileGranularities, decompose: Geometry => Seq[Geometry]): IndexedDecomposedEntity = {
        val segments = decompose(geometry).toArray
        val index = SpatialIndex(segments, theta)
        IndexedDecomposedEntity(id, geometry, segments, index)
    }
}
