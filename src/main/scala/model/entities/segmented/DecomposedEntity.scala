package model.entities.segmented

import model.{IM, TileGranularities}
import model.approximations.FineGrainedEnvelopes
import model.entities.EntityT
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.union.UnaryUnionOp

import scala.collection.JavaConverters._

case class DecomposedEntity(originalID: String, geometry: Geometry, theta: TileGranularities, approximation: FineGrainedEnvelopes,
                            segments: IndexedSeq[Geometry]) extends SegmentedEntityT[Geometry] {

    def findIntersectingSegments(e: EntityT): Seq[(Geometry, Geometry)] ={
        e match {
            case DecomposedEntity(_,_,_, tApproximation,tSegments) =>
                approximation.findIntersectingEnvelopesIndices(tApproximation)
                    .map{case (i, j) => (segments(i), tSegments(j)) }
            case IndexedDecomposedEntity(_,_,_,tApproximation,tSegments, _) =>
                approximation.findIntersectingEnvelopesIndices(tApproximation)
                    .map{case (i, j) => (segments(i), tSegments(j)) }
            case _ =>
                approximation.findIntersectingEnvelopesIndices(e.approximation)
                    .map{case (i, _) => (segments(i), e.geometry) }
        }
    }

    def findIntersectingSegmentsIndices(e: EntityT): Seq[(Int, Int)] =
        approximation.findIntersectingEnvelopesIndices(e.approximation)


    override def getIntersectionMatrix(e: EntityT): IM = {
        val candidateSegments = findIntersectingSegments(e)
        val im =
            if (candidateSegments.length < 10) {
                if (candidateSegments.length > 1){
                    val sourceSegments = candidateSegments.map(_._1).asJava
                    val sourceSegmentsU = new UnaryUnionOp(sourceSegments).union()

                    val targetSegments = candidateSegments.map(_._2).asJava
                    val targetSegmentsU = new UnaryUnionOp(targetSegments).union()
                    sourceSegmentsU.relate(targetSegmentsU)
                }
                else {
                    val (s, t) = candidateSegments.head
                    s.relate(t)
                }
            } else geometry.relate(e.geometry)
        IM(this, e, im)
    }
}

object DecomposedEntity {

    def apply(e: EntityT, theta: TileGranularities, decompose: Geometry => Seq[Geometry]): DecomposedEntity ={
        val segments = decompose(e.geometry)
        val approximation = FineGrainedEnvelopes(e.geometry, segments)
        DecomposedEntity(e.originalID, e.geometry, theta, approximation, segments.toIndexedSeq)
    }

    def apply(originalID: String, geom: Geometry, theta: TileGranularities, decompose: Geometry => Seq[Geometry]): DecomposedEntity ={
        val segments = decompose(geom)
        val approximation = FineGrainedEnvelopes(geom, segments)
        DecomposedEntity(originalID, geom, theta, approximation, segments.toIndexedSeq)
    }
}
