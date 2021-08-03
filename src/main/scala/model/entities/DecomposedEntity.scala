package model.entities

import model.IM
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

import scala.collection.JavaConverters._

case class DecomposedEntity(originalID: String = "", geometry: Geometry, segments: Seq[Geometry]) extends Entity {

    override def intersectingMBR(e: Entity, relation: Relation): Boolean = {
        lazy val segmentsIntersection: Boolean = e match {
            case fe: DecomposedEntity =>
                segments.exists { fg1 =>
                    val segmentsEnv = fg1.getEnvelopeInternal
                    fe.segments.exists(fg2 => EnvelopeOp.checkIntersection(segmentsEnv, fg2.getEnvelopeInternal, relation))
                }

            case fe: IndexedDecomposedEntity =>
                segments.exists { fg1 =>
                    val segmentsEnv = fg1.getEnvelopeInternal
                    fe.segments.exists(fg2 => EnvelopeOp.checkIntersection(segmentsEnv, fg2.getEnvelopeInternal, relation))
                }

            case _ =>
                segments.exists { fg =>
                    val segmentsEnv = fg.getEnvelopeInternal
                    EnvelopeOp.checkIntersection(segmentsEnv, e.env, relation)
                }
        }
        val envIntersection: Boolean = EnvelopeOp.checkIntersection(env, e.env, relation)
        envIntersection && segmentsIntersection
    }


    def findIntersectingSegments(e: Entity): Seq[(Geometry, Geometry)] =
        e match {
            case fe: DecomposedEntity =>
                for (f1 <- segments;
                     f2 <- fe.segments
                     if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, f2.getEnvelopeInternal, Relation.DE9IM)
                     ) yield (f1, f2)

            case fe: IndexedDecomposedEntity =>
                for (f1 <- segments;
                     f2 <- fe.segments
                     if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, f2.getEnvelopeInternal, Relation.DE9IM)
                     ) yield (f1, f2)
            case _ =>
                for (f1 <- segments
                     if EnvelopeOp.checkIntersection(f1.getEnvelopeInternal, e.env, Relation.DE9IM)
                     ) yield (f1, e.geometry)
        }


    override def getIntersectionMatrix(e: Entity): IM = {
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

    def apply(e: Entity, decompose: Geometry => Seq[Geometry]): DecomposedEntity ={
        val segments = decompose(e.geometry)
        DecomposedEntity(e.originalID, e.geometry, segments)
    }

    def apply(originalID: String, geom: Geometry, decompose: Geometry => Seq[Geometry]): DecomposedEntity ={
        val segments = decompose(geom)
        DecomposedEntity(originalID, geom, segments)
    }
}
