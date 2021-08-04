package model.entities.segmented

import model.IM
import model.entities.Entity
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

import scala.collection.JavaConverters._

case class DecomposedEntity(originalID: String, geometry: Geometry, segments: Seq[Geometry]) extends SegmentedEntityT[Geometry] {

    override def intersectingMBR(e: Entity, relation: Relation): Boolean = {
        lazy val segmentsEnvIntersection: Boolean = e match {
            case DecomposedEntity(_, _, targetSegments) => segments.exists { segment1 =>
                val segmentEnv = segment1.getEnvelopeInternal
                targetSegments.exists(segment2 => EnvelopeOp.checkIntersection(segmentEnv, segment2.getEnvelopeInternal, relation))
            }
            case IndexedDecomposedEntity(_, _, targetSegments, _) => segments.exists { segment1 =>
                val segmentEnv = segment1.getEnvelopeInternal
                targetSegments.exists(segment2 => EnvelopeOp.checkIntersection(segmentEnv, segment2.getEnvelopeInternal, relation))
            }
            case FineGrainedEntity(_, _, targetEnvSegments) => segments.exists { segment1 =>
                val segmentEnv = segment1.getEnvelopeInternal
                targetEnvSegments.exists(env2 => EnvelopeOp.checkIntersection(segmentEnv, env2, relation))
            }
            case _ => segments.exists { fg =>
                val segmentEnv = fg.getEnvelopeInternal
                EnvelopeOp.checkIntersection(segmentEnv, e.env, relation)
            }
        }
        val envIntersection: Boolean = EnvelopeOp.checkIntersection(env, e.env, relation)
        envIntersection && segmentsEnvIntersection
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
