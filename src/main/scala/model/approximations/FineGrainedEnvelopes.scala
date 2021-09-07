package model.approximations

import model.TileGranularities
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation
import utils.geometryUtils.GeometryUtils

import scala.annotation.tailrec
import scala.math.{max, min}

case class FineGrainedEnvelopes(env: Envelope, envelopes: List[Envelope]) extends GeometryApproximationT{

    override def getEnvelopes: List[Envelope] = envelopes

    /**
     * checks if the two geometry approximations Intersect
     * since condition evaluations are short-circuit
     *
     * @param targetApproximation target geometry approximation
     * @return true if the MBRs relate
     */
    override def approximateIntersection(targetApproximation: GeometryApproximationT): Boolean = {

//         Alternative Implementations
//         1
//        for (e1 <- envelopes; e2 <- approximation.getEnvelopes; if EnvelopeOp.checkIntersection(e1, e2, Relation.INTERSECTS))
//            return true
//        false

//        2
//        segments.exists { segment1 =>
//            val segmentEnv = segment1.getEnvelopeInternal
//            targetSegments.exists(segment2 => EnvelopeOp.checkIntersection(segmentEnv, segment2.getEnvelopeInternal))
//        }

        @tailrec
        def checkIntersection(envelopes: List[Envelope], tEnvelope: Envelope): Boolean =
            envelopes match {
                case Nil => false
                case sHead :: sTail =>
                    val intersection = checkRelation(sHead, tEnvelope, Relation.INTERSECTS)
                    if (intersection) intersection else checkIntersection(sTail, tEnvelope)
            }
        @tailrec
        def checkListsIntersection(envelopes: List[Envelope], targetEnvelopes: List[Envelope]): Boolean =
            targetEnvelopes match {
                case Nil => false
                case tHead :: tTail  =>
                    val intersection = checkIntersection(envelopes, tHead)
                    if (intersection) intersection else checkListsIntersection(envelopes, tTail)
            }

        val envelopeIntersection =  checkRelation(env, targetApproximation.env, Relation.INTERSECTS)
        envelopeIntersection && checkListsIntersection(envelopes, targetApproximation.getEnvelopes)
    }



    def findIntersectingEnvelopesIndices(targetApproximation: GeometryApproximationT): List[(Int, Int)] ={

        @tailrec
        def checkIntersection(envelopes: List[(Envelope, Int)], tEnvelope: (Envelope, Int), acc: List[(Int, Int)]): List[(Int, Int)] = {
            envelopes match {
                case Nil => acc
                case sHead :: sTail =>
                    val intersection = checkRelation(sHead._1, tEnvelope._1, Relation.INTERSECTS)
                    val newAcc = if (intersection) (sHead._2, tEnvelope._2) :: acc else acc
                    checkIntersection(sTail, tEnvelope, newAcc)
            }
        }

        @tailrec
        def checkListsIntersection(envelopes: List[(Envelope, Int)], targetEnvelopes: List[(Envelope, Int)], acc: List[(Int, Int)]): List[(Int, Int)] ={
            targetEnvelopes match {
                case Nil => acc
                case tHead :: tTail  =>
                    val newAcc = checkIntersection(envelopes, tHead, acc)
                    checkListsIntersection(envelopes, tTail, newAcc)
            }
        }

        targetApproximation match {
            case MBR(env) =>
                checkIntersection(envelopes.zipWithIndex, (env, 0), Nil)
            case FineGrainedEnvelopes(_, tFineGrainedEnvelopes) =>
                checkListsIntersection(envelopes.zipWithIndex, tFineGrainedEnvelopes.zipWithIndex, Nil)
        }
    }

    def findFirstIntersectingEnvelopes(targetApproximation: GeometryApproximationT): Option[(Envelope, Envelope)] = {
        @tailrec
        def checkIntersection(envelopes: List[Envelope], tEnvelope: Envelope): Option[(Envelope, Envelope)] =
            envelopes match {
                case Nil => None
                case sHead :: sTail =>
                    val intersection = checkRelation(sHead, tEnvelope, Relation.INTERSECTS)
                    if (intersection) Some((sHead, tEnvelope))
                    else checkIntersection(sTail, tEnvelope)
            }
        @tailrec
        def checkListsIntersection(envelopes: List[Envelope], targetEnvelopes: List[Envelope]): Option[(Envelope, Envelope)] =
            targetEnvelopes match {
                case Nil => None
                case tHead :: tTail =>
                    val intersectingEnvelopes = checkIntersection(envelopes, tHead)
                    if (intersectingEnvelopes.isDefined) intersectingEnvelopes
                    else checkListsIntersection(envelopes, tTail)
            }

        targetApproximation match {
            case MBR(env) =>
                checkIntersection(envelopes, env)
            case FineGrainedEnvelopes(_, tEnvelopes) =>
                checkListsIntersection(envelopes, tEnvelopes)
        }
    }


    override def getReferencePoint(targetApproximation: GeometryApproximationT, theta: TileGranularities): (Double, Double) ={
        // IMPROVE: always seeks for the first intersecting pair
        val envelopes = findFirstIntersectingEnvelopes(targetApproximation)
        envelopes match {
            case Some((env1, env2)) =>
                val minX1 = env1.getMinX /theta.x
                val minX2 = env2.getMinX /theta.x
                val maxY1 = env1.getMaxY /theta.y
                val maxY2 = env2.getMaxY /theta.y
                val rfX: Double = max(minX1, minX2) + epsilon
                val rfY: Double = min(maxY1, maxY2) + epsilon
                (rfX, rfY)
            case None =>
                super.getReferencePoint(targetApproximation, theta)
        }
    }

    override def getOverlappingTiles(theta: TileGranularities): Seq[(Int, Int)] ={
        // IMPROVE: distinct is expensive
        envelopes.map(env => adjustEnvelope(env, theta)).flatMap{
            case (x1, x2, y1, y2) =>
                for (x <- x1 to x2; y <- y1 to y2) yield (x, y)
        }.distinct
    }

    override def toString: String = s"FineGrainedEnvelopes(${envelopes.length}, ${envelopes.map(e => GeometryUtils.geomFactory.toGeometry(e)).mkString("\n")})"
}

object FineGrainedEnvelopes{
    def apply(geometry: Geometry, geometries: Seq[Geometry]): FineGrainedEnvelopes =
        FineGrainedEnvelopes(geometry.getEnvelopeInternal, geometries.map(g => g.getEnvelopeInternal).toList)

    def apply(geometry: Geometry, envelopes: List[Envelope]): FineGrainedEnvelopes =
        FineGrainedEnvelopes(geometry.getEnvelopeInternal, envelopes)

    def apply(geometry: Geometry, transformer: Geometry => List[Envelope]): FineGrainedEnvelopes =
        FineGrainedEnvelopes(geometry.getEnvelopeInternal, transformer(geometry))
}
