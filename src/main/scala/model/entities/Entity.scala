package model.entities

import model.IM
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp

import scala.language.implicitConversions

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

trait Entity extends Serializable {

    val originalID: String
    val geometry: Geometry
    val env: Envelope = geometry.getEnvelopeInternal

    def getEnvelopeInternal(): Envelope = env
    def getMinX: Double = env.getMinX
    def getMaxX: Double = env.getMaxX
    def getMinY: Double = env.getMinY
    def getMaxY: Double = env.getMaxY


    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    def relate(target: Entity, relation: Relation): Boolean =
        relation match {
            case Relation.CONTAINS => geometry.contains(target.geometry)
            case Relation.INTERSECTS => geometry.intersects(target.geometry)
            case Relation.CROSSES => geometry.crosses(target.geometry)
            case Relation.COVERS => geometry.covers(target.geometry)
            case Relation.COVEREDBY => geometry.coveredBy(target.geometry)
            case Relation.OVERLAPS => geometry.overlaps(target.geometry)
            case Relation.TOUCHES => geometry.touches(target.geometry)
            case Relation.DISJOINT => geometry.disjoint(target.geometry)
            case Relation.EQUALS => geometry.equals(target.geometry)
            case Relation.WITHIN => geometry.within(target.geometry)
            case Relation.DE9IM => ! geometry.disjoint(target.geometry)
            case _ => false
        }

    /**
     *  checks if MBRs relate
     * @param se target spatial entity
     * @param relation examining relation
     * @return true if the MBRs relate
     */
    def intersectingMBR(se: Entity, relation: Relation): Boolean = EnvelopeOp.checkIntersection(env, se.env, relation)


    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: Entity): IM = {
        val im = geometry.relate(se.geometry)
        IM(this, se, im)
    }

    override def toString: String = s"$originalID, ${env.toString}"

    def getIntersectingInterior(e: Entity): Envelope = EnvelopeOp.getIntersectingInterior(env, e.env)

}


