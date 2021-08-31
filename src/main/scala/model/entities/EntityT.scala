package model.entities

import model.IM
import model.approximations.GeometryApproximationT
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

import scala.language.implicitConversions

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

trait EntityT extends Serializable {

    val originalID: String
    val geometry: Geometry
    val geometryApproximation: GeometryApproximationT

    def getEnvelopeInternal(): Envelope = geometryApproximation.getEnvelopeInternal()

    // TODO: these are used in indexing - resulting more blocks than necessary
    def getMinX: Double = geometryApproximation.getMinX
    def getMaxX: Double = geometryApproximation.getMaxX
    def getMinY: Double = geometryApproximation.getMinY
    def getMaxY: Double = geometryApproximation.getMaxY


    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    def relate(target: EntityT, relation: Relation): Boolean = geometryApproximation.approximateIntersection(target.geometryApproximation)

    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: EntityT): IM = {
        val im = geometry.relate(se.geometry)
        IM(this, se, im)
    }

    override def toString: String = s"$originalID, ${geometryApproximation.toString}"

    def approximateIntersection(e: EntityT): Boolean = geometryApproximation.approximateIntersection(e.geometryApproximation)

    def getIntersectingInterior(e: EntityT): Envelope = geometryApproximation.getIntersectingInterior(e.geometryApproximation)

}


