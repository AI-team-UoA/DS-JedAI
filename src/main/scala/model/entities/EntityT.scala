package model.entities

import model.{IM, TileGranularities}
import model.approximations.GeometryApproximationT
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.configuration.Constants.Relation.Relation

import scala.language.implicitConversions

/**
 * @author George Mandilaras (NKUA)
 */

trait EntityT extends Serializable {

    val originalID: String
    val geometry: Geometry
    val approximation: GeometryApproximationT
    val tileGranularities: TileGranularities

    def getEnvelopeInternal(): Envelope = approximation.getEnvelopeInternal()

    def getMinX: Double = approximation.getMinX
    def getMaxX: Double = approximation.getMaxX
    def getMinY: Double = approximation.getMinY
    def getMaxY: Double = approximation.getMaxY


    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    def relate(target: EntityT, relation: Relation): Boolean = approximation.approximateIntersection(target.approximation)

    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: EntityT): IM = {
        val im = geometry.relate(se.geometry)
        IM(this, se, im)
    }

    override def toString: String = s"Entity($originalID, ${geometry.toString}, ${approximation.toString})"

    def approximateIntersection(e: EntityT): Boolean = approximation.approximateIntersection(e.approximation)

    def getIntersectingInterior(e: EntityT): Envelope = approximation.getIntersectingInterior(e.approximation)

}


