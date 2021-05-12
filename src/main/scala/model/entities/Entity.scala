package model.entities

import model.{IM, MBR, TileGranularities}
import org.locationtech.jts.geom.{Envelope, Geometry}
import utils.Constants.Relation
import utils.Constants.Relation.Relation

import scala.language.implicitConversions

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

trait Entity extends Serializable {

    // any envelope has the extended functionalities of MBR
    implicit def envelopeToMBR(env: Envelope): MBR = MBR(env)

    val originalID: String
    val geometry: Geometry
    val env: Envelope = geometry.getEnvelopeInternal


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
    def intersectingMBR(se: Entity, relation: Relation): Boolean = env.intersectingMBR(se.env, relation)

    /**
     *  Reference point techniques. Remove duplicate comparisons by allowing the comparison
     *  only in block that contains the upper-left intersection point
     * @param se target spatial entity
     * @param block block that belongs to
     * @param tileGranularities tile granularities
     * @param partition the partition it belongs to
     * @return true if the comparison is in the block that contains the RF
     */
    def referencePointFiltering(se: Entity, block: (Int, Int), tileGranularities: TileGranularities, partition: MBR): Boolean =
             env.referencePointFiltering(se.env, block, tileGranularities, partition)

    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: Entity): IM = {
        val im = geometry.relate(se.geometry)
        IM(this, se, im)
    }

    override def toString: String = s"$originalID, ${MBR.toString}"

    def getMinX: Double = env.getMinX
    def getMaxX: Double = env.getMaxX
    def getMinY: Double = env.getMinY
    def getMaxY: Double = env.getMaxY

    def getAdjustedMBR(tileGranularities: TileGranularities): MBR = env.adjust(tileGranularities)

    def getIntersectingInterior(e: Entity): MBR = env.getIntersectingInterior(e.env)
}


