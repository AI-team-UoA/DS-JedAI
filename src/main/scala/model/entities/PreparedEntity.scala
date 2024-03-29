package model.entities
import model.TileGranularities
import model.approximations.{GeometryApproximationT, MBR}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation

case class PreparedEntity(originalID: String, geometry: Geometry, theta: TileGranularities, approximation: GeometryApproximationT) extends EntityT {
    val preparedGeometry: PreparedGeometry = PreparedGeometryFactory.prepare(geometry)

    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    override def relate(target: EntityT, relation: Relation): Boolean =
        relation match {
            case Relation.CONTAINS => preparedGeometry.contains(target.geometry)
            case Relation.INTERSECTS => preparedGeometry.intersects(target.geometry)
            case Relation.CROSSES => preparedGeometry.crosses(target.geometry)
            case Relation.COVERS => preparedGeometry.covers(target.geometry)
            case Relation.COVEREDBY => preparedGeometry.coveredBy(target.geometry)
            case Relation.OVERLAPS => preparedGeometry.overlaps(target.geometry)
            case Relation.TOUCHES => preparedGeometry.touches(target.geometry)
            case Relation.DISJOINT => preparedGeometry.disjoint(target.geometry)
            case Relation.EQUALS => geometry.equals(target.geometry)
            case Relation.WITHIN => preparedGeometry.within(target.geometry)
            case Relation.DE9IM => ! preparedGeometry.disjoint(target.geometry)
            case _ => false
        }

    override def toString: String = s"PreparedEntity($originalID,${preparedGeometry.toString} ${approximation.toString})"
}

object PreparedEntity {

    def apply(originalID: String, geometry: Geometry, theta: TileGranularities): PreparedEntity = {
        PreparedEntity(originalID, geometry, theta, MBR(geometry.getEnvelopeInternal))
    }

    def apply(originalID: String, geometry: Geometry, theta: TileGranularities, approximationTransformer: Geometry => GeometryApproximationT): PreparedEntity = {
        PreparedEntity(originalID, geometry, theta, approximationTransformer(geometry))
    }
}
