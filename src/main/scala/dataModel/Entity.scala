package dataModel

import com.vividsolutions.jts.geom.{Geometry, IntersectionMatrix}
import com.vividsolutions.jts.io.WKTReader
import utils.Constants.Relation
import utils.Constants.Relation.Relation

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */

trait Entity extends Serializable {

    val originalID: String
    val geometry: Geometry
    val mbr: MBR

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
    def testMBR(se: Entity, relation: Relation*): Boolean = mbr.testMBR(se.mbr, relation)

    /**
     *  Reference point techniques. Remove duplicate comparisons by allowing the comparison
     *  only in block that contains the upper-left intersection point
     * @param se target spatial entity
     * @param block block that belongs to
     * @param thetaXY theta
     * @param partition the partition it belongs to
     * @return true if the comparison is in the block that contains the RF
     */
    def referencePointFiltering(se: Entity, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBR]=None): Boolean =
        partition match {
            case Some(p) => mbr.referencePointFiltering(se.mbr, block, thetaXY, p)
            case None => mbr.referencePointFiltering(se.mbr, block, thetaXY)
        }

    /**
     *  filter comparisons based on spatial criteria
     * @param se target spatial entity
     * @param relation examining relation
     * @param block block the comparison belongs to
     * @param thetaXY theta
     * @param partition the partition the comparisons belong to
     * @return true if comparison is necessary
     */
    def filter(se: Entity, relation: Relation, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBR]=None): Boolean =
        testMBR(se, relation) && referencePointFiltering(se, block, thetaXY, partition)

    /**
     * Get the blocks of the spatial entity
     *
     * @param thetaXY coordinated are adjusted to the selected theta
     * @param filter filter the blocks based on this function
     * @return the coordinates of the blocks
     */
    def index(thetaXY: (Double, Double), filter: ((Int, Int)) => Boolean = (_:(Int,Int)) => true): Seq[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        if (mbr.minX == 0 && mbr.maxX == 0 && mbr.minY == 0 && mbr.maxY == 0) Seq((0, 0))
        val maxX = math.ceil(mbr.maxX / thetaX).toInt
        val minX = math.floor(mbr.minX / thetaX).toInt
        val maxY = math.ceil(mbr.maxY / thetaY).toInt
        val minY = math.floor(mbr.minY / thetaY).toInt

        for (x <- minX to maxX; y <- minY to maxY; if filter((x, y))) yield (x, y)
    }

    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: Entity): IntersectionMatrix = geometry.relate(se.geometry)

    override def toString: String = s"$originalID, ${MBR.toString}"
}

case class SpatialEntity(originalID: String = "", geometry: Geometry, mbr: MBR) extends Entity


/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(originalID: String, wkt: String): Entity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBR(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

    def apply(originalID: String, geom: Geometry): Entity ={
        val geometry: Geometry = geom
        val mbb = MBR(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

}
