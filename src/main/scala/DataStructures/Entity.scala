package DataStructures

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
    val mbb: MBB

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
     *  checks if the MBB intersects
     * @param se target spatial entity
     * @param relation examining relation
     * @return true if the MBBs intersect
     */
    def testMBB(se: Entity, relation: Relation*): Boolean = mbb.testMBB(se.mbb, relation)

    /**
     *  Reference point techniques. Remove duplicate comparisons by allowing the comparison
     *  only in block that contains the upper-left intersection point
     * @param se target spatial entity
     * @param block block that belongs to
     * @param thetaXY theta
     * @param partition the partition it belongs to
     * @return true if the comparison is in the block that contains the RF
     */
    def referencePointFiltering(se: Entity, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBB]=None): Boolean =
        partition match {
            case Some(p) => mbb.referencePointFiltering(se.mbb, block, thetaXY, p)
            case None => mbb.referencePointFiltering(se.mbb, block, thetaXY)
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
    def filter(se: Entity, relation: Relation, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBB]=None): Boolean =
        testMBB(se, relation) && referencePointFiltering(se, block, thetaXY, partition)

    /**
     * Get the blocks of the spatial entity
     *
     * @param thetaXY coordinated are adjusted to the selected theta
     * @param filter filter the blocks based on this function
     * @return the coordinates of the blocks
     */
    def index(thetaXY: (Double, Double), filter: ((Int, Int)) => Boolean = (_:(Int,Int)) => true): Seq[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        if (mbb.minX == 0 && mbb.maxX == 0 && mbb.minY == 0 && mbb.maxY == 0) Seq((0, 0))
        val maxX = math.ceil(mbb.maxX / thetaX).toInt
        val minX = math.floor(mbb.minX / thetaX).toInt
        val maxY = math.ceil(mbb.maxY / thetaY).toInt
        val minY = math.floor(mbb.minY / thetaY).toInt

        for (x <- minX to maxX; y <- minY to maxY; if filter((x, y))) yield (x, y)
    }

    /**
     *  compute Intersection matrix
     * @param se target entity
     * @return IntersectionMatrix
     */
    def getIntersectionMatrix(se: Entity): IntersectionMatrix = geometry.relate(se.geometry)
}

case class SpatialEntity(originalID: String = "", geometry: Geometry, mbb: MBB) extends Entity


/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(originalID: String, wkt: String): Entity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBB(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

    def apply(originalID: String, geom: Geometry): Entity ={
        val geometry: Geometry = geom
        val mbb = MBB(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

}
