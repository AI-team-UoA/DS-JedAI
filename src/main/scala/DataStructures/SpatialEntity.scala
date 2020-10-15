package DataStructures

import com.vividsolutions.jts.geom.{Geometry, IntersectionMatrix}
import com.vividsolutions.jts.io.WKTReader
import utils.Constants.Relation
import utils.Constants.Relation.Relation

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class SpatialEntity(originalID: String = "", geometry: Geometry, mbb: MBB){

    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    def relate(target: SpatialEntity, relation: Relation): Boolean ={
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
            case _ => false
        }
    }

    def testMBB(se: SpatialEntity, relation: Relation*): Boolean = mbb.testMBB(se.mbb, relation)

    def referencePointFiltering(se: SpatialEntity, block: (Int, Int), thetaXY: (Double, Double), partition: Option[MBB]=None): Boolean ={
        partition match {
            case Some(p) => mbb.referencePointFiltering(se.mbb, block, thetaXY, p)
            case None => mbb.referencePointFiltering(se.mbb, block, thetaXY)
        }
    }

    def partitionRF(mbb:MBB, thetaXY: (Double, Double), partition: MBB): Boolean ={
        mbb.partitionRF(mbb, thetaXY, partition)
    }


        /**
     * Get the blocks of the spatial entity
     *
     * @param thetaXY coordinated are adjusted to the selected theta
     * @param filter filter the blocks based on this function
     * @return the coordinates of the blocks
     */
    def index(thetaXY: (Double, Double), filter: ((Int, Int)) => Boolean = (_:(Int,Int)) => true): IndexedSeq[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        if (mbb.minX == 0 && mbb.maxX == 0 && mbb.minY == 0 && mbb.maxY == 0)
            Array((0,0))
        val maxX = math.ceil(mbb.maxX / thetaX).toInt
        val minX = math.floor(mbb.minX / thetaX).toInt
        val maxY = math.ceil(mbb.maxY / thetaY).toInt
        val minY = math.floor(mbb.minY / thetaY).toInt

        for (x <- minX to maxX; y <- minY to maxY; if filter((x, y))) yield (x, y)
    }

    def getIntersectionMatrix(t: SpatialEntity): IntersectionMatrix = geometry.relate(t.geometry)

}

/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(originalID: String, wkt: String): SpatialEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBB(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

    def apply(originalID: String, geom: Geometry): SpatialEntity ={
        val geometry: Geometry = geom
        val mbb = MBB(geometry)

        SpatialEntity(originalID, geometry, mbb)
    }

}
