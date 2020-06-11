package DataStructures

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import utils.Constants

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class SpatialEntity(id: Int, originalID: String = "", geometry: Geometry,  mbb: MBB, crossesMeridian: Boolean){

    /**
     * Find the relation with another SpatialEntity
     * @param target the target entity
     * @param relation the selected relation
     * @return whether the relation holds
     */
    def relate(target: SpatialEntity, relation: String): Boolean ={
        relation match {
            case Constants.CONTAINS => geometry.contains(target.geometry)
            case Constants.INTERSECTS => geometry.intersects(target.geometry)
            case Constants.CROSSES => geometry.crosses(target.geometry)
            case Constants.COVERS => geometry.covers(target.geometry)
            case Constants.COVEREDBY => geometry.coveredBy(target.geometry)
            case Constants.OVERLAPS => geometry.overlaps(target.geometry)
            case Constants.TOUCHES => geometry.touches(target.geometry)
            case Constants.DISJOINT => geometry.disjoint(target.geometry)
            case Constants.EQUALS => geometry.equals(target.geometry)
            case Constants.WITHIN => geometry.within(target.geometry)
            case _ => false
        }
    }

    /**
     * Get the blocks of the spatial entity
     *
     * @param thetaXY coordinated are adjusted to the selected theta
     * @param filter filter the blocks based on this function
     * @return the coordinates of the blocks
     */
    def index(thetaXY: (Double, Double), filter: ((Int, Int)) => Boolean = (_:(Int,Int)) => true): Array[(Int, Int)] = {
        val (thetaX, thetaY) = thetaXY

        val maxX = math.ceil(mbb.maxX / thetaX).toInt
        val minX = math.floor(mbb.minX / thetaX).toInt
        val maxY = math.ceil(mbb.maxY / thetaY).toInt
        val minY = math.floor(mbb.minY / thetaY).toInt

        (for (x <- minX to maxX; y <- minY to maxY; if filter((x, y))) yield (x, y)).toArray
    }

}

/**
 * auxiliary constructors
 */
object SpatialEntity {

    def apply(id: Int, originalID: String, wkt: String): SpatialEntity ={
        val wktReader = new WKTReader()
        val geometry: Geometry = wktReader.read(wkt)
        val mbb = MBB(geometry)
        val crossesMeridian =  mbb.crossesMeridian

        SpatialEntity(id, originalID, geometry, mbb, crossesMeridian)
    }

    def apply(id: Int, originalID: String, geom: Geometry): SpatialEntity ={
        val geometry: Geometry = geom
        val mbb = MBB(geometry)
        val crossesMeridian =  mbb.crossesMeridian

        SpatialEntity(id, originalID, geometry, mbb, crossesMeridian)
    }

}
