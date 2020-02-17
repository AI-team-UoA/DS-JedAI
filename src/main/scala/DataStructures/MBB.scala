package DataStructures

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import utils.Constant

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
case class MBB(maxX:Double, minX:Double, maxY:Double, minY:Double){

    def getGeometry: Geometry ={
        val coordsList: List[(Double, Double)] = List((minX, minY), (minX, maxY), (maxX, maxY), (maxX, minY), (minX, minY))
        val coordsAr: Array[Coordinate] = coordsList.map(c => new Coordinate(c._1, c._2)).toArray
        val gf: GeometryFactory = new GeometryFactory()
        gf.createPolygon(coordsAr)
    }

    def crossesMeridian: Boolean ={
        (minX < Constant.MAX_LONG && maxX > Constant.MAX_LONG) || (minX < Constant.MIN_LONG && maxX > Constant.MIN_LONG)
    }


    def splitOnMeridian: (MBB, MBB) ={
        if (minX < Constant.MIN_LONG && maxX > Constant.MIN_LONG){
            val easternMBB: MBB = MBB(minX, Constant.MIN_LONG, maxY, minY)
            val westernMBB: MBB = MBB(Constant.MIN_LONG, maxX, maxY, minY)

            (westernMBB, easternMBB)
        }
        else if (minX < Constant.MAX_LONG && maxX >Constant.MAX_LONG) {
            val easternMBB: MBB = MBB(maxX, Constant.MAX_LONG, maxY, minY)
            val westernMBB: MBB = MBB(Constant.MAX_LONG, minX, maxY, minY)

            (westernMBB, easternMBB)
        }
        else (null, null)
    }
}

object  MBB {
    def apply(geom: Geometry): MBB ={
        val env = geom.getEnvelopeInternal
        MBB(env.getMaxX, env.getMinX, env.getMaxY, env.getMinY)
    }
}
