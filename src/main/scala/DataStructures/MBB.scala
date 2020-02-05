package DataStructures

import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}

case class MBB(maxX:Double, minX:Double, maxY:Double, minY:Double){

    def getGeometry: Geometry ={
        val coordsList: List[(Double, Double)] = List((minX, minY), (minX, maxY), (maxX, maxY), (maxX, minY), (minX, minY))
        val coordsAr: Array[Coordinate] = coordsList.map(c => new Coordinate(c._1, c._2)).toArray
        val gf: GeometryFactory = new GeometryFactory()
        gf.createPolygon(coordsAr)
    }

    def crossesMeridian: Boolean ={
        (minX < 180d && maxX > 180d) || (minX < -180d && maxX > -180d)
    }


    def splitOnMeridian: (MBB, MBB) ={
        if (minX < -180d && maxX > -180d){
            val easternMBB: MBB = MBB(minX, -180d, maxY, minY)
            val westernMBB: MBB = MBB(-180d, maxX, maxY, minY)

            (westernMBB, easternMBB)
        }
        else if (minX < 180d && maxX >180d) {
            val easternMBB: MBB = MBB(maxX, 180d, maxY, minY)
            val westernMBB: MBB = MBB(180d, minX, maxY, minY)

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
