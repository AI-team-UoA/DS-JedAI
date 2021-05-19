package utils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, LineString, Polygon}
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import scala.collection.JavaConverters._

object GridFragmentation {
    val geometryFactory = new GeometryFactory()
    val epsilon: Double = 1e-8



    def getVerticalBlades(env: Envelope, thetaX: Double): Seq[LineString] ={
        val minX = env.getMinX
        val maxX = env.getMaxX
        val n = math.floor(minX/thetaX) +1
        val bladeStart: Double = n*thetaX

        for (x <- BigDecimal(bladeStart) until maxX by thetaX)
            yield {
                geometryFactory.
                    createLineString(Array(new Coordinate(x.toDouble, env.getMinY-epsilon), new Coordinate(x.toDouble, env.getMaxY+epsilon)))
            }
    }



    def getHorizontalBlades(env: Envelope, thetaY: Double): Seq[LineString] ={
        val minY = env.getMinY
        val maxY = env.getMaxY
        val n = math.floor(minY/thetaY) +1
        val bladeStart: Double = n*thetaY

        for (y <- BigDecimal(bladeStart) until maxY by thetaY)
            yield geometryFactory
                .createLineString(Array(new Coordinate(env.getMinX-epsilon, y.toDouble), new Coordinate(env.getMaxX+epsilon, y.toDouble)))
    }


    def splitPolygon(polygon: Polygon, theta: TileGranularities): Seq[Polygon] = {

        def split(polygon: Polygon): Seq[Polygon] = {
            val env = polygon.getEnvelopeInternal
            val horizontalBlades = getHorizontalBlades(env, theta.y)
            val verticalBlades = getVerticalBlades(env, theta.x)

            val exteriorRing = polygon.getExteriorRing

            val polygonizer = new Polygonizer()
            val blades: Seq[Geometry] = verticalBlades ++ horizontalBlades
            val union = new UnaryUnionOp(blades.asJava).union()

            polygonizer.add(exteriorRing.union(union))

            val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
            newPolygons.filter(p => polygon.contains(p.getInteriorPoint)).toSeq
        }

        val env = polygon.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(polygon)
        else Seq(polygon)
    }


}
