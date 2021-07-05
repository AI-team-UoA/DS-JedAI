package utils.geometryUtils.decompose

import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, LineString, Polygon}
import utils.geometryUtils.GeometryUtils.geomFactory

/**
 * GridDecomposer Trait
 * Decompose based on a grid defined by tile granularities
 * @tparam T type either Envelope or Geometry
 */
trait GridDecomposerT[T] extends DecomposerT[T] {

    /**
     * Find points that define vertical blades based on ThetaX
     * @param env envelope
     * @param thetaX granularity on X axes
     * @return a list of points
     */
    def getVerticalPoints(env: Envelope, thetaX: Double): Seq[Double] ={
        val minX = env.getMinX
        val maxX = env.getMaxX
        val n = math.floor(minX / thetaX) + 1
        val bladeStart: BigDecimal = BigDecimal(thetaX*n)

        for (x <- bladeStart until maxX by thetaX)  yield  x.toDouble
    }

    /**
     * Find points that define horizontal blades based on ThetaY
     * @param env envelope
     * @param thetaY granularity on Y axes
     * @return a list of points
     */
    def getHorizontalPoints(env: Envelope, thetaY: Double): Seq[Double] ={
        val minY = env.getMinY
        val maxY = env.getMaxY
        val n = math.floor(minY/thetaY) + 1
        val bladeStart: BigDecimal = BigDecimal(thetaY*n)

        for (y <- bladeStart until maxY by thetaY) yield y.toDouble
    }

    /**
     * Find vertical blades based on ThetaX
     * @param env envelope
     * @param thetaX granularity on X axes
     * @return a list of lineStrings
     */
    def getVerticalBlades(env: Envelope, thetaX: Double): Seq[LineString] = {
        getVerticalPoints(env, thetaX).map{x =>
            geomFactory.createLineString(Array(
                    new Coordinate(x, env.getMinY - epsilon),
                    new Coordinate(x, env.getMaxY + epsilon)
                ))
        }
    }

    /**
     * Find horizontal blades based on ThetaY
     * @param env envelope
     * @param thetaY granularity on Y axes
     * @return a list of lineStrings
     */
    def getHorizontalBlades(env: Envelope, thetaY: Double): Seq[LineString] = {
        getHorizontalPoints(env, thetaY).map{ y =>
                geomFactory.createLineString(Array(
                    new Coordinate(env.getMinX - epsilon, y),
                    new Coordinate(env.getMaxX + epsilon, y)
                ))
        }
    }


    /**
     * Combine the vertical/horizontal blades with the interior rings of a polygon
     * @param polygon       polygon
     * @param blade         blades
     * @param innerRings    interior rings of polygon
     * @param isHorizontal  blades are horizontal otherwise vertical
     * @return            a list of lineStrings adjusted to inner rings
     */
    def combineBladeWithInteriorRings(polygon: Polygon, blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): Seq[LineString] = {

        // epsilon is a small value to add in the segments so to slightly intersect thus not result to dangling lines
        val (xEpsilon, yEpsilon) = if (isHorizontal) xYEpsilon else xYEpsilon.swap

        // the blade is a linestring formed by two points
        val start = blade.getCoordinateN(0)
        val end = blade.getCoordinateN(1)

        // define the cross condition based on line's orientation
        val crossCondition: Envelope => Boolean = if (isHorizontal) env => start.y >= env.getMinY && start.y <= env.getMaxY
        else env => start.x >= env.getMinX && start.x <= env.getMaxX

        // ordering of the coordinates, to define max and min
        implicit val ordering: Ordering[Coordinate] = Ordering.by[Coordinate, Double](c => if (isHorizontal) c.x else c.y)

        // sort inner rings envelope by y
        // find the ones that intersect with the line
        // for each intersecting inner ring find the intersection coordinates,
        //   - sort them and get the first and the last,
        //   - create line segments that do not overlap the inner ring
        val checkpoint = start

        val segments: Seq[LineString] = innerRings
            .map(ir => (ir, ir.getEnvelopeInternal))
            .filter{ case (_, env) => crossCondition(env)}
            .sortBy{ case (_, env) => if (isHorizontal) env.getMinX else env.getMinY }
            .flatMap { case (ir, _) =>

                val intersectingCollection = ir.intersection(blade)
                val intersectionPoints: Seq[Coordinate] = (0 until intersectingCollection.getNumGeometries)
                    .map(i => intersectingCollection.getGeometryN(i))
                    .flatMap(g => g.getCoordinates)
                    .sorted(ordering)

                val segmentsPoints = start +: intersectionPoints :+ end
                for (point <- segmentsPoints.sliding(2, 2)) yield {
                    val start = point.head
                    start.setX(start.x + xEpsilon)
                    start.setY(start.y + yEpsilon)

                    val end = point.last
                    end.setX(end.x - xEpsilon)
                    end.setY(end.y - yEpsilon)
                    geometryFactory.createLineString(Array(start, end))
                }
            }

        // create the last line segment
        val segment = geometryFactory.createLineString(Array(checkpoint, end))
        segments :+ segment
    }
}
