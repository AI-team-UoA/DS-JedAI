package utils.geometryUtils.decompose

import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, LineString, Polygon}
import utils.geometryUtils.GeometryUtils
import utils.geometryUtils.GeometryUtils.geomFactory

import scala.collection.SortedSet
import scala.util.Try

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
        val bladeStart = if (thetaX != 0){
            val n = math.floor(minX / thetaX) + 1
            BigDecimal(thetaX*n)
        }
        else
            BigDecimal(minX)
        val step = if (thetaX == 0) 1 else  thetaX
        for (x <- bladeStart until maxX by step)  yield x.toDouble
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
        val bladeStart = if (thetaY != 0){
            val n = math.floor(minY / thetaY) + 1
            BigDecimal(thetaY*n)
        }
        else
            BigDecimal(minY)
        val step = if (thetaY == 0) 1 else  thetaY
        for (y <- bladeStart until maxY by step) yield y.toDouble
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
     * Find the intermediate points in the edge (c1, c2) that either their x is in the set of vertical points (Xes)
     * or their y is in the set of horizontal points (Yes)
     * @param c1                start of edge
     * @param c2                end of edge
     * @param verticalPoints    set of Xes
     * @param horizontalPoints  set of Yes
     * @return
     */
    def findIntermediatePoints(c1: Coordinate, c2: Coordinate, verticalPoints: SortedSet[Double], horizontalPoints: SortedSet[Double]): List[Coordinate] ={
        val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
        val vp = verticalPoints.from(minX).to(maxX)
        val intersectingVerticalPoints = GeometryUtils.getIntersectionWithVerticalLine(c1, c2, vp).toList

        val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
        val hp = horizontalPoints.from(minY).to(maxY)
        val intersectingHorizontalPoints = GeometryUtils.getIntersectionWithHorizontalLine(c1, c2, hp).toList
        intersectingHorizontalPoints ::: intersectingVerticalPoints
    }


    /**
     * Combine the vertical/horizontal blades with the interior rings of a polygon
     * @param blade         blades
     * @param innerRings    interior rings of polygon
     * @param isHorizontal  blades are horizontal otherwise vertical
     * @return            a list of lineStrings adjusted to inner rings
     */
    def combineBladeWithInteriorRings(blade: LineString, innerRings: Seq[Geometry], isHorizontal: Boolean): Seq[LineString] = {

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
