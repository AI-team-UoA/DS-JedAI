package utils.geometryUtils


import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ListBuffer
import scala.util.Try

object GeometryUtils {

    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val epsilon: Double = 1e-8

    def flattenCollection(collection: Geometry): Seq[Geometry] =
        for (i <- 0 until collection.getNumGeometries) yield {
            val g = collection.getGeometryN(i)
            g.setUserData(collection.getUserData)
            g
        }


    def flattenSRDDCollections(srdd: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
        srdd.rawSpatialRDD = srdd.rawSpatialRDD.rdd.flatMap(g => if (g.getNumGeometries > 1) flattenCollection(g) else Seq(g))
        srdd
    }

    /**
     * Get the central points by splitting the segments defined by the two points recursively
     * until the extend of the segment does not exceed the threshold
     * @param c1 first point
     * @param c2 last point
     * @param threshold threshold
     * @return a list of interior points
     */
    def getCenterPoints(c1: Double, c2: Double, threshold: Double): List[Double] = {

        @scala.annotation.tailrec
        def getCenterPoints(points: List[Double], threshold: Double, accumulatedPoints: ListBuffer[Double]): List[Double] = {
            points match {
                case start :: end :: tail if math.abs(end - start) > threshold =>
                    val mid = (start + end) / 2
                    getCenterPoints(start :: mid :: end :: tail, threshold, accumulatedPoints)
                case start :: end :: tail =>
                    accumulatedPoints.appendAll(start :: end :: Nil)
                    getCenterPoints(tail, threshold, accumulatedPoints)
                case last :: Nil =>
                    accumulatedPoints.append(last)
                    accumulatedPoints.toList
                case Nil =>
                    accumulatedPoints.toList
            }
        }

        getCenterPoints(List(c1, c2), threshold, new ListBuffer[Double]())
    }

    /**
     * compute the interior points of the edge/LineSegment [c1, c2] that crosses one of the horizontal blades.
     * To compute the interior points, we find the line of the edge
     *      y = a*x + b
     * and then we solve toward x given y (horizontal blade).
     * The final points are computed as ( (y-b)/a, y)
     * @param c1 first point of edge
     * @param c2 last point of edge
     * @param yes a list of y defining the horizontal blades
     * @return a list of intermediate points
     */
    def getIntersectionWithHorizontalLine(c1: Coordinate, c2: Coordinate, yes: Iterable[Double]): Iterable[Coordinate] ={
        val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
        val slopeOpt = Try((c2.y - c1.y) / (c2.x - c1.x)).toOption
        slopeOpt match {
            case None | Some(0) =>
                Nil
            case Some(Double.PositiveInfinity) | Some(Double.NegativeInfinity) =>
                yes.map(y => new Coordinate(c1.x, y))
            case Some(slope) =>
                val b = c1.y - c1.x*slope
                yes.filter(y => y >= minY && y <= maxY)
                    .map(y =>
                        new Coordinate( (y-b)/slope, y))
            }
    }

    /**
     * compute the interior points of the edge/LineSegment [c1, c2] that crosses one of the vertical blades.
     * To compute the interior points, we find the line of the edge
     *      y = a*x + b
     * and then we solve toward y given x (vertical blade).
     * The final points are computed as (x, x*a+b)
     * @param c1 first point of edge
     * @param c2 last point of edge
     * @param xes a list of x defining the vertical blades
     * @return a list of intermediate points
     */
    def getIntersectionWithVerticalLine(c1: Coordinate, c2: Coordinate, xes: Iterable[Double]): Iterable[Coordinate] ={
        val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
        val slopeOpt = Try((c2.y - c1.y) / (c2.x - c1.x)).toOption
        slopeOpt match {
            case None | Some(0) =>
                xes.map(x => new Coordinate(x, c1.y))
            case Some(Double.PositiveInfinity) | Some(Double.NegativeInfinity) =>
                Nil
            case Some(slope) =>
                val b = c1.y - c1.x*slope
                xes.filter(x => x >= minX && x <= maxX)
                    .map(x => new Coordinate(x, x*slope + b))
        }
    }
}
