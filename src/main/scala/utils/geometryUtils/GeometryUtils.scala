package utils.geometryUtils


import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ListBuffer
import scala.util.Try

object GeometryUtils {

    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()

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

    def getCenterPoints(points: List[Double], threshold: Double): List[Double] = {

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

        getCenterPoints(points, threshold, new ListBuffer[Double]())
    }

    def getHorizontalIntersectingPoints(c1: Coordinate, c2: Coordinate, yes: Iterable[Double]): Iterable[Coordinate] ={
        val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
        val slopeOpt = Try((c2.y - c1.y) / (c2.x - c1.x)).toOption
        slopeOpt match {
            case None | Some(0) =>
                Nil
            case Some(Double.PositiveInfinity) =>
                yes.map(y => new Coordinate(c1.x, y))
            case Some(slope) =>
                val b = c1.y - c1.x*slope
                yes.filter(y => y >= minY && y <= maxY)
                    .map(y => new Coordinate( (y-b)/slope, y))
            }
    }

    def getVerticalIntersectingPoints(c1: Coordinate, c2: Coordinate, xes: Iterable[Double]): Iterable[Coordinate] ={
        val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
        val slopeOpt = Try((c2.y - c1.y) / (c2.x - c1.x)).toOption
        slopeOpt match {
            case None | Some(0) =>
                xes.map(x => new Coordinate(x, c1.y))
            case Some(Double.PositiveInfinity) =>
                Nil
            case Some(slope) =>
                val b = c1.y - c1.x*slope
                xes.filter(x => x >= minX && x <= maxX)
                    .map(x => new Coordinate(x, x*slope + b))
        }
    }
}
