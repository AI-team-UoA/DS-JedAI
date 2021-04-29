package utils

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object GeometryUtils {

    val csf: CoordinateArraySequenceFactory = CoordinateArraySequenceFactory.instance()
    val geometryFactory = new GeometryFactory()

    val epsilon: Double = 1e-8


    def flattenCollection(collection: Geometry): Seq[Geometry] =
        for (i <- 0 until collection.getNumGeometries) yield {
            val g = collection.getGeometryN(i)
            g.setUserData(collection.getUserData)
            g
        }


    def flattenCollections(srdd: SpatialRDD[Geometry]): SpatialRDD[Geometry] ={
        srdd.rawSpatialRDD = srdd.rawSpatialRDD.rdd.flatMap(g => if (g.getNumGeometries > 1) flattenCollection(g) else Seq(g))
        srdd
    }


    def splitBigGeometries(geometry: Geometry, areaThreshold: Double = 1e-3): List[Geometry] = {
        geometry match {
            case polygon: Polygon if polygon.getEnvelopeInternal.getArea > areaThreshold=>
                val polygons = recursivePolygonSplit(List(polygon), areaThreshold)
                polygons

            case _ => List(geometry)
        }
    }

    /**
     * Recursively, split the polygons into sub-polygons. The procedure is repeated
     * until no produced polygon's area exceed the Area Threshold.
     *
     * @param polygons a list of Polygons
     * @param areaThreshold the Area Threshold
     * @param results the list of sub-polygons produced in the previous recursion
     * @return A list of sub-polygons
     */
    @tailrec
    def recursivePolygonSplit(polygons: List[Polygon], areaThreshold: Double, results: List[Polygon] = Nil): List[Polygon] ={
        val (bigPolygons, smallPolygons) = polygons.partition(p => p.getEnvelopeInternal.getArea > areaThreshold)
        if (bigPolygons.nonEmpty) {
            val newPolygons = bigPolygons.flatMap(p => splitPolygon(p))
            val newResults =  smallPolygons ::: results
            recursivePolygonSplit(newPolygons, areaThreshold, newResults)
        } else
            smallPolygons ::: results
    }


    /**
     * Splits a polygon into sub-polygons, using a horizontal and a vertical line
     * that pass through the centroid.
     *
     * @param polygon polygon
     * @return a list of sub-polygons
     */
    def splitPolygon(polygon: Polygon): List[Polygon] ={

        val exteriorRing = polygon.getExteriorRing
        val interiorRings = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i)).toList
        val horizontalBoundaries = getHorizontalBlade(polygon)
        val verticalBoundaries: List[LineString] = getVerticalBlade(polygon)

        val polygonizer = new Polygonizer()
        val innerGeom: List[Geometry] = verticalBoundaries ::: horizontalBoundaries ::: interiorRings
        val union = new UnaryUnionOp(innerGeom.asJava).union()

        polygonizer.add(exteriorRing.union(union))

        val newPolygons = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])
        val f1 = newPolygons.filter(p => polygon.contains(p.getInteriorPoint))
        f1.toList
    }



    /**
     * Line to split the Polygon Horizontally
     * If the polygon contains inner holes, then it adjust the horizontal line
     * so to not cross the holes.
     *
     * @param polygon polygon
     * @return the horizontal blade
     */
    def getHorizontalBlade(polygon: Polygon): List[LineString] = {

        val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
        val innerRingsWithEnvelopes: Seq[(Envelope, Geometry)] = innerRings.map(ir => (ir.getEnvelopeInternal, ir))

        val centroid = polygon.getCentroid
        val env = polygon.getEnvelopeInternal
        val y = centroid.getY

        // define the horizontal line
        // it will start from MBR.minX, it will pass through the centroid and it will end in MBR.maxX
        val start = new Coordinate(env.getMinX, y)
        val end = new Coordinate(env.getMaxX, y)
        val line = geometryFactory.createLineString(Array(start, end))

        if (innerRings.isEmpty) {
            line :: Nil
        }
        else {
            val segments: ListBuffer[LineString] = new ListBuffer[LineString]()
            var checkpoint = start

            // sort inner rings envelope by X
            innerRingsWithEnvelopes.sortBy(_._1.getMinX).foreach { case (mbr, ir) =>

                // find the ones that intersect with the horizontal line
                if (y >= mbr.getMinY && y <= mbr.getMaxY && checkpoint.x < mbr.getMinX) {

                    // find intersection points between line and inner polygon
                    val intersectionPoints: (Coordinate, Coordinate) = {
                        val ip = ir.intersection(line)
                        val firstPoint = ip.getGeometryN(0).asInstanceOf[Point].getCoordinate
                        val newFirstPoint = new Coordinate(firstPoint.x+epsilon, firstPoint.y)

                        val lastPoint = ip.getGeometryN(1).asInstanceOf[Point].getCoordinate
                        val newLastPoint = new Coordinate(lastPoint.x-epsilon, lastPoint.y)
                        (newFirstPoint, newLastPoint)
                    }

                    // break the line into a smaller segment. The next segment will start out of the inner polygon
                    val segment = geometryFactory.createLineString(Array(checkpoint, intersectionPoints._1))
                    segments += segment
                    checkpoint = intersectionPoints._2
                }
            }

            // create the last line segment
            val segment = geometryFactory.createLineString(Array(checkpoint, end))
            segments += segment
            segments.toList
        }
    }

    /**
     * Line to split the Polygon Vertically
     * If the polygon contains inner holes, then it adjust the vertical line
     * so to not cross the holes.
     *
     * @param polygon polygon
     * @return the vertical blade
     */
    def getVerticalBlade(polygon: Polygon): List[LineString] = {

        val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
        val innerRingsWithEnvelopes: Seq[(Envelope, Geometry)] = innerRings.map(ir => (ir.getEnvelopeInternal, ir))

        val centroid = polygon.getCentroid
        val env = polygon.getEnvelopeInternal
        val x = centroid.getX

        // define the vertical line
        // it will start from MBR.minY, it will pass through the centroid and it will end in MBR.maxY
        val start = new Coordinate(x, env.getMinY)
        val end = new Coordinate(x, env.getMaxY)
        val line = geometryFactory.createLineString(Array(start, end))

        if (innerRings.isEmpty) {
            line :: Nil
        }
        else {
            val segments: ListBuffer[LineString] = new ListBuffer[LineString]()
            var checkpoint = start

            // sort inner rings envelope by y
            innerRingsWithEnvelopes.sortBy(_._1.getMinY).foreach { case (mbr, ir) =>

                // find the ones that intersect with the vertical line
                if (x >= mbr.getMinX && x <= mbr.getMaxX && checkpoint.y < mbr.getMinY) {

                    // find intersection points between line and inner polygon
                    val intersectionPoints: (Coordinate, Coordinate) = {
                        val ip = ir.intersection(line)
                        val firstPoint = ip.getGeometryN(0).asInstanceOf[Point].getCoordinate
                        val newFirstPoint = new Coordinate(firstPoint.x, firstPoint.y+epsilon)

                        val lastPoint = ip.getGeometryN(1).asInstanceOf[Point].getCoordinate
                        val newLastPoint = new Coordinate(lastPoint.x, lastPoint.y-epsilon)
                        (newFirstPoint, newLastPoint)
                    }

                    // break the line into a smaller segment. The next segment will start out of the inner polygon
                    val segment = geometryFactory.createLineString(Array(checkpoint, intersectionPoints._1))
                    segments += segment
                    checkpoint = intersectionPoints._2
                }
            }

            // create the last line segment
            val segment = geometryFactory.createLineString(Array(checkpoint, end))
            segments += segment
            segments.toList
        }
    }
}
