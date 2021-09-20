package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._
import org.locationtech.jts.operation.polygonize.Polygonizer
import org.locationtech.jts.operation.union.UnaryUnionOp
import utils.geometryUtils.GeometryUtils
import utils.geometryUtils.GeometryUtils.flattenCollection

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer

/**
  * Decompose Geometry based on the grid defined by tile-granularities
  * @param theta tile granularity
 */
case class GridDecomposer(theta: TileGranularities) extends GridDecomposerT[Geometry] {


    /**
     * Decompose a geometry
     * @param geometry geometry
     * @return a list of geometries
     */
    def decomposeGeometry(geometry: Geometry): Seq[Geometry] = {
        geometry match {
            case polygon: Polygon => decomposePolygon(polygon)
            case line: LineString => decomposeLineString(line)
            case gc: GeometryCollection => flattenCollection(gc).flatMap(g => decomposeGeometry(g))
            case _ => Seq(geometry)
        }
    }


    /**
     * Decompose a polygon into multiple segments
     * @param polygon polygon
     * @return a list of segments geometries
     */
    def decomposePolygon(polygon: Polygon): Seq[Geometry] = {

        def split(polygon: Polygon): Seq[Geometry] = {

            val blades = {
                val innerRings: Seq[Geometry] = (0 until polygon.getNumInteriorRing).map(i => polygon.getInteriorRingN(i))
                // find blades based on which to split
                val horizontalBlades = getHorizontalBlades(polygon.getEnvelopeInternal, theta.y)
                    .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = true))
                val verticalBlades = getVerticalBlades(polygon.getEnvelopeInternal, theta.x)
                    .flatMap(b => combineBladeWithInteriorRings(polygon, b, innerRings, isHorizontal = false))

                val blades: Seq[Geometry] = verticalBlades ++ horizontalBlades ++ innerRings
                if (blades.nonEmpty) new UnaryUnionOp(blades.asJava).union() else geometryFactory.createEmpty(2)
            }

            val polygonWithBlades = polygon.getExteriorRing.union(blades)
            val polygonizer = new Polygonizer()
            polygonizer.add(polygonWithBlades)
            val segments = polygonizer.getPolygons.asScala.map(p => p.asInstanceOf[Polygon])

            // filter the polygons that cover holes
            segments
                .filter(p => polygon.contains(p.getInteriorPoint))
                .map(p => GeometryUtils.reducePrecision(p))
                .toSeq
        }

        val env = polygon.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(polygon)
        else Seq(polygon)
    }

    /**
     * Decompose a lineString into multiple segments
     * @param line LineString
     * @return a list of segments geometries
     */
    def decomposeLineString(line: LineString): Seq[Geometry] = {

        def split(line: LineString): Seq[Geometry] = {
            // find blades based on which to split
            val horizontalBlades = getHorizontalBlades(line.getEnvelopeInternal, theta.y)
            val verticalBlades = getVerticalBlades(line.getEnvelopeInternal, theta.x)
            val blades = geometryFactory.createMultiLineString((verticalBlades ++ horizontalBlades).toArray)

            // find line segments by removing the intersection points
            val lineSegments = line.difference(blades).asInstanceOf[MultiLineString]
            (0 until lineSegments.getNumGeometries).map(i => lineSegments.getGeometryN(i))
                .map(l => GeometryUtils.reducePrecision(l))

        }
        val env = line.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) split(line)
        else Seq(line)
    }

    /**
     * Decompose a lineString into multiple segments
     * significantly slower
     * @param line LineString
     * @return a list of segments geometries
     */
    def decomposeLineString_(line: LineString): Seq[Geometry] = {
        val env = line.getEnvelopeInternal
        val verticalPointsSeq = env.getMinX +: getVerticalPoints(env, theta.x) :+ env.getMaxX
        val horizontalPointsSeq = env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY

        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        // a list of functions that given an edge returns the index of the segments it belongs to
        // most probably it will return a single region
        val regionsConditions: Seq[(Coordinate, Coordinate) => Option[Int]] =
            (for (x <- verticalPoints.sliding(2); y <- horizontalPoints.sliding(2)) yield (x, y))
                .zipWithIndex.map { case ((x, y), i) =>
                    val (minX, maxX) = (x.head, x.last)
                    val (minY, maxY) = (y.head, y.last)
                    (head: Coordinate, last: Coordinate) =>
                        if (head.x >= minX && head.x <= maxX && head.y >= minY && head.y <= maxY &&
                            last.x >= minX && last.x <= maxX && last.y >= minY && last.y <= maxY
                    ) Some(i) else None
            }.toSeq

        @tailrec
        def computeGeometrySegments(geometryCoordinates: List[Coordinate], previousNode: Coordinate,
                                    segmentsCoordinates: Array[ListBuffer[ListBuffer[Coordinate]]]): Array[ListBuffer[ListBuffer[Coordinate]]] = {
            geometryCoordinates match {
                case Nil => segmentsCoordinates
                case currentNode :: tail =>
                    // define coordinate ordering
                    val coordinateOrdering = if (previousNode.compareTo(currentNode) > 1) implicitly[Ordering[Coordinate]] else implicitly[Ordering[Coordinate]].reverse
                    val intermediatePoints = findIntermediatePoints(previousNode, currentNode, verticalPoints, horizontalPoints).sorted(coordinateOrdering)

                    // compute the points within the edge - if returned points do not contain the points of the edge
                    // we add them
                    val coordinates = intermediatePoints match {
                        case Nil => previousNode :: currentNode :: Nil
                        case points =>
                            val points_ = if (points.head != previousNode) previousNode +:points else points
                            if (points_.last != currentNode) points_ :+ currentNode else points_
                    }
                    coordinates.sliding(2).foreach { c =>
                        val startOfEdge = c.head
                        val endOfEdge = c.last
                        val regionIndices =  regionsConditions.flatMap(regionF => regionF(startOfEdge, endOfEdge))
                        regionIndices.foreach { i =>
                            val listOfLists: ListBuffer[ListBuffer[Coordinate]] = segmentsCoordinates(i)
                            if (listOfLists.isEmpty) {
                                // if it's empty we add the start and the end
                                val coordinatesList = new ListBuffer[Coordinate]()
                                coordinatesList.append(startOfEdge)
                                coordinatesList.append(endOfEdge)
                                listOfLists.append(coordinatesList)
                            }
                            else {
                                // if it's not empty there are two cases:
                                //  - it is consecutive i.e. the last node is the same as the start => we extend the existing list
                                //  - it is not consecutive => a new geometry starts  i.e. a new list is initialized
                                val lastList = listOfLists.last
                                val lastCoordinate = lastList.last
                                if (lastCoordinate == startOfEdge) {
                                    lastList.append(endOfEdge)
                                }
                                else{
                                    val coordinatesList = new ListBuffer[Coordinate]()
                                    coordinatesList.append(startOfEdge)
                                    coordinatesList.append(endOfEdge)
                                    listOfLists.append(coordinatesList)
                                }
                            }
                        }
                    }
                    computeGeometrySegments(tail, currentNode, segmentsCoordinates)
            }
        }

        // An Array of empty Envelopes - this will contain the fine-grained envelopes
        val emptySegment: Array[ListBuffer[ListBuffer[Coordinate]]] = Array.fill(regionsConditions.length)(ListBuffer[ListBuffer[Coordinate]]())
        val coordinateList = line.getCoordinates.toList
        val segments = computeGeometrySegments(coordinateList.tail, coordinateList.head, emptySegment)

        segments.filter(_.nonEmpty).flatMap(seq => seq.filter(_.size > 1).map(l => geometryFactory.createLineString(l.toArray)))
    }
}
