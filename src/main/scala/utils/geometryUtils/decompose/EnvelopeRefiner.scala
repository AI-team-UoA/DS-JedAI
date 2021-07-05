package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, LineString, LinearRing, Polygon}
import utils.geometryUtils.GeometryUtils

import scala.collection.SortedSet
import scala.math.ceil

/**
 * EnvelopeRefiner creates Fine-Grained Envelopes as a more precise approximation than the initial
 * @param theta tile granularity
 */
case class EnvelopeRefiner(theta: TileGranularities) extends GridDecomposerT[Envelope] {

    val SPLIT_LOG_BASE: Int = 50

    /**
     * Generate Fine-Grained Envelopes for input polygon
     * @param polygon input polygon
     * @return List of Fine-Grained envelopes
     */
    override def decomposePolygon(polygon: Polygon): Seq[Envelope] = decomposeGeometry(polygon)

    /**
     * Generate Fine-Grained Envelopes for input line
     * @param line input line
     * @return List of Fine-Grained envelopes
     */
    override def decomposeLineString(line: LineString): Seq[Envelope] = decomposeGeometry(line)

    /**
     * Generate Fine-Grained Envelopes for input geometry
     * @param geometry input geometry
     * @return List of Fine-Grained envelopes
     */
    override  def decomposeGeometry(geometry: Geometry): List[Envelope] ={

        /**
         * Define threshold that computes how many times to split
         * It is based on *theta* and *SPLIT_LOG_BASE*.
         *      If theta >= SPLIT_LOG_BASE -> splittingT >= theta
         *      if theta < SPLIT_LOG_BASE  -> splittingT < theta
         * We use this method in order to restrict the numbers of splits, in cases
         * that a geometry exceeds significantly theta. To do this, we use an
         * inverse logarithmic function.
         *
         * @param env input envelope
         * @return a new adjusted theta threshold
         */
        def getSplitGranularity(env: Envelope): TileGranularities = {
            val log: (Int, Int) => Double = (x, b) => math.log10(x)/math.log10(b)

            val xTimes = ceil(env.getWidth / theta.x).toInt
            val xRatio =
                if (xTimes != 0){
                    val xTimesAdjusted = math.ceil((1/log(xTimes+1, SPLIT_LOG_BASE)) * xTimes)
                    xTimes/xTimesAdjusted
                }
                else 1

            val yTimes = ceil(env.getHeight / theta.y).toInt
            val yRatio =
                if (yTimes != 0){
                    val yTimesAdjusted = math.ceil((1/log(yTimes+1, SPLIT_LOG_BASE)) * yTimes)
                    yTimes/yTimesAdjusted
                }
                else 1

            theta * (xRatio, yRatio)
        }

        // check if exceeds threshold
        val env = geometry.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) {
            val splitTheta = getSplitGranularity(env)
            refineEnvelope(geometry, splitTheta)
        }
        else
            List(env)
    }

    /**
     * Produce the Fine-Grained Envelopes
     * @param geom input geometry
     * @param theta splitting threshold
     * @return a List of Fine-Grained Envelopes
     */
    def refineEnvelope(geom: Geometry, theta: TileGranularities): List[Envelope] ={

        /**
         * A mutable, easy to update class of MBR, for computing Fine-Grained Envelopes
         * @param minX minX
         * @param maxX maxX
         * @param minY minY
         * @param maxY maxY
         */
        case class MBR(var minX: Double = Double.PositiveInfinity, var maxX: Double = Double.NegativeInfinity,
                       var minY: Double = Double.PositiveInfinity, var maxY: Double = Double.NegativeInfinity){

            def update(x: Double, y: Double): Unit = {
                if (minX > x) minX = x
                if (maxX < x) maxX = x
                if (minY > y) minY = y
                if (maxY < y) maxY = y
            }
            def getEnvelope: Envelope = new Envelope(minX, maxX, minY, maxY)
            def getGeometry: Geometry = geometryFactory.toGeometry(getEnvelope)
            def isEmpty: Boolean = minX == Double.PositiveInfinity || maxX == Double.NegativeInfinity ||
                minY == Double.PositiveInfinity || maxY == Double.NegativeInfinity
        }

        // find the points based on which it will split
        // the points are based on the grid defined by theta
        val env: Envelope = geom.getEnvelopeInternal
        val (verticalPointsSeq,horizontalPointsSeq) =
        geom match {
            case _: LineString =>
                // in case of lineString we consider both dimensions
                val vertical = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
                val horizontal= env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
                (vertical, horizontal)
            case _ if env.getWidth > env.getHeight =>
                // in case of polygons, we consider only one dimension
                // cut vertically
                val vertical = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
                val horizontal = env.getMinY :: env.getMaxY :: Nil
                (vertical, horizontal)
            case _ =>
                // cut horizontally
                val vertical = env.getMinX :: env.getMaxX :: Nil
                val horizontal = env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
                (vertical, horizontal)
        }
        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        // a list of functions that given a point returns the index of the envelope it belongs to
        val regionsConditions: Seq[(Double, Double) => Option[Int]] =
            (for (x <- verticalPoints.sliding(2); y <- horizontalPoints.sliding(2)) yield (x,y))
                .zipWithIndex.map{ case ((x, y), i) =>
                    (x1: Double, y1: Double) =>
                        if (x1 >= x.head && x1 <= x.last && y1 >= y.head && y1 <= y.last) Some(i) else None
                }.toSeq
        // An Array of empty Envelopes - this will contain the fine-grained envelopes
        val envelopes: Array[MBR] = Array.fill(regionsConditions.length)(MBR())

        geom match {
            case polygon: Polygon =>
                // in case of polygons, we consider interior rings as well
                val exteriorRing: LinearRing = polygon.getExteriorRing
                val interiorRings: Seq[LinearRing] = for (i <- 0 until polygon.getNumInteriorRing) yield polygon.getInteriorRingN(i)

                // for each coordinate of the geometry, get the edges and find the intermediate points
                // in case it intersects one of the blades.
                // Then, find the Envelope each point lies, and update its extend
                for (ring <- exteriorRing +: interiorRings) {
                    for (c <- ring.getCoordinates.sliding(2)) {
                        val c1 = c.head
                        val c2 = c.last
                        val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
                        for (p <- c1 +: intermediatePoints :+ c2) {
                            regionsConditions.flatMap(regionF => regionF(p.x, p.y))
                                .foreach (i => envelopes(i).update(p.x, p.y) )
                        }
                    }
                }
            case _ =>
                for (c <- geom.getCoordinates.sliding(2)){
                    val c1 = c.head
                    val c2 = c.last
                    val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
                    for (p <- c1 +: intermediatePoints :+ c2){
                        regionsConditions.flatMap(regionF => regionF(p.x, p.y))
                            .foreach (i => envelopes(i).update(p.x, p.y) )
                    }
                }
        }

        envelopes.filter(! _.isEmpty).map(e => e.getEnvelope).toList
    }


    def findIntermediatePoints(c1: Coordinate, c2: Coordinate, verticalPoints: SortedSet[Double], horizontalPoints: SortedSet[Double]): List[Coordinate] ={
        val (maxX, minX) = if (c1.x > c2.x) (c1.x, c2.x) else (c2.x, c1.x)
        val vp = verticalPoints.from(minX).to(maxX)
        val intersectingVerticalPoints = GeometryUtils.getIntersectionWithVerticalLine(c1, c2, vp).toList

        val (maxY, minY) = if (c1.y > c2.y) (c1.y, c2.y) else (c2.y, c1.y)
        val hp = horizontalPoints.from(minY).to(maxY)
        val intersectingHorizontalPoints = GeometryUtils.getIntersectionWithHorizontalLine(c1, c2, hp).toList
        intersectingHorizontalPoints ::: intersectingVerticalPoints
    }

}
