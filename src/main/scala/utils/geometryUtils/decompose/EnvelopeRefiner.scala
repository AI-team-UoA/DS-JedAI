package utils.geometryUtils.decompose

import model.TileGranularities
import org.locationtech.jts.geom._

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
    override def decomposePolygon(polygon: Polygon): List[Envelope] = refine(polygon, theta)

    /**
     * Generate Fine-Grained Envelopes for input line
     * @param line input line
     * @return List of Fine-Grained envelopes
     */
    override def decomposeLineString(line: LineString): List[Envelope] = refine(line, theta)

    /**
     * Generate Fine-Grained Envelopes for input geometry
     * @param geometry input geometry
     * @return List of Fine-Grained envelopes
     */
    override def decomposeGeometry(geometry: Geometry): List[Envelope] = refine(geometry, theta)

    /** Produce the Fine-Grained Envelopes
     * @param geometry input geometry
     * @param theta splitting threshold
     * @return a List of Fine-Grained Envelopes
     */
    def refine(geometry: Geometry, theta: TileGranularities): List[Envelope] ={
        val env = geometry.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) {
            geometry match {
                case line: LineString => refineLineString(line, theta)
                case _ => refineGeometry(geometry, theta)
            }
        }
        else
            List(env)
    }

    /**
     * A mutable, easy to update class of MBR, for computing Fine-Grained Envelopes
     * The fine-Grained envelope of the given region will be defined by the (minX, maxX, minY, maxY)
     *
     * @param region an envelope that describes the initial area of the MBR
     * @param minX minX
     * @param maxX maxX
     * @param minY minY
     * @param maxY maxY
     */
    case class MBR(region: Envelope, var minX: Double = Double.PositiveInfinity, var maxX: Double = Double.NegativeInfinity,
                   var minY: Double = Double.PositiveInfinity, var maxY: Double = Double.NegativeInfinity) {

        /**
         * check if a given point belongs to the envelope
         * @param x latitude
         * @param y longitude
         * @return true if the point is inside the envelope
         */
        def contains(x: Double, y: Double): Boolean = (region.getMinX <= x && region.getMaxX >= x) && (region.getMinY <= y && region.getMaxY >= y)

        def isEmpty: Boolean = minX == Double.PositiveInfinity || maxX == Double.NegativeInfinity || minY == Double.PositiveInfinity || maxY == Double.NegativeInfinity

        def nonEmpty: Boolean = !isEmpty

        def update(x: Double, y: Double): Unit = {
            if (minX > x) minX = x
            if (maxX < x) maxX = x
            if (minY > y) minY = y
            if (maxY < y) maxY = y
        }

        def getEnvelope: Envelope = new Envelope(minX, maxX, minY, maxY)

        def getGeometry: Geometry = geometryFactory.toGeometry(getEnvelope)
    }

    /**
     * For a given theta find the fine-grained envelopes
     * @param geometry a geometry
     * @param theta splitting threshold
     * @return fine-grained envelopes
     */
    def refineGeometry(geometry: Geometry, theta: TileGranularities): List[Envelope] = {

        // find the points based on which it will split
        // the points are based on the grid defined by theta
        val env: Envelope = geometry.getEnvelopeInternal
        val (verticalPointsSeq, horizontalPointsSeq, envelopes): (Seq[Double], Seq[Double], List[MBR]) = geometry match {
            case _ if env.getWidth > env.getHeight =>
                // cut vertically
                val vertical = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
                val horizontal = env.getMinY :: env.getMaxY :: Nil
                val envelopes = vertical.sliding(2).map(Xes => MBR(new Envelope(Xes.head, Xes.last, env.getMinY, env.getMaxY), minX = Xes.head, maxX = Xes.last)).toList
                (vertical, horizontal, envelopes)
            case _ =>
                // cut horizontally
                val vertical = env.getMinX :: env.getMaxX :: Nil
                val horizontal = env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
                val envelopes = horizontal.sliding(2).map(Yes => MBR(new Envelope(env.getMinX, env.getMaxX, Yes.head, Yes.last), minY = Yes.head, maxY = Yes.last)).toList
                (vertical, horizontal, envelopes)
        }

        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        for (c <- geometry.getCoordinates.sliding(2)) {
            val c1 = c.head
            val c2 = c.last

            // find intermediate points, might be necessary for the borders of the envelopes.
            val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
            for (c <- c1 +: intermediatePoints :+ c2) {
                //TODO instead of filter, try find and see if produces correct results
                envelopes.filter(env => env.contains(c.x, c.y)).foreach(env => env.update(c.x, c.y))
            }
        }
        val envs = envelopes.filter(_.nonEmpty)
        envs.map(e => e.getEnvelope)
    }


    /**
     * For a given theta find the fine-grained envelopes
     * @param linestring a linestring
     * @param theta splitting threshold
     * @return fine-grained envelopes
     */
    def refineLineString(linestring: LineString, theta: TileGranularities): List[Envelope] = {

        val env: Envelope = linestring.getEnvelopeInternal
        val verticalPointsSeq = env.getMinX +:getVerticalPoints(env, theta.x) :+ env.getMaxX
        val horizontalPointsSeq = env.getMinY +: getHorizontalPoints(env, theta.y) :+ env.getMaxY
        val envelopes: List[MBR] = (for (x <- verticalPointsSeq.sliding(2);
                                         y <- horizontalPointsSeq.sliding(2)) yield { MBR(new Envelope(x.head, x.last, y.head, y.last))
                                    }).toList

        val verticalPoints: SortedSet[Double] = collection.SortedSet(verticalPointsSeq: _*)
        val horizontalPoints: SortedSet[Double] = collection.SortedSet(horizontalPointsSeq: _*)

        for (c <- linestring.getCoordinates.sliding(2)) {
            val c1 = c.head
            val c2 = c.last
            val intermediatePoints = findIntermediatePoints(c1, c2, verticalPoints, horizontalPoints)
            for (c <- c1 +: intermediatePoints :+ c2)
                envelopes.filter(env => env.contains(c.x, c.y)).foreach(env => env.update(c.x, c.y))
        }
        val envs = envelopes.filter(_.nonEmpty)
        envs.map(e => e.getEnvelope)
    }


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
}
