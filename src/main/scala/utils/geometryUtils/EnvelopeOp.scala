package utils.geometryUtils

import model.TileGranularities
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, LineString, Point}
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes.EnvelopeIntersectionTypes

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import math._


object EnvelopeOp {

    val epsilon: Double = 1e-8
    val geometryFactory = new GeometryFactory()
    val SPLIT_CONST: Int = 200

    def checkIntersection(env1: Envelope, env2: Envelope, relation: Relation): Boolean = {
        relation match {
            case Relation.CONTAINS | Relation.COVERS => env1.contains(env2)
            case Relation.WITHIN | Relation.COVEREDBY => env2.contains(env1)
            case Relation.INTERSECTS | Relation.CROSSES | Relation.OVERLAPS | Relation.DE9IM=> env1.intersects(env2)
            case Relation.TOUCHES => env1.getMaxX == env2.getMaxX || env1.getMinX == env2.getMinX || env1.getMaxY == env2.getMaxY || env1.getMinY == env2.getMinY
            case Relation.DISJOINT => env1.disjoint(env2)
            case Relation.EQUALS => env1.equals(env2)
            case _ => false
        }
    }

    object EnvelopeIntersectionTypes extends Enumeration {
        type EnvelopeIntersectionTypes = Value
        // order defines ordering
        val RANK1, RANK2, RANK3, RANK0 = Value
    }
    def getIntersectingEnvelopesType(env1: Envelope, env2: Envelope): EnvelopeIntersectionTypes ={
        if (env1.disjoint(env2))
            EnvelopeIntersectionTypes.RANK0
        else if (env1.contains(env2) || env2.contains(env1))
            EnvelopeIntersectionTypes.RANK3
        else if ( (env1.getMinX == env2.getMinX && env1.getMaxX == env2.getMaxX) || (env1.getMinY == env2.getMinY && env1.getMaxY == env2.getMaxY))
            EnvelopeIntersectionTypes.RANK1
        else if ( (env2.getMinX == env1.getMinX && env2.getMaxX == env1.getMaxX) || (env2.getMinY == env1.getMinY && env2.getMaxY == env1.getMaxY))
            EnvelopeIntersectionTypes.RANK1
        else
            EnvelopeIntersectionTypes.RANK2
    }


    /**
     * check if the envelopes satisfy the input relations
     *
     * @param env1 envelope
     * @param env2 envelope
     * @param relations a sequence of relations
     * @return true if the envelope satisfy all relations
     */
    def intersectingMBR(env1: Envelope, env2: Envelope, relations: Seq[Relation]): Boolean = relations.exists { r => checkIntersection(env1, env2, r) }

    def getArea(env: Envelope): Double = env.getArea

    def getIntersectingInterior(env1: Envelope, env2: Envelope): Envelope = env1.intersection(env2)

    def getCentroid(env: Envelope): Point = {
        val x = (env.getMaxX + env.getMinX)/2
        val y = (env.getMaxY + env.getMinY)/2
        geometryFactory.createPoint(new Coordinate(x, y))
    }

    def adjust(env: Envelope, tileGranularities: TileGranularities): Envelope ={
        val maxX = env.getMaxX / tileGranularities.x
        val minX = env.getMinX / tileGranularities.x
        val maxY = env.getMaxY / tileGranularities.y
        val minY = env.getMinY / tileGranularities.y

        new Envelope(minX, maxX, minY, maxY)
    }


    def getFineGrainedEnvelope(geom: Geometry, theta: TileGranularities): List[Envelope] ={

        def getSplitGranularity(env: Envelope): TileGranularities = {
            val log: (Int, Int) => Double = (x, b) => math.log10(x)/math.log10(b)

            val xTimes = ceil(env.getWidth / theta.x).toInt
            val xTimesAdjusted = math.ceil((1/log(xTimes+1, SPLIT_CONST)) * xTimes)
            val xRatio = xTimesAdjusted/xTimes


            val yTimes = ceil(env.getHeight / theta.y).toInt
            val yTimesAdjusted = math.ceil((1/log(yTimes+1, SPLIT_CONST)) * yTimes)
            val yRatio = yTimesAdjusted/yTimes

            theta * (xRatio, yRatio)
        }

        val env = geom.getEnvelopeInternal
        if (env.getWidth > theta.x || env.getHeight > theta.y) {
            val splitTheta = getSplitGranularity(env)

            val fineGrainedEnvelopes = envelopeSplit(Left(geom), splitTheta)

//            geom match {
//                case _: Polygon =>
//                    var innerMaxX = fineGrainedEnvelopes.head.getMinX
//                    var innerMinX = fineGrainedEnvelopes.head.getMaxX
//                    var innerMaxY = fineGrainedEnvelopes.head.getMinY
//                    var innerMinY = fineGrainedEnvelopes.head.getMaxY
//
//                    fineGrainedEnvelopes.tail.foreach { e =>
//                        if (e.getMinX > innerMaxX) innerMaxX = e.getMinX
//                        if (e.getMaxX < innerMinX) innerMinX = e.getMaxX
//                        if (e.getMinY > innerMinY) innerMaxY = e.getMinY
//                        if (e.getMaxY < innerMinY) innerMinY = e.getMaxY
//                    }
//                    val innerEnv = new Envelope(innerMinX, innerMaxX, innerMinY, innerMaxY)
//                    val innerEnvs = envelopeSplit(Right(innerEnv), theta)//.filter(e => geom.contains(getCentroid(e)))
//                    fineGrainedEnvelopes ::: innerEnvs
//                case _ =>
//                    fineGrainedEnvelopes
//            }
            fineGrainedEnvelopes
        }
        else
            List(env)
    }


    def envelopeSplit(geom: Either[Geometry, Envelope], theta: TileGranularities): List[Envelope] ={

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

        @scala.annotation.tailrec
        def getCenterPoints(points: List[Double], threshold: Double, accumulatedPoints: ListBuffer[Double]): List[Double] ={
            points match {
                case start :: end :: tail if math.abs(end - start) > threshold =>
                    val mid = (start + end) / 2
                    getCenterPoints(start :: mid :: end :: tail, threshold, accumulatedPoints)
                case start :: end :: tail =>
                    accumulatedPoints.appendAll( start :: end :: Nil)
                    getCenterPoints(tail, threshold, accumulatedPoints)
                case last :: Nil =>
                    accumulatedPoints.append(last)
                    accumulatedPoints.toList
                case Nil =>
                    accumulatedPoints.toList
            }
        }

        val env: Envelope = geom match {
            case Right(e) => e
            case Left(g) => g.getEnvelopeInternal
        }

        val verticalPoints: Seq[Double] = getCenterPoints(List(env.getMinX, env.getMaxX), theta.x, new ListBuffer[Double])
        val horizontalPoints: immutable.Seq[Double] = getCenterPoints(List(env.getMinY, env.getMaxY), theta.y, new ListBuffer[Double])

        val coordinates: Seq[Coordinate] = geom match {
            case Left(g) =>
                val verticalBlades: Seq[LineString] = verticalPoints
                    .map(x => Array(new Coordinate(x, env.getMinY-epsilon), new Coordinate(x, env.getMaxY+epsilon)))
                    .map(coords => GeometryUtils.geomFactory.createLineString(coords))
                val horizontalBlades: Seq[LineString] = horizontalPoints
                    .map(y => Array(new Coordinate(env.getMinX-epsilon, y), new Coordinate(env.getMaxX+epsilon, y)))
                    .map(coords => GeometryUtils.geomFactory.createLineString(coords))

                val intersectionPoints = (horizontalBlades ++ verticalBlades).flatMap(blade => g.intersection(blade).getCoordinates)
                g.getCoordinates ++ intersectionPoints

            case Right(e) =>
                val verticalIntersectionPoints = verticalPoints.flatMap(x => Seq(new Coordinate(x, e.getMinY), new Coordinate(x, e.getMaxY)))
                val horizontalIntersectionPoints = horizontalPoints.flatMap(y => Seq(new Coordinate(e.getMinX, y), new Coordinate(e.getMaxX, y)))
                val envCoordinates = Seq(
                    new Coordinate(e.getMinX, e.getMinY), new Coordinate(e.getMinX, e.getMaxY),
                    new Coordinate(e.getMaxX, e.getMaxY), new Coordinate(e.getMaxX, e.getMinY)
                )
                verticalIntersectionPoints ++ horizontalIntersectionPoints ++ envCoordinates
        }

        val regionsConditions: Array[(Double, Double) => Boolean] = (
            for (x <- verticalPoints.sliding(2);
                 y <- horizontalPoints.sliding(2)) yield {
                (x1: Double, y1: Double) => x1 >= x.head && x1 <= x.last && y1 >= y.head && y1 <= y.last
            }).toArray

        val envelopes: Array[MBR] = Array.fill(regionsConditions.length)(MBR())
        for (c <- coordinates){
            val regionIndices: Seq[Int] = regionsConditions.zipWithIndex.filter{case (cond, i) => cond(c.x, c.y)}.map(_._2)
            regionIndices.foreach(i => envelopes(i).update(c.x, c.y))
        }

        envelopes.filter(! _.isEmpty).map(e => e.getEnvelope).toList
    }
}
