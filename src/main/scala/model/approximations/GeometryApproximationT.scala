package model.approximations

import model.TileGranularities
import org.locationtech.jts.geom.Envelope
import utils.configuration.Constants.Relation
import utils.configuration.Constants.Relation.Relation
import utils.geometryUtils.{EnvelopeOp, GeometryUtils}

import scala.math.BigDecimal.RoundingMode
import scala.math.{ceil, floor, max, min}

trait GeometryApproximationT {
    val epsilon: Double = 1e-8
    val divisionPrecision: Int = 6

    val env: Envelope

    def getEnvelopeInternal(): Envelope = env
    def getEnvelopes: List[Envelope] = List(env)
    def getMinX: Double = env.getMinX
    def getMaxX: Double = env.getMaxX
    def getMinY: Double = env.getMinY
    def getMaxY: Double = env.getMaxY

    def getArea: Double = env.getArea


    /**
     *  checks if the geometry approximations relate
     * @param approximation target geometry approximation
     * @return true if the approximations relate
     */
    def approximateIntersection(approximation: GeometryApproximationT): Boolean = approximation.getEnvelopes.exists(appr => checkRelation(env, appr, Relation.INTERSECTS))

    def getReferencePoint(targetApproximation: GeometryApproximationT, theta: TileGranularities): (Double, Double) ={
        val targetEnv = targetApproximation.env
        val minX1 = env.getMinX /theta.x
        val minX2 = targetEnv.getMinX /theta.x
        val maxY1 = env.getMaxY /theta.y
        val maxY2 = targetEnv.getMaxY /theta.y
        val rfX: Double = max(minX1, minX2) + epsilon
        val rfY: Double = min(maxY1, maxY2) + epsilon
        (rfX, rfY)
    }

    def getIntersectingInterior(e: GeometryApproximationT): Envelope = EnvelopeOp.getIntersectingInterior(getEnvelopeInternal(), e.getEnvelopeInternal())

    def getOverlappingTiles(theta: TileGranularities): Seq[(Int, Int)] = {
        if (env.getHeight == 0 && env.getWidth == 0) Seq((0, 0))
        else {
            val (x1, x2, y1, y2) = adjustEnvelope(env, theta)
            for (x <- x1 until x2; y <- y1 until y2) yield (x, y)
        }
    }

    def getNumOfOverlappingTiles(theta: TileGranularities):Int =
        (ceil(getMaxX/theta.x).toInt - floor(getMinX/theta.x).toInt + 1) * (ceil(getMaxY/theta.y).toInt - floor(getMinY/theta.y).toInt + 1)

    def getNumOfCommonTiles(apx: GeometryApproximationT, theta: TileGranularities):Int =
            (min(ceil(getMaxX/theta.x), ceil(apx.getMaxX/theta.x)).toInt - max(floor(getMinX/theta.x),floor(apx.getMinX/theta.x)).toInt + 1) *
                (min(ceil(getMaxY/theta.y), ceil(apx.getMaxY/theta.y)).toInt - max(floor(getMinY/theta.y), floor(apx.getMinY/theta.y)).toInt + 1)

    def adjustEnvelope(env: Envelope, theta: TileGranularities): (Int, Int, Int, Int) ={
        val minX = env.getMinX
        val maxX = env.getMaxX
        val minY = env.getMinY
        val maxY = env.getMaxY
        val x1 = math.floor(BigDecimal(minX / theta.x).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
        val x2 = math.ceil(BigDecimal(maxX / theta.x).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
        val y1 = math.floor(BigDecimal(minY / theta.y).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
        val y2 = math.ceil(BigDecimal(maxY / theta.y).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
        (x1, x2, y1, y2)
    }

    def checkRelation(env1 : Envelope, env2: Envelope, relation: Relation): Boolean = {
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

    override def toString: String = s"MBR(${GeometryUtils.geomFactory.toGeometry(env)})"
}
