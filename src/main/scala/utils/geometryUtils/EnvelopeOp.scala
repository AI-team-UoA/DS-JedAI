package utils.geometryUtils

import model.TileGranularities
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import utils.geometryUtils.EnvelopeOp.EnvelopeIntersectionTypes.EnvelopeIntersectionTypes


object EnvelopeOp {

    val epsilon: Double = 1e-8
    val geometryFactory = new GeometryFactory()
    val SPLIT_LOG_BASE: Int = 50

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
}
