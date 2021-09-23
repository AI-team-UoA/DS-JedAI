package model.weightedPairs

import model.TileGranularities
import model.entities.EntityT
import org.apache.commons.math3.stat.inference.ChiSquareTest
import utils.configuration.Constants.WeightingFunction.{MBRO, WeightingFunction}
import utils.configuration.Constants.{COMPOSITE, HYBRID, SIMPLE, THIN_MULTI_COMPOSITE, WeightingFunction, WeightingScheme}

import scala.math.{ceil, floor, max, min}


case class WeightedPairFactory(mainWF: WeightingFunction, secondaryWF: Option[WeightingFunction],
                               weightingScheme: WeightingScheme, tileGranularities:TileGranularities, totalBlocks: Double) {

    /**
     * compute the main weight of a pair of entities
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getMainWeight(s: EntityT, t: EntityT): Float = getWeight(s, t, mainWF)


    /**
     * compute the secondary weight of a pair of entities, if the secondary scheme was provided
     * @param s source entity
     * @param t target entity
     * @return a weight
     */
    def getSecondaryWeight(s: EntityT, t: EntityT): Float = secondaryWF match {
        case Some(wf) => getWeight(s, t, wf)
        case None => 0f
    }

    def createWeightedPair(counter: Int, s: EntityT, sIndex: Int, t:EntityT, tIndex: Int): WeightedPairT = {
        weightingScheme match {
            case SIMPLE =>
                val mw = getMainWeight(s, t)
                SimpleWP(counter, sIndex, tIndex, mw)
            case COMPOSITE =>
                val mw = getMainWeight(s, t)
                val sw = getSecondaryWeight(s, t)
                CompositeWP(counter, sIndex, tIndex, mw, sw)
            case HYBRID =>
                val mw = getMainWeight(s, t)
                val sw = getSecondaryWeight(s, t)
                HybridWP(counter, sIndex, tIndex, mw, sw)

            case THIN_MULTI_COMPOSITE =>
                val mw = jaccardSimilarity(s, t)
                val sw = coOccurenceFrequency(s, t)
                val lw = minimumBoundingRectangleOverlap(s, t)
                ThinMultiCompositePair(counter, sIndex, tIndex, mw, sw, lw)
        }
    }

    /**
     * Weight a pair
     * @param s Spatial entity
     * @param t Spatial entity
     * @return weight
     */
    def getWeight(s: EntityT, t: EntityT, wf: WeightingFunction): Float = {
        wf match {
            case WeightingFunction.MBRO => minimumBoundingRectangleOverlap(s, t)
            case WeightingFunction.ISP => inversePointSum(s, t)
            case WeightingFunction.JS => jaccardSimilarity(s, t)
            case WeightingFunction.PEARSON_X2 => pearsonsX2(s, t)
            case WeightingFunction.CF | _ => coOccurenceFrequency(s, t)
        }
    }

    def getNumOfBlocks(e: EntityT): Int =
        (ceil(e.getMaxX/tileGranularities.x).toInt - floor(e.getMinX/tileGranularities.x).toInt + 1) * (ceil(e.getMaxY/tileGranularities.y).toInt - floor(e.getMinY/tileGranularities.y).toInt + 1)

    def getCommonBlocks(s: EntityT, t: EntityT): Int =
        (min(ceil(s.getMaxX/tileGranularities.x), ceil(t.getMaxX/tileGranularities.x)).toInt - max(floor(s.getMinX/tileGranularities.x),floor(t.getMinX/tileGranularities.x)).toInt + 1) *
            (min(ceil(s.getMaxY/tileGranularities.y), ceil(t.getMaxY/tileGranularities.y)).toInt - max(floor(s.getMinY/tileGranularities.y), floor(t.getMinY/tileGranularities.y)).toInt + 1)

    def coOccurenceFrequency(s: EntityT, t: EntityT): Float = getCommonBlocks(s, t).toFloat

    def jaccardSimilarity(s: EntityT, t: EntityT): Float ={
        val sBlocks = getNumOfBlocks(s)
        val tBlocks = getNumOfBlocks(t)
        val cb = getCommonBlocks(s, t)
        cb / (sBlocks + tBlocks - cb)
    }

    def pearsonsX2(s: EntityT, t: EntityT): Float ={
        val sBlocks = getNumOfBlocks(s)
        val tBlocks = getNumOfBlocks(t)
        val cb = getCommonBlocks(s, t)
        val v1: Array[Long] = Array[Long](cb, (tBlocks - cb).toLong)
        val v2: Array[Long] = Array[Long]((sBlocks - cb).toLong, (totalBlocks - (v1(0) + v1(1) + (sBlocks - cb))).toLong)
        val chiTest = new ChiSquareTest()
        chiTest.chiSquare(Array(v1, v2)).toFloat
    }

    def minimumBoundingRectangleOverlap(s: EntityT, t: EntityT): Float ={
        val intersectionArea = s.getIntersectingInterior(t).getArea
        val w = intersectionArea / (s.approximation.getArea + t.approximation.getArea - intersectionArea)
        if (!w.isNaN) w.toFloat else 0f
    }

    def inversePointSum(s: EntityT, t: EntityT): Float = 1f / (s.geometry.getNumPoints + t.geometry.getNumPoints)
}