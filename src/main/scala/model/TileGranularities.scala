package model

import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Envelope
import utils.Constants.ThetaOption
import utils.Constants.ThetaOption.ThetaOption

case class TileGranularities(x: Double, y: Double){
    def *(n: Double) : TileGranularities = TileGranularities(x*n, y*n)
}

object TileGranularities {

    def apply(source: RDD[Envelope], thetaOption: ThetaOption = ThetaOption.AVG): TileGranularities ={

        val sourceCount = source.count()
        apply(source, sourceCount, thetaOption)
    }

    def apply(source: RDD[Envelope], count: Long, thetaOption: ThetaOption): TileGranularities ={
        val (tx, ty) = thetaOption match {
            case ThetaOption.MIN =>
                // need filtering because there are cases where the geometries are perpendicular to the axes
                // hence its width or height is equal to 0.0
                val thetaX = source.map(env => env.getMaxX - env.getMinX).filter(_ != 0.0d).min
                val thetaY = source.map(env => env.getMaxY - env.getMinY).filter(_ != 0.0d).min
                (thetaX, thetaY)
            case ThetaOption.MAX =>
                val thetaX = source.map(env => env.getMaxX - env.getMinX).max
                val thetaY = source.map(env => env.getMaxY - env.getMinY).max
                (thetaX, thetaY)
            case ThetaOption.AVG =>
                val thetaX = source.map(env => env.getMaxX - env.getMinX).sum() / count
                val thetaY = source.map(env => env.getMaxY - env.getMinY).sum() / count
                (thetaX, thetaY)
            case ThetaOption.AVG_x2 =>
                val thetaXs = source.map(env => env.getMaxX - env.getMinX).sum() / count
                val thetaYs = source.map(env => env.getMaxY - env.getMinY).sum() / count
                val thetaX = 0.5 * thetaXs
                val thetaY = 0.5 * thetaYs
                (thetaX, thetaY)
            case _ =>
                (1d, 1d)
        }
        TileGranularities(tx, ty)
    }



    def apply(source: Seq[Envelope], count: Long, thetaOption: ThetaOption): TileGranularities ={
        val (tx, ty) = thetaOption match {
            case ThetaOption.MIN =>
                // need filtering because there are cases where the geometries are perpendicular to the axes
                // hence its width or height is equal to 0.0
                val thetaX = source.map(env => env.getMaxX - env.getMinX).filter(_ != 0.0d).min
                val thetaY = source.map(env => env.getMaxY - env.getMinY).filter(_ != 0.0d).min
                (thetaX, thetaY)
            case ThetaOption.MAX =>
                val thetaX = source.map(env => env.getMaxX - env.getMinX).max
                val thetaY = source.map(env => env.getMaxY - env.getMinY).max
                (thetaX, thetaY)
            case ThetaOption.AVG =>
                val thetaX = source.map(env => env.getMaxX - env.getMinX).sum / count
                val thetaY = source.map(env => env.getMaxY - env.getMinY).sum / count
                (thetaX, thetaY)
            case ThetaOption.AVG_x2 =>
                val thetaXs = source.map(env => env.getMaxX - env.getMinX).sum / count
                val thetaYs = source.map(env => env.getMaxY - env.getMinY).sum / count
                val thetaX = 0.5 * thetaXs
                val thetaY = 0.5 * thetaYs
                (thetaX, thetaY)
            case _ =>
                (1d, 1d)
        }
        TileGranularities(tx, ty)
    }
}
