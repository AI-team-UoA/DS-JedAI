package model.structures

import model.TileGranularities
import model.entities.EntityT
import org.locationtech.jts.geom.Envelope

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

case class SpatialIndex[T <: {def getEnvelopeInternal(): Envelope}](entities: Array[T], theta: TileGranularities) {

    val divisionPrecision: Int = 6
    var index: mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[Int]]] = new mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[Int]]]()

    entities.zipWithIndex.foreach { case (e, i) =>
        val indices = index(e)
        indices.foreach(c => insert(c, i))
    }

    lazy val indices: Set[(Int, Int)] = getIndices.toSet

    def index(t: T): Seq[(Int, Int)] = {
        t match {
            case t: EntityT =>
                t.getOverlappingTiles(theta)
            case _ =>
                val env = t.getEnvelopeInternal()
                indexEnvelope(env)
        }
    }

    def indexEnvelope(env: Envelope): Seq[(Int, Int)] = {
        val minX = env.getMinX
        val maxX = env.getMaxX
        val minY = env.getMinY
        val maxY = env.getMaxY
        if (minX == 0 && maxX == 0 && minY == 0 && maxY == 0) Seq((0, 0))
        else {
            val x1 = math.floor(BigDecimal(minX / theta.x).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
            val x2 = math.ceil(BigDecimal(maxX / theta.x).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
            val y1 = math.floor(BigDecimal(minY / theta.y).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt
            val y2 = math.ceil(BigDecimal(maxY / theta.y).setScale(divisionPrecision, RoundingMode.HALF_EVEN).toDouble).toInt

            for (x <- x1 until x2; y <- y1 until y2) yield (x, y)
        }
    }


    private def insert(c: (Int, Int), i: Int): Unit = {
        val (x, y) = c
        if (index.contains(x))
            if (index(x).contains(y))
                index(x)(y).append(i)
            else
                index(x).put(y, ListBuffer[Int](i))
        else {
            val l = ListBuffer[Int](i)
            val h = mutable.HashMap[Int, ListBuffer[Int]]()
            h.put(y, l)
            index.put(x, h)
        }
    }

    def getCandidates(t: T): Seq[T] = index(t).flatMap(c => get(c))

    def contains(c: (Int, Int)): Boolean = index.contains(c._1) && index(c._1).contains(c._2)

    def get(c: (Int, Int)): Seq[T] = if (contains(c)) index(c._1)(c._2).map(i => entities(i)) else Seq()

    def getIndices(c: (Int, Int)): Seq[Int] = if (contains(c)) index(c._1)(c._2) else Seq()

    def getWithIndex(c: (Int, Int)): Seq[(Int, T)] = if (contains(c)) index(c._1)(c._2).map(i => (i, entities(i))) else Seq()

    def getDistinctFromTiles(tiles: Seq[(Int, Int)]): Seq[T] = {
        val indices = for (t <- tiles; if contains(t)) yield index(t._1)(t._2)
        indices.flatten.distinct.map(i => entities(i))
    }

    def keys: mutable.HashMap[Int, scala.collection.Set[Int]] = index.map(i => i._1 -> i._2.keySet)

    def getIndices: Seq[(Int, Int)] = index.iterator.flatMap { case (i, map) => map.keysIterator.map(j => (i, j)) }.toSeq

    def getValues: Seq[ListBuffer[Int]] = index.values.flatMap(hm => hm.values).toSeq

}
