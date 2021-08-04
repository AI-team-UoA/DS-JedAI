package model

import model.entities.segmented.FineGrainedEntity
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
            case fge: FineGrainedEntity =>
                fge.segments.flatMap(env =>indexEnvelope(env) )
            case _ =>
                val env = t.getEnvelopeInternal()
                indexEnvelope(env)
        }
    }

    def indexEnvelope(env: Envelope): Seq[(Int, Int)] ={
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
        val indices = for (t <- tiles ; if contains(t)) yield index(t._1)(t._2)
        indices.flatten.distinct.map(i => entities(i))
    }

    def keys: mutable.HashMap[Int, scala.collection.Set[Int]] = index.map(i => i._1 -> i._2.keySet)

    def getIndices: Seq[(Int, Int)] = index.iterator.flatMap { case (i, map) => map.keysIterator.map(j => (i, j)) }.toSeq

    def getValues: Seq[ListBuffer[Int]] = index.values.flatMap(hm => hm.values).toSeq

}

/** Alternative implementation, slower but prettier */

//    def index(t: T): Seq[(Int, Int)] = {
//        val env = t.getEnvelopeInternal()
//        val minX = env.getMinX
//        val maxX = env.getMaxX
//        val minY = env.getMinY
//        val maxY = env.getMaxY
//        if (minX == 0 && maxX == 0 && minY == 0 && maxY == 0) Seq((0, 0))
//        else {
//            val x1 = math.floor(minX / theta.x).toInt
//            val x2 = math.ceil(maxX / theta.x).toInt
//            val y1 = math.floor(minY / theta.y).toInt
//            val y2 = math.ceil(maxY / theta.y).toInt
//            for (x <- x1 until x2; y <- y1 until y2) yield (x, y)
//        }
//    }


//    val index: Map[Int, Map[Int, List[Int]]] = buildIndex()
//
//    def tuplesToMapOfList[A]: (Map[Int, List[A]], (Int, A)) => Map[Int, List[A]] = (acc: Map[Int, List[A]], curr: (Int, A)) => {
//        val key: Int = curr._1
//        val oldList: List[A] = acc.getOrElse(key, List())
//        val newList = curr._2 :: oldList
//        acc.updated(key, newList)
//    }
//
//    def buildIndex(): Map[Int, Map[Int, List[Int]]] ={
//
//        val indexedEntities: Seq[(Int, (Int, Int))] = entities.zipWithIndex.flatMap{ case(se, i) =>
//            val indices = indexEntity(se)
//            indices.map{ case (x, y)=> (x, (y, i)) }
//        }
//
//        val xMap: Map[Int, List[(Int, Int)]] = indexedEntities.foldLeft(Map.empty[Int, List[(Int, Int)]])(tuplesToMapOfList[(Int, Int)])
//
//        val xyMap: Map[Int, Map[Int, List[Int]]] = xMap.mapValues(p => p.foldLeft(Map.empty[Int, List[Int]])(tuplesToMapOfList[Int]))
//        xyMap
//    }
//
//    def getCandidates(se: Entity): Seq[Entity] = indexEntity(se).flatMap(c => get(c)).flatten
//
//    def contains(c: (Int, Int)): Boolean = index.contains(c._1) && index(c._1).contains(c._2)
//
//    def get(c:(Int, Int)): Option[Seq[Entity]] =  if (contains(c)) Some(index(c._1)(c._2).map(i => entities(i))) else None
//
//    def getWithIndex(c:(Int, Int)): Option[Seq[(Int, Entity)]] =  if (contains(c)) Some(index(c._1)(c._2).map(i => (i, entities(i)))) else None
//
//    def keys: Map[Int, Set[Int]] = index.map(i => i._1 -> i._2.keySet)
//
//    def getIndices: Seq[(Int, Int)] = index.iterator.flatMap{ case (i, map) => map.keysIterator.map(j => (i, j))}.toSeq


