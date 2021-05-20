package model

import model.entities.Entity

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class SpatialIndex(entities: Array[Entity], tileGranularities: TileGranularities){

    var index: mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[Int]]] = new mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[Int]]]()
    entities.zipWithIndex.foreach(e => indexEntity(e._1).foreach(c => insert(c, e._2)))

    def indexEntity(se: Entity): Seq[(Int, Int)] = {

        if (se.getMinX == 0 && se.getMaxX == 0 && se.getMinY == 0 && se.getMaxY == 0) Seq((0, 0))
        else{
            val maxX = math.ceil(se.getMaxX / tileGranularities.x).toInt
            val minX = math.floor(se.getMinX / tileGranularities.x).toInt
            val maxY = math.ceil(se.getMaxY / tileGranularities.y).toInt
            val minY = math.floor(se.getMinY / tileGranularities.y).toInt
            //TODO check here that until is correct
            for (x <- minX until maxX; y <- minY until maxY) yield (x, y)
        }
    }


    def insert(c: (Int, Int), i: Int): Unit = {
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

    def getCandidates(se: Entity): Seq[Entity] = indexEntity(se).flatMap(c => get(c))

    def contains(c: (Int, Int)): Boolean = index.contains(c._1) && index(c._1).contains(c._2)

    def get(c:(Int, Int)): Seq[Entity] =  if (contains(c)) index(c._1)(c._2).map(i => entities(i)) else Seq()

    def getWithIndex(c:(Int, Int)): Seq[(Int, Entity)] =  if (contains(c)) index(c._1)(c._2).map(i => (i, entities(i))) else Seq()

    def keys: mutable.HashMap[Int, scala.collection.Set[Int]] = index.map(i => i._1 -> i._2.keySet)

    def getIndices: Seq[(Int, Int)] = index.iterator.flatMap{ case (i, map) => map.keysIterator.map(j => (i, j))}.toSeq

    def getValues: Seq[ListBuffer[Int]] = index.values.flatMap(hm => hm.values).toSeq



/** Alternative implementation, slower but prettier */

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


}