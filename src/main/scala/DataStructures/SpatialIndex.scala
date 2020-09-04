package DataStructures

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.Set

class SpatialIndex(){

    val index: mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[Int]]] = new mutable.HashMap()

    def insert(c: (Int, Int), i: Int): Unit = {
        if (index.contains(c._1))
            if (index(c._1).contains(c._2))
                index(c._1)(c._2).append(i)
            else
                index(c._1).put(c._2, ListBuffer(i))
        else {
            val l = ListBuffer[Int](i)
            val h = mutable.HashMap[Int, ListBuffer[Int]]()
            h.put(c._2, l)
            index.put(c._1, h)
        }
    }

    def contains(c: (Int, Int)): Boolean = index.contains(c._1) && index(c._1).contains(c._2)

    def get(c:(Int, Int)): ListBuffer[Int] = index(c._1)(c._2)

    def asKeys: mutable.HashMap[Int, Set[Int]] =
       index.map(i => i._1 -> i._2.keySet)

    def getIndices: ArrayBuffer[(Int, Int)] ={
        val indices = ArrayBuffer[(Int, Int)]()
        index.foreach{ case(i1, indicesSet) => indicesSet.keysIterator.foreach(i2 => indices.append((i1, i2)))}
        indices
    }
}