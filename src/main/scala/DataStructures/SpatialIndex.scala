package DataStructures

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.Set

class SpatialIndex(){

    val index: mutable.HashMap[Int, mutable.HashMap[Int, ArrayBuffer[Int]]] = new mutable.HashMap()

    def insert(c: (Int, Int), i: Int): Unit = {
        if (index.contains(c._1))
            if (index(c._1).contains(c._2))
                index(c._1)(c._2) += i
            else
                index(c._1).put(c._2, ArrayBuffer(i))
        else {
            val l = ArrayBuffer[Int](i)
            val h = mutable.HashMap[Int, ArrayBuffer[Int]]()
            h.put(c._2, l)
            index.put(c._1, h)
        }
    }

    def contains(c: (Int, Int)): Boolean = index.contains(c._1) && index(c._1).contains(c._2)

    def get(c:(Int, Int)): ArrayBuffer[Int] = index(c._1)(c._2)

    def asKeys: mutable.HashMap[Int, Set[Int]] =
       index.map(i => i._1 -> i._2.keySet)

}