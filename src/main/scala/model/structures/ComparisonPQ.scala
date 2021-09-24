package model.structures

import model.weightedPairs.WeightedPairT
import org.spark_project.guava.collect.MinMaxPriorityQueue

import java.util
import scala.collection.JavaConverters._


sealed trait ComparisonPQ {
    val pq: util.AbstractCollection[WeightedPairT]
    val maxSize: Long

    def enqueue(wp: WeightedPairT): Unit ={
        pq.add(wp)
        if (pq.size > maxSize)
            dequeueLast()
    }

    def enqueueAll(items: Iterator[WeightedPairT]): Unit = items.foreach(wp => enqueue(wp))

    def take(n: Int): Iterator[WeightedPairT] = {
        val size = math.max(pq.size(), n)
        Iterator.continually{ dequeueHead() }.take(size)
    }

    def dequeueAll: Iterator[WeightedPairT] = take(pq.size())

    def clear(): Unit = pq.clear()

    def isEmpty: Boolean = pq.isEmpty

    def size(): Int = pq.size()

    def dequeueHead(): WeightedPairT

    def dequeueLast(): WeightedPairT

    def iterator(): Iterator[WeightedPairT] = pq.iterator().asScala

    def dynamicUpdate(wp: WeightedPairT): Unit = {}
}


case class StaticComparisonPQ(maxSize: Long) extends ComparisonPQ{

    val pq: MinMaxPriorityQueue[WeightedPairT] = MinMaxPriorityQueue.maximumSize(maxSize.toInt+1).create()

    def dequeueHead(): WeightedPairT = pq.pollFirst()

    def dequeueLast(): WeightedPairT = pq.pollLast()

}

case class DynamicComparisonPQ(maxSize: Long) extends ComparisonPQ{

    val pq: util.TreeSet[WeightedPairT] = new util.TreeSet[WeightedPairT]()

    def dequeueHead(): WeightedPairT = pq.pollFirst()

    def dequeueLast(): WeightedPairT = pq.pollLast()

    override def dynamicUpdate(wp: WeightedPairT): Unit ={
        val exists = pq.remove(wp)
        if (exists){
            wp.incrementRelatedMatches()
            enqueue(wp)
        }
    }
}





