package model

import java.util
import org.spark_project.guava.collect.MinMaxPriorityQueue
import scala.collection.JavaConverters._


sealed trait ComparisonPQ {
    val pq: util.AbstractCollection[WeightedPair]
    val maxSize: Long

    def enqueue(wp: WeightedPair): Unit ={
        pq.add(wp)
        if (pq.size > maxSize)
            dequeueLast()
    }

    def enqueueAll(items: Iterator[WeightedPair]): Unit = items.foreach(wp => enqueue(wp))

    def take(n: Option[Int]): Iterator[WeightedPair] =
        n match {
            case Some(n) => Iterator.continually{ dequeueHead() }.take(n)
            case None =>  Iterator.continually{ dequeueHead() }.takeWhile(_ => !pq.isEmpty)
        }

    def take(n: Int): Iterator[WeightedPair] = take(Option(n))

    def dequeueAll: Iterator[WeightedPair] = take(None)

    def clear(): Unit = pq.clear()

    def isEmpty: Boolean = pq.isEmpty

    def size(): Int = pq.size()

    def dequeueHead(): WeightedPair

    def dequeueLast(): WeightedPair

    def iterator(): Iterator[WeightedPair] = pq.iterator().asScala

}


case class StaticComparisonPQ(maxSize: Long) extends ComparisonPQ{

    val pq: MinMaxPriorityQueue[WeightedPair] = MinMaxPriorityQueue.maximumSize(maxSize.toInt+1).create()

    def dequeueHead(): WeightedPair = pq.pollFirst()

    def dequeueLast(): WeightedPair = pq.pollLast()

}

case class DynamicComparisonPQ(maxSize: Long) extends ComparisonPQ{

    val pq: util.TreeSet[WeightedPair] = new util.TreeSet[WeightedPair]()

    def dequeueHead(): WeightedPair = pq.pollFirst()

    def dequeueLast(): WeightedPair = pq.pollLast()

    def dynamicUpdate(wp: WeightedPair): Unit ={
        val exists = pq.remove(wp)
        if (exists){
            wp.incrementRelatedMatches()
            enqueue(wp)
        }
    }
}





