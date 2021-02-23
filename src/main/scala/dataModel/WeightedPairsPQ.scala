package dataModel

import org.spark_project.guava.collect.MinMaxPriorityQueue
import scala.collection.JavaConverters._

case class WeightedPairsPQ(maxSize: Int){

    lazy val pq: MinMaxPriorityQueue[WeightedPair] = MinMaxPriorityQueue.maximumSize(maxSize+1).create()


    def enqueue(wp: WeightedPair): Unit ={
            pq.add(wp)
            if (pq.size > maxSize)
                pq.pollLast()
    }

    def enqueueAll(items: Iterator[WeightedPair]): Unit = items.foreach(wp => enqueue(wp))

    def take(n: Option[Int]): Iterator[WeightedPair] =
        n match {
            case Some(n) => Iterator.continually{ pq.pollFirst() }.take(n)
            case None =>  Iterator.continually{ pq.pollFirst() }.takeWhile(_ => !pq.isEmpty)
        }

    def dynamicUpdate(wp: WeightedPair): Unit ={
        val exists = pq.remove(wp)
        if (exists){
            wp.incrementRelatedMatches()
            enqueue(wp)
        }
    }

    def take(n: Int): Iterator[WeightedPair] = take(Option(n))

    def dequeueAll: Iterator[WeightedPair] = take(None)

    def clear(): Unit = pq.clear()

    def isEmpty: Boolean = pq.isEmpty

    def size(): Int = pq.size()

    def dequeueHead(): WeightedPair = pq.pollFirst()

    def dequeue(): WeightedPair = pq.pollLast()

    def iterator(): Iterator[WeightedPair] = pq.iterator().asScala
}



