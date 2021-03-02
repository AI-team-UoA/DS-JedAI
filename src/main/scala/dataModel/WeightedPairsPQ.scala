package dataModel

import java.util
import scala.collection.JavaConverters._

case class WeightedPairsPQ(maxSize: Int){

    val pq: util.TreeSet[WeightedPair] = new util.TreeSet[WeightedPair]()

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

    def dequeueLast(): WeightedPair = pq.pollLast()

    def iterator(): Iterator[WeightedPair] = pq.iterator().asScala
}



