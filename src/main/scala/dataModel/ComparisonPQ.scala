package dataModel

import org.spark_project.guava.collect.MinMaxPriorityQueue
import scala.collection.JavaConverters._

/**
 * a wrapper of guava min-max PQ.
 *
 * @param maxSize max size of PQ
 * @tparam T the type of input items
 */
case class ComparisonPQ[T](maxSize: Int){

    var minW: Float = 0f
    val ordering: Ordering[(Float, T)] =  Ordering.by[(Float, T), Float](_._1).reverse
    lazy val pq: MinMaxPriorityQueue[(Float, T)] = MinMaxPriorityQueue.orderedBy(ordering).maximumSize(maxSize+1).create()

    /**
     * if w is smaller than minW then omit it.
     * Otherwise, insert it into PQ and if PQ exceed max size,
     * remove item with the smallest weight and update minW
     *
     * @param w the weight of the item
     * @param item item to insert
     */
    def enqueue(w: Float, item: T): Unit ={
        if (minW < w) {
            pq.add((w, item))
            if (pq.size > maxSize)
                minW = pq.pollLast()._1
        }
    }

    def enqueueAll(items: Iterator[(T, Float)]): Unit = items.foreach{ case(item, w) => enqueue(w, item)}

    def take(n: Option[Int]): Iterator[(Float, T)] =
        n match {
            case Some(n) => Iterator.continually{ pq.pollFirst() }.take(n)
            case None =>  Iterator.continually{ pq.pollFirst() }.takeWhile(_ => !pq.isEmpty)
        }

    def take(n: Int): Iterator[(Float, T)] = take(Option(n))

    def dequeueAll: Iterator[(Float, T)] = take(None)

    def clear(): Unit = {
        pq.clear()
        minW = 0f
    }

    def isEmpty: Boolean = pq.isEmpty

    def size(): Int = pq.size()

    def dequeueHead(): (Float, T) = pq.pollFirst()

    def dequeue(): (Float, T) = pq.pollLast()

    def iterator(): Iterator[(Float, T)] = pq.iterator().asScala
}



