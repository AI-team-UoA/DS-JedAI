package model

import cats.data.NonEmptyList
import model.entities.Entity
import model.entities.segmented.DecomposedEntity
import org.locationtech.jts.geom.Geometry

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import cats.implicits._



/**
 *  path of indices that end up to list of Source entities
 *
 *      1 -- | Seq (S1, S2, S3, ..)
 *           | 2 -------------------> |Seq (S3, ..)
 *      2 --| Seq (S4, S5,..)
 *          | 4 --------------------- |Seq (S8, ..)
 *                                    |5--------------------| Seq(S7, S6, ...)
 */

/**
 *
 * @param referenceIndex    points to the corresponding segment
 * @param entities entities assigned to this node
 * @param children children trie nodes
 */
case class IndicesPrefixTrieNode[T](referenceIndex: Int, entities: ListBuffer[ListBuffer[T]], children: Array[Option[IndicesPrefixTrieNode[T]]]) {
    val MAX_SIZE = 256

    def insert(e: T): Unit = {
        if (entities.isEmpty) entities.append(new ListBuffer[T]())
        val lastEntities = entities.last
        if (lastEntities.length < MAX_SIZE)
            lastEntities.append(e)
        else{
            val newEntities = new ListBuffer[T]()
            newEntities.append(e)
            entities.append(newEntities)
        }
    }

    def insert(indices: List[Int], e: T, size: Int): Unit = indices match {
        case head :: tail if head == referenceIndex => insert(tail, e, size)
        case head :: tail =>
            val currentIndex = head - (referenceIndex+1)
            children(currentIndex) match {
                case Some(trie) =>
                    trie.insert(tail, e, size)
                case None =>
                    val trie = IndicesPrefixTrieNode(head, new ListBuffer[ListBuffer[T]](), Array.fill[Option[IndicesPrefixTrieNode[T]]](size-(head+1))(None))
                    trie.insert(tail, e, size)
                    children(currentIndex) = Some(trie)
            }
        case Nil => insert(e)
    }


    def getFlattenNodes(parentIndices: List[Int], accumulated: List[(List[Int], List[T])]):
    List[(List[Int], List[T])] ={

        @tailrec
        def flattenNode(nodes: List[Option[IndicesPrefixTrieNode[T]]], parentIndices: List[Int],
                        accumulated: List[(List[Int], List[T])]): List[(List[Int], List[T])] ={

            nodes match {
                case head :: tail =>
                    head match {
                        case Some(trie) =>
                            val subTrieResults = trie.getFlattenNodes(parentIndices, accumulated)
                            flattenNode(tail, parentIndices, subTrieResults)
                        case None =>
                            flattenNode(tail, parentIndices, accumulated)
                    }
                case Nil => accumulated
            }
        }

        val newIndices: List[Int] = if (referenceIndex > -1) referenceIndex :: parentIndices else parentIndices
        val nodeResults = if (entities.nonEmpty) entities.map(e => (newIndices, e.toList)).toList ::: accumulated
                          else accumulated
        flattenNode(children.toList, newIndices, nodeResults)
    }
}


case class IndicesPrefixTrie[T](head: IndicesPrefixTrieNode[T], segments: IndexedSeq[Geometry]){
    val AVG_WEIGHT: Int = 20000
    val BUFFER: Int = 5000
    val size: Int = segments.size

    def insert(indices: List[Int], e: T): Unit = head.insert(indices, e, size)
    def getFlattenNodes: List[(List[Int], List[T])] = head.getFlattenNodes(Nil, Nil)

    def getNodeCost(node: IndicesPrefixTrieNode[T], extraWeight: Long = 0L): Long = {
        if (node.referenceIndex >= 0) (segments(node.referenceIndex).getNumPoints * node.entities.length) + extraWeight
        else 0L
    }

    def balanceTrie(): Unit ={

        def compressNode(node: IndicesPrefixTrieNode[T], cost: Long): Unit ={
            val someChildren =  NonEmptyList.fromList(node.children.toList.flatten)
            someChildren match {
                case Some(children) =>
                    val (nodeCost, minNode) = children.map(n => (getNodeCost(n) + cost, n)).toList.minBy(_._1)
                    if (nodeCost < AVG_WEIGHT + BUFFER){
                        minNode.entities.appendAll(node.entities)
                        node.entities.clear()
                    }
                case None =>
            }
        }


        def balance(node: IndicesPrefixTrieNode[T]): Unit ={
            val cost = getNodeCost(node)
            if (cost < AVG_WEIGHT - BUFFER)
                compressNode(node, cost)
            else
                node.children.flatten.foreach(ch => balance(ch))
        }

        balance(head)
    }
}


object IndicesPrefixTrie {

    def apply(mainEntity: DecomposedEntity, entities: Seq[Entity]): IndicesPrefixTrie[Entity] ={

        // for each entity, find the indices of the segments it intersects
        val intersectingTargetSegments: Seq[(Entity, Seq[Int])] = entities.map(se => (se, mainEntity.findIntersectingSegmentsIndices(se).map(_._1)))
        val size = mainEntity.segments.size

        val head = IndicesPrefixTrieNode[Entity](referenceIndex = -1, new ListBuffer[ListBuffer[Entity]](), Array.fill(size)(None))
        intersectingTargetSegments.filter(_._2.nonEmpty)
            .foreach{ case (se, segmentsIndices) => head.insert(segmentsIndices.sorted.toList, se, size) }
        IndicesPrefixTrie[Entity](head, mainEntity.segments)
    }


    def apply(entities: Seq[(Int, List[Int])], segments: IndexedSeq[Geometry]): IndicesPrefixTrie[Int] ={
        val size = segments.size
        val head = IndicesPrefixTrieNode[Int](referenceIndex = -1, new ListBuffer[ListBuffer[Int]](), Array.fill(size)(None))
        entities.foreach{ case (id, indices) => head.insert(indices.sorted, id, size) }
        IndicesPrefixTrie[Int](head, segments)
    }
}
