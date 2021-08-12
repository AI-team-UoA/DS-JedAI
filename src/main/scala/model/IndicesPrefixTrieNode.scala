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
case class IndicesPrefixTrieNode(referenceIndex: Int, entities: ListBuffer[ListBuffer[Entity]], children: Array[Option[IndicesPrefixTrieNode]]) {
    val MAX_SIZE = 512

    def insert(e: Entity): Unit = {
        if (entities.isEmpty) entities.append(new ListBuffer[Entity]())
        val lastEntities = entities.last
        if (lastEntities.length < MAX_SIZE)
            lastEntities.append(e)
        else{
            val newEntities = new ListBuffer[Entity]()
            newEntities.append(e)
            entities.append(newEntities)
        }
    }

    def insert(indices: List[Int], e: Entity, size: Int): Unit = indices match {
        case head :: tail if head == referenceIndex => insert(tail, e, size)
        case head :: tail =>
            val currentIndex = head - (referenceIndex+1)
            children(currentIndex) match {
                case Some(trie) =>
                    trie.insert(tail, e, size)
                case None =>
                    val trie = IndicesPrefixTrieNode(head, new ListBuffer[ListBuffer[Entity]](), Array.fill(size-head)(None))
                    trie.insert(tail, e, size)
                    children(currentIndex) = Some(trie)
            }
        case Nil => insert(e)
    }


    def getFlattenNodes(parentIndices: List[Int], accumulated: List[(List[Int], List[Entity])]):
    List[(List[Int], List[Entity])] ={

        @tailrec
        def flattenNode(nodes: List[Option[IndicesPrefixTrieNode]], parentIndices: List[Int],
                        accumulated: List[(List[Int], List[Entity])]): List[(List[Int], List[Entity])] ={

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

        val newIndices: List[Int] = referenceIndex :: parentIndices
        val nodeResults =   if (entities.nonEmpty) entities.map(e => (newIndices, e.toList)).toList ::: accumulated
                            else accumulated
        flattenNode(children.toList, newIndices, nodeResults)
    }

}


case class IndicesPrefixTrie(head: IndicesPrefixTrieNode, mainEntity: DecomposedEntity){
    val AVG_WEIGHT: Int = 20000
    val BUFFER: Int = 5000
    val size: Int = mainEntity.segments.length
    val segments: IndexedSeq[Geometry] = mainEntity.segments

    def insert(indices: List[Int], e: Entity): Unit = head.insert(indices, e, size)
    def getFlattenNodes: List[(List[Int], List[Entity])] = head.getFlattenNodes(Nil, Nil)

    def getNodeCost(node: IndicesPrefixTrieNode, extraWeight: Long = 0L): Long =
        (segments(node.referenceIndex).getNumPoints * node.entities.length) + extraWeight

    def balanceTrie(): Unit ={

        def compressNode(node: IndicesPrefixTrieNode, cost: Long): Unit ={
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



        def balance(node: IndicesPrefixTrieNode): Unit ={
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

    def apply(mainEntity: DecomposedEntity, entities: Seq[Entity]): IndicesPrefixTrie ={

        // for each entity, find the indices of the segments it intersects
        val intersectingTargetSegments: Seq[(Entity, Seq[Int])] = entities.map(se => (se, mainEntity.findIntersectingSegmentsIndices(se).map(_._1)))
        val size = mainEntity.segments.size

        val head = IndicesPrefixTrieNode(0, new ListBuffer[ListBuffer[Entity]](), Array.fill(size-1)(None))
        intersectingTargetSegments
            .filter(_._2.nonEmpty)
            .foreach{ case (se, segmentsIndices) => head.insert(segmentsIndices.sorted.toList, se, size) }

        IndicesPrefixTrie(head, mainEntity)
    }
}
