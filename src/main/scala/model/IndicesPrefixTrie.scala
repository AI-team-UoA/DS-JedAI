package model

import model.entities.Entity

import scala.collection.mutable.ListBuffer

/**
 *  path of indices that end up to list of Source entities
 *
 *      1 -- | Seq (S1, S2, S3, ..)
 *           | 2 -------------------> |Seq (S3, ..)
 *      2 --| Seq (S4, S5,..)
 *          | 4 --------------------- |Seq (S8, ..)
 *                                    |5--------------------| Seq(S7, S6, ...)
 */
case class IndicesPrefixTrie(entities: ListBuffer[Entity], children: Array[Option[IndicesPrefixTrie]], size: Int) {

    def insert(e: Entity): Unit = entities.append(e)

    def insert(indices: List[Int], e: Entity): Unit = indices match {
        case head :: Nil =>
            children(head) match {
                case Some(trie) =>
                    trie.insert(e)
                case None =>
                    val trie = new IndicesPrefixTrie(new ListBuffer[Entity](), Array.fill(size)(None), size)
                    trie.insert(e)
                    children(head) = Some(trie)
            }
        case head :: tail =>
            children(head) match {
                case Some(trie) =>
                    trie.insert(tail, e)
                case None =>
                    val trie = new IndicesPrefixTrie(new ListBuffer[Entity](), Array.fill(size)(None), size)
                    trie.insert(tail, e)
                    children(head) = Some(trie)
            }
        case _ =>
    }

    def getFlattenNodes: List[(List[Int], List[Entity])] ={

        def flattenNode(nodes: List[(Option[IndicesPrefixTrie], Int)], parentIndices: List[Int],
                        accumulated: List[(List[Int], List[Entity])]): List[(List[Int], List[Entity])] ={
            nodes match {
                case head :: tail =>
                    head._1 match {
                        case Some(trie) =>
                            val newIndices = head._2 :: parentIndices
                            val nodeResults = if (trie.entities.nonEmpty) (newIndices, trie.entities.toList) :: accumulated else accumulated
                            val subTrieResults = flattenNode(trie.children.toList.zipWithIndex, newIndices, nodeResults)
                            flattenNode(tail, parentIndices, subTrieResults)
                        case None =>
                            flattenNode(tail, parentIndices, accumulated)
                    }
                case Nil => accumulated
            }
        }
        flattenNode(children.zipWithIndex.toList, Nil, Nil)
    }
}


object IndicesPrefixTrie {
    def apply(size: Int): IndicesPrefixTrie = IndicesPrefixTrie(new ListBuffer[Entity](), Array.fill(size)(None), size)

    def apply(size: Int, intersectingSegmentIndices: Seq[(Entity, Seq[Int])]): IndicesPrefixTrie ={
        val trie = IndicesPrefixTrie(size)
        intersectingSegmentIndices
            .filter(_._2.nonEmpty)
            .foreach{ case (se, segmentsIndices) => trie.insert(segmentsIndices.sorted.toList, se) }
        trie
    }
}
