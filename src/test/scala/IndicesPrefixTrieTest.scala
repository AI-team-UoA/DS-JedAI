import model.structures.{IndicesPrefixTrie, IndicesPrefixTrieNode}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import utils.geometryUtils.GeometryUtils

class IndicesPrefixTrieTest extends AnyWordSpec with Matchers  {
    val geomFactory: GeometryFactory = GeometryUtils.geomFactory
    val emptyGeometriesGenerator: Int => Seq[Geometry] = (x: Int) => Range(0, x).map(i => geomFactory.createEmpty(2))
    val emptyGeom10: IndexedSeq[Geometry] = emptyGeometriesGenerator(10).toIndexedSeq


    "IndicesPrefixTrie" should {

        "generate the following verifications" in {
            val items: Seq[(Int, List[Int])] = Seq(
                (1, List(1,2,3)),
                (2, List(1,2)),
                (3, List(1,2)),
                (4, List(1,2)),
                (5, List(1,2)),
                (6, List(1,2,3)),
                (7, List(1,2)),
                (8, List(3)),
                (9, List(3)),
                (10, List(4)),
                (11, List(4)),
                (12, List(3,4)),
                (13, List(3,4)),
                (14, List(0)),
                (15, List(0, 1))
            )
            val correctResults = Seq(
                (List(4), List(10,11)),
                (List(4,3), List(12,13)),
                (List(3), List(8,9)),
                (List(3,2,1), List(1,6)),
                (List(2,1), List(2,3,4,5,7)),
                (List(1,0), List(15)),
                (List(0), List(14))
            )

            val trie: IndicesPrefixTrie[Int] = IndicesPrefixTrie(items, emptyGeom10)
            val results = trie.getFlattenNodes
            results shouldBe correctResults
        }

        "occupy minimum storage size" in {
            val items: Seq[(Int, List[Int])] = Seq(
                (1, List(0,2,3,4,5,6,7,8,9)),
                (2, List(1,2,3,4,5,6,7,8,9)),
                (3, List(2,3,4,5,6,7,8,9)),
                (4, List(3,4,5,6,7,8,9)),
                (5, List(4,5,6,7,8,9)),
                (6, List(5,6,7,8,9)),
                (7, List(6,7,8,9)),
                (8, List(7,8,9)),
                (9, List(8,9)),
                (10, List(9))
            )
            def checkSizes(node: IndicesPrefixTrieNode[Int], initialSize: Int): Boolean ={
                lazy val childrenResults = node.children.flatten.forall(n => checkSizes(n, initialSize))
                node.children.length == (initialSize - (node.referenceIndex + 1)) && childrenResults
            }

            val trie: IndicesPrefixTrie[Int] = IndicesPrefixTrie(items, emptyGeom10)
            val check = checkSizes(trie.head, emptyGeom10.length)
            assert(check)
        }
    }
}
