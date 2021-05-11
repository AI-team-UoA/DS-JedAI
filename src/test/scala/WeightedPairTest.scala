import model.{CompositeWP, DynamicComparisonPQ, HybridWP, MainWP, StaticComparisonPQ}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class WeightedPairTest extends AnyFunSuite with Matchers  {


    test("models.WeightedPair - MainWP with StaticComparisonPQ") {
       val pairs = List(
           MainWP(1, 1, 11, mainWeight = 0.9f),
           MainWP(2, 2, 10, mainWeight = 0.8f, secondaryWeight = 0.1f),
           MainWP(3, 3, 9, mainWeight = 0.7f, secondaryWeight = 0.2f),
           MainWP(4, 4, 8, mainWeight = 0.6f, secondaryWeight = 0.3f),
           MainWP(45, 5, 7, mainWeight = 0.6f, secondaryWeight = 0.3f),
           MainWP(5, 6, 6, mainWeight = 0.5f, secondaryWeight = 0.4f),
           MainWP(6, 7, 5, mainWeight = 0.4f, secondaryWeight = 0.5f),
           MainWP(7, 8, 4, mainWeight = 0.3f, secondaryWeight = 0.6f),
           MainWP(8, 9, 3, mainWeight = 0.2f, secondaryWeight = 0.7f),
           MainWP(9, 10, 2, mainWeight = 0.1f, secondaryWeight = 0.8f),
           MainWP(10, 11, 1, mainWeight = 0f, secondaryWeight = 0.9f)
       )
        val pq = StaticComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        val correctResults = List(1, 2, 3, 45, 4, 5, 6, 7, 8, 9, 10)
        val results = pq.dequeueAll.map(_.counter).toList
        results shouldBe correctResults
    }


    test("models.WeightedPair - CompositeWP with StaticComparisonPQ") {
        val pairs = List(
            CompositeWP(1, 1, 13, mainWeight = 0.9f, secondaryWeight = 0.1f),
            CompositeWP(2, 2, 12, mainWeight = 0.9f, secondaryWeight = 0.2f),
            CompositeWP(3, 3, 11, mainWeight = 0.8f, secondaryWeight = 0.1f),
            CompositeWP(4, 4, 10, mainWeight = 0.7f, secondaryWeight = 0.2f),
            CompositeWP(5, 5, 9, mainWeight = 0.6f, secondaryWeight = 0.3f),
            CompositeWP(6, 6, 8, mainWeight = 0.6f, secondaryWeight = 0.3f),
            CompositeWP(7, 7, 7, mainWeight = 0.5f, secondaryWeight = 0.4f),
            CompositeWP(8, 8, 6, mainWeight = 0.4f, secondaryWeight = 0.5f),
            CompositeWP(9, 9, 5, mainWeight = 0.3f, secondaryWeight = 0.6f),
            CompositeWP(10, 10, 4, mainWeight = 0.3f, secondaryWeight = 0.9f),
            CompositeWP(11, 11, 3, mainWeight = 0.2f, secondaryWeight = 0.7f),
            CompositeWP(12, 12, 2, mainWeight = 0.1f, secondaryWeight = 0.8f),
            CompositeWP(13, 13, 1, mainWeight = 0f, secondaryWeight = 0.9f)
        )
        val pq = StaticComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        val correctResults = List(2, 1, 3, 4, 6, 5, 7, 8, 10, 9, 11, 12, 13)
        val results = pq.dequeueAll.map(_.entityId1).toList
        results shouldBe correctResults
    }



    test("models.WeightedPair - HybridWP with StaticComparisonPQ") {
        val pairs = List(
            HybridWP(1, 1, 13, mainWeight = 0.9f, secondaryWeight = 1f),
            HybridWP(2, 2, 12, mainWeight = 0.8f, secondaryWeight = 2f),
            HybridWP(3, 3, 11, mainWeight = 0.7f, secondaryWeight = 3f),
            HybridWP(4, 4, 10, mainWeight = 0.6f, secondaryWeight = 4f),
            HybridWP(5, 5, 9, mainWeight = 0.5f, secondaryWeight = 5f),
            HybridWP(6, 6, 8, mainWeight = 0.4f, secondaryWeight = 6f),
            HybridWP(7, 7, 7, mainWeight = 0.3f, secondaryWeight = 7f),
            HybridWP(8, 8, 6, mainWeight = 0.2f, secondaryWeight = 8f),
            HybridWP(9, 9, 5, mainWeight = 0.1f, secondaryWeight = 9f),
            HybridWP(10,10, 4, mainWeight = 0.03f, secondaryWeight = 10f),
            HybridWP(11, 11, 3, mainWeight = 0.02f, secondaryWeight = 11f),
            HybridWP(12, 12, 2, mainWeight = 0.01f, secondaryWeight = 12f),
            HybridWP(13, 13, 1, mainWeight = 0f, secondaryWeight = 13f)
        )
        val pq = StaticComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        val correctResults = List(5, 6, 4, 7, 3, 8, 2, 9, 1, 10, 11, 12, 13)
        val results = pq.dequeueAll.map(_.entityId1).toList
        results shouldBe correctResults
    }


    test("models.WeightedPair - MainWP with DynamicComparisonPQ") {
        val pairs = List(
            MainWP(1, 1, 11, mainWeight = 0.9f),
            MainWP(2, 2, 10, mainWeight = 0.8f, secondaryWeight = 0.1f),
            MainWP(3, 3, 9, mainWeight = 0.7f, secondaryWeight = 0.2f),
            MainWP(4, 4, 8, mainWeight = 0.6f, secondaryWeight = 0.3f),
            MainWP(45, 5, 7, mainWeight = 0.6f, secondaryWeight = 0.3f),
            MainWP(5, 6, 6, mainWeight = 0.5f, secondaryWeight = 0.4f),
            MainWP(6, 7, 5, mainWeight = 0.4f, secondaryWeight = 0.5f),
            MainWP(7, 8, 4, mainWeight = 0.3f, secondaryWeight = 0.6f),
            MainWP(8, 9, 3, mainWeight = 0.2f, secondaryWeight = 0.7f),
            MainWP(9, 10, 2, mainWeight = 0.1f, secondaryWeight = 0.8f),
            MainWP(10, 11, 1, mainWeight = 0f, secondaryWeight = 0.9f)
        )
        val pq = DynamicComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        pq.dynamicUpdate(pairs(3))
        pq.dynamicUpdate(pairs(3))
        pq.dynamicUpdate(pairs(3))

        val correctResults = List(4, 1, 2, 3, 45, 5, 6, 7, 8, 9, 10)
        val results = pq.dequeueAll.map(_.counter).toList
        results shouldBe correctResults
    }


    test("models.WeightedPair - CompositeWP with DynamicComparisonPQ") {
        val pairs = List(
            CompositeWP(1, 1, 13, mainWeight = 0.9f, secondaryWeight = 0.1f),
            CompositeWP(2, 2, 12, mainWeight = 0.9f, secondaryWeight = 0.2f),
            CompositeWP(3, 3, 11, mainWeight = 0.8f, secondaryWeight = 0.1f),
            CompositeWP(4, 4, 10, mainWeight = 0.7f, secondaryWeight = 0.2f),
            CompositeWP(5, 5, 9, mainWeight = 0.6f, secondaryWeight = 0.3f),
            CompositeWP(6, 6, 8, mainWeight = 0.6f, secondaryWeight = 0.3f),
            CompositeWP(7, 7, 7, mainWeight = 0.5f, secondaryWeight = 0.4f),
            CompositeWP(8, 8, 6, mainWeight = 0.4f, secondaryWeight = 0.5f),
            CompositeWP(9, 9, 5, mainWeight = 0.3f, secondaryWeight = 0.6f),
            CompositeWP(10, 10, 4, mainWeight = 0.3f, secondaryWeight = 0.9f),
            CompositeWP(11, 11, 3, mainWeight = 0.2f, secondaryWeight = 0.7f),
            CompositeWP(12, 12, 2, mainWeight = 0.1f, secondaryWeight = 0.8f),
            CompositeWP(13, 13, 1, mainWeight = 0f, secondaryWeight = 0.9f)
        )
        val pq = DynamicComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        pq.dynamicUpdate(pairs(7))
        pq.dynamicUpdate(pairs(7))
        pq.dynamicUpdate(pairs(7))

        pq.dynamicUpdate(pairs(8))
        pq.dynamicUpdate(pairs(8))

        val correctResults = List(8, 9, 2, 1, 3, 4, 6, 5, 7, 10, 11, 12, 13)
        val results = pq.dequeueAll.map(_.entityId1).toList
        results shouldBe correctResults
    }



    test("models.WeightedPair - HybridWP with DynamicComparisonPQ") {
        val pairs = List(
            HybridWP(1, 1, 13, mainWeight = 0.9f, secondaryWeight = 1f),
            HybridWP(2, 2, 12, mainWeight = 0.8f, secondaryWeight = 2f),
            HybridWP(3, 3, 11, mainWeight = 0.7f, secondaryWeight = 3f),
            HybridWP(4, 4, 10, mainWeight = 0.6f, secondaryWeight = 4f),
            HybridWP(5, 5, 9, mainWeight = 0.5f, secondaryWeight = 5f),
            HybridWP(6, 6, 8, mainWeight = 0.4f, secondaryWeight = 6f),
            HybridWP(7, 7, 7, mainWeight = 0.3f, secondaryWeight = 7f),
            HybridWP(8, 8, 6, mainWeight = 0.2f, secondaryWeight = 8f),
            HybridWP(9, 9, 5, mainWeight = 0.1f, secondaryWeight = 9f),
            HybridWP(10,10, 4, mainWeight = 0.03f, secondaryWeight = 10f),
            HybridWP(11, 11, 3, mainWeight = 0.02f, secondaryWeight = 11f),
            HybridWP(12, 12, 2, mainWeight = 0.01f, secondaryWeight = 12f),
            HybridWP(13, 13, 1, mainWeight = 0f, secondaryWeight = 13f)
        )
        val pq = DynamicComparisonPQ(pairs.length)
        pq.enqueueAll(pairs.iterator)

        pq.dynamicUpdate(pairs.head)
        pq.dynamicUpdate(pairs.head)
        pq.dynamicUpdate(pairs.head)

        pq.dynamicUpdate(pairs(11))

        val correctResults = List(1, 5, 6, 4, 7, 3, 8, 2, 9, 12, 10, 11, 13)
        val results = pq.dequeueAll.map(_.entityId1).toList
        results shouldBe correctResults
    }

}
