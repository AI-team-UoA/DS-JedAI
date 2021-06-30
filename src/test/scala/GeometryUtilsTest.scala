import org.locationtech.jts.geom.Coordinate
import org.scalatest.wordspec.AnyWordSpec
import utils.geometryUtils.GeometryUtils

class GeometryUtilsTest extends AnyWordSpec {


    "getCenterPoints" should {
        "return (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)" in {
            val targetResults = List(1, 5.5, 10)
            val points = GeometryUtils.getCenterPoints(List(1, 10), threshold = 5)
            assert(targetResults == points)
        }

        "return (1, 3, 5, 7, 9, 10)" in {
            val targetResults = List(1, 3.25, 5.5, 7.75, 10)
            val points = GeometryUtils.getCenterPoints(List(1, 10), threshold = 4)
            assert(targetResults == points)
        }
    }

    "getHorizontalIntersectingPoints" should {
        "return (3,3) and (5,5) when being intersected with y=3 and y=5" in {
            val targetResults = List(new Coordinate(3, 3), new Coordinate(5, 5))
            val points = GeometryUtils.getIntersectionWithHorizontalLine(new Coordinate(2, 2), new Coordinate(6, 6), List(3, 5))
            assert(points == targetResults)
        }
        "return (x,y) if input segments are perpendicular" in {
            val targetResults = List(new Coordinate(2,3), new Coordinate(2,5))
            val points = GeometryUtils.getIntersectionWithHorizontalLine(new Coordinate(2,2), new Coordinate(2, 6), List(3, 5))
            assert(points == targetResults)
        }
        "return Nil if input segments are also horizontal" in {
            val targetResults = Nil
            val points = GeometryUtils.getIntersectionWithHorizontalLine(new Coordinate(2,2), new Coordinate(6,2), List(3, 5))
            assert(points == targetResults)
        }
    }

    "getVerticalIntersectingPoints" should {
        "return (3,3) and (5,5) when being intersected with x=3 and x=5" in {
            val targetResults = List(new Coordinate(3,3), new Coordinate(5, 5))
            val points = GeometryUtils.getIntersectionWithVerticalLine(new Coordinate(2,2), new Coordinate(6,6), List(3, 5))
            assert(points == targetResults)
        }
        "return (x,y) if input segments are perpendicular" in {
            val targetResults = List(new Coordinate(3,2), new Coordinate(5,2))
            val points = GeometryUtils.getIntersectionWithVerticalLine(new Coordinate(2,2), new Coordinate(6,2), List(3, 5))
            assert(points == targetResults)
        }
        "return Nil if input segments are also vertical" in {
            val targetResults = Nil
            val points = GeometryUtils.getIntersectionWithVerticalLine(new Coordinate(2,2), new Coordinate(2,6), List(3, 5))
            assert(points == targetResults)
        }
    }
}
