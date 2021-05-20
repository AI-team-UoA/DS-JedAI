import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GridFragmentationTest  extends AnyFunSuite with Matchers {
    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val delta = 1e-10
    val lineT = 2e-1
    val polygonT = 5e-2





}
