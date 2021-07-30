

import TestingGeometries._
import model.{SpatialIndex, TileGranularities}
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.operation.union.UnaryUnionOp
import org.scalatest.wordspec.AnyWordSpec
import utils.configuration.Constants.ThetaOption
import utils.geometryUtils.decompose.{EnvelopeRefiner, GridDecomposer, RecursiveDecomposer}
import utils.geometryUtils.{GeometryUtils, decompose}

import scala.collection.JavaConverters._


class DecompositionTest extends AnyWordSpec {

    val wktReader = new WKTReader()
    val geomFactory = new GeometryFactory()
    val delta = 1e-5
    val lineT = 2e-1
    val polygonT = 5e-2

    def tileToPolygon(t: (Int, Int), theta: TileGranularities): Geometry ={
        val x1 = t._1 * theta.x
        val y1 = t._2 * theta.y
        val x2 = (t._1+1) * theta.x
        val y2 = (t._2+1) * theta.y
        geomFactory.toGeometry(new Envelope(x1, x2, y1, y2))
    }

    /**
     * Test Geometry Collection flattening
     */
    "Flatten GeometryCollections" should {
        "maintain same area" in {
            assert(
                collections.forall { gc =>
                    val geometries: Seq[Geometry] = GeometryUtils.flattenCollection(gc)
                    val merged = geometries.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - gc.getArea)
                    diff < delta
                }
            )
        }
    }


    "RecursiveFragmentation" should {
       "produce fragments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
           val decomposer = RecursiveDecomposer(theta)
           assert(
                polygons.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
        "produce line segments of same length as the initial LineString" in {
            val theta = TileGranularities(lineStrings.map(p => p.getEnvelopeInternal), lineStrings.length, ThetaOption.AVG_x2)
            val decomposer = decompose.RecursiveDecomposer(theta)
            assert(
                lineStrings.forall { l =>
                    val lineSegments: Seq[Geometry] = decomposer.decomposeLineString(l)
                    val linesLength: Double = lineSegments.map(_.getLength).sum
                    val diff = math.abs(linesLength - l.getLength)
                    diff < delta
                }
            )
        }
        "produce fragments of same area as the initial Polygon, despite inner holes" in {
            val theta = TileGranularities(polygonsWithHoles.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = decompose.RecursiveDecomposer(theta)
            assert(
                polygonsWithHoles.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
    }

    "1D RecursiveFragmentation" should {
        "produce fragments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = RecursiveDecomposer(theta)
            assert(
                polygons.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon1D(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
        "produce fragments of same area as the initial Polygon, despite inner holes" in {
            val theta = TileGranularities(polygonsWithHoles.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = decompose.RecursiveDecomposer(theta)
            assert(
                polygonsWithHoles.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon1D(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
    }


    "2D GridFragmentation" should {
        "produce fragments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = GridDecomposer(theta)

            assert(
                polygons.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }

        "produce line segments of same length as the initial LineString" in {
            val theta = TileGranularities(lineStrings.map(p => p.getEnvelopeInternal), lineStrings.length, ThetaOption.AVG_x2)
            val decomposer = decompose.GridDecomposer(theta)
            assert(
                lineStrings.forall { l =>
                    val lineSegments: Seq[Geometry] = decomposer.decomposeLineString(l)
                    val linesLength: Double = lineSegments.map(_.getLength).sum
                    val diff = math.abs(linesLength - l.getLength)
                    diff < delta
                }
            )
        }

        "produce fragments of same area as the initial Polygon, despite inner holes" in {
            val geometries = polygonsWithHoles
            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val decomposer = decompose.GridDecomposer(theta)

            assert(
                geometries.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposeGeometry(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }

        "produce fragments that overlap a single tile" in {
            val geometries = source ++ target ++ polygons ++ polygonsWithHoles ++ lineStrings ++ geometryCollections
            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val index = SpatialIndex[Geometry](Array(), theta)
            val decomposer = decompose.GridDecomposer(theta)

            assert(
                geometries
                    .filter(p => p.getEnvelopeInternal.getWidth > theta.x || p.getEnvelopeInternal.getHeight > theta.y)
                    .forall { g =>
                        val fragments: Seq[Geometry] = decomposer.decomposeGeometry(g)
                        val tiles = fragments.map(f => index.index(f))
                        val tilesG = tiles.map(ts => ts.map(t => tileToPolygon(t, theta)))
                        val res = tiles.forall(_.length == 1)
                        res
                    }
            )
        }

        "intersecting geometries must have common tiles" in {
            val geometries = TestingGeometries.intersectingPolygonsWKT.map(p => wktReader.read(p))
            val p1 = geometries.head
            val p2 = geometries(1)

            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val decomposer = decompose.GridDecomposer(theta)

            val fragments1: Seq[Geometry] = decomposer.decomposeGeometry(p1)
            val fragments2: Seq[Geometry] = decomposer.decomposeGeometry(p2)

            val index1 = new SpatialIndex[Geometry](fragments1.toArray, theta)
            val index2 = new SpatialIndex[Geometry](fragments2.toArray, theta)

            val tilesIntersection = index1.indices.intersect(index2.indices)
            assert(tilesIntersection.nonEmpty)
        }
    }

    "1D GridFragmentation" should {
        "produce fragments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = GridDecomposer(theta)

            assert(
                polygons.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon1D(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }

        "produce fragments of same area as the initial Polygon, despite inner holes" in {
            val geometries = polygonsWithHoles
            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val decomposer = decompose.GridDecomposer(theta)

            assert(
                geometries.forall { p =>
                    val fragments: Seq[Geometry] = decomposer.decomposePolygon1D(p)
                    val merged = fragments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
    }


    "Fragmentation methods" should {
        val geometries = lineStrings ++ polygons ++ geometryCollections
        val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
        val decomposer = decompose.RecursiveDecomposer(theta)

        "RecursiveFragmentation - support all geometry types" in {
            assert(
                geometries.forall { g =>
                    val res: Seq[Geometry] = decomposer.decomposeGeometry(g)
                    val gArea: Double = res.map(_.getArea).sum
                    val diffArea = math.abs(gArea - g.getArea)
                    diffArea < delta
                }
            )
        }
        "GridFragmentation - support all geometry types" in {
            val decomposer = decompose.GridDecomposer(theta)
            assert(
                geometries.forall { g =>
                    val res: Seq[Geometry] = decomposer.decomposeGeometry(g)
                    val gArea: Double = res.map(_.getArea).sum
                    val diffArea = math.abs(gArea - g.getArea)
                    diffArea < delta
                }
            )
        }
    }

    "Envelope Fragmentation" should {
        val geometries = polygons ++ geometryCollections
        val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
        "Produce smaller Envelopes" in {
            val refiner = EnvelopeRefiner(theta)
            assert(
                geometries.forall { geom =>
                    val envelopes: Seq[Geometry] = refiner.decomposeGeometry(geom).map(e => geomFactory.toGeometry(e))
                    val env = geomFactory.toGeometry(geom.getEnvelopeInternal)
                    val unionEnv = UnaryUnionOp.union(envelopes.asJava)
                    val envelopesArea = unionEnv.getArea
                    envelopesArea <= env.getArea
                }
            )
        }
    }
}