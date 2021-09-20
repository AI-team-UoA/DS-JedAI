

import TestingGeometries._
import model.TileGranularities
import model.structures.SpatialIndex
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


    "RecursiveDecomposition" should {
       "produce segments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
           val decomposer = RecursiveDecomposer(theta)
           assert(
                polygons.forall { p =>
                    val segments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = segments.foldLeft(emptyPolygon)(_ union _)
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
        "produce segments of same area as the initial Polygon, despite inner holes" in {
            val theta = TileGranularities(polygonsWithHoles.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = decompose.RecursiveDecomposer(theta)
            assert(
                polygonsWithHoles.forall { p =>
                    val segments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = segments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }
    }


    "2D GridDecomposition" should {
        "produce segments of same area as the initial Polygon" in {
            val theta = TileGranularities(polygons.map(p => p.getEnvelopeInternal), polygons.length, ThetaOption.AVG_x2)
            val decomposer = GridDecomposer(theta)

            assert(
                polygons.forall { p =>
                    val segments: Seq[Geometry] = decomposer.decomposePolygon(p)
                    val merged = segments.foldLeft(emptyPolygon)(_ union _)
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

        "produce segments of same area as the initial Polygon, despite inner holes" in {
            val geometries = polygonsWithHoles
            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val decomposer = decompose.GridDecomposer(theta)

            assert(
                geometries.forall { p =>
                    val segments: Seq[Geometry] = decomposer.decomposeGeometry(p)
                    val merged = segments.foldLeft(emptyPolygon)(_ union _)
                    val diff = math.abs(merged.getArea - p.getArea)
                    diff < delta
                }
            )
        }

        "produce segments that overlap a single tile" in {
            val geometries = source ++ target ++ polygons ++ polygonsWithHoles ++ lineStrings ++ geometryCollections
            val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
            val index = SpatialIndex[Geometry](Array(), theta)
            val decomposer = decompose.GridDecomposer(theta)

            assert(
                geometries
                    .filter(p => p.getEnvelopeInternal.getWidth > theta.x || p.getEnvelopeInternal.getHeight > theta.y)
                    .forall { g =>
                        val segments: Seq[Geometry] = decomposer.decomposeGeometry(g)
                        val tiles = segments.map(f => index.index(f))
//                        val tilesG = tiles.map(ts => ts.map(t => tileToPolygon(t, theta)))
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

            val segments1: Seq[Geometry] = decomposer.decomposeGeometry(p1)
            val segments2: Seq[Geometry] = decomposer.decomposeGeometry(p2)

            val index1 = new SpatialIndex[Geometry](segments1.toArray, theta)
            val index2 = new SpatialIndex[Geometry](segments2.toArray, theta)

            val tilesIntersection = index1.indices.intersect(index2.indices)
            assert(tilesIntersection.nonEmpty)
        }
    }


    "Decomposition methods" should {
        val geometries = lineStrings ++ polygons ++ geometryCollections
        val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)
        val decomposer = decompose.RecursiveDecomposer(theta)

        "RecursiveDecomposition - support all geometry types" in {
            assert(
                geometries.forall { g =>
                    val res: Seq[Geometry] = decomposer.decomposeGeometry(g)
                    val gArea: Double = res.map(_.getArea).sum
                    val diffArea = math.abs(gArea - g.getArea)
                    diffArea < delta
                }
            )
        }
        "GridDecomposition - support all geometry types" in {
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

    "Envelope Decomposition" should {
        val geometries = polygons ++ geometryCollections
        val theta = TileGranularities(geometries.map(p => p.getEnvelopeInternal), geometries.length, ThetaOption.AVG_x2)

        "Produce smaller Envelopes" in {
            val refiner = EnvelopeRefiner(theta)
            assert(
                geometries.forall { geom =>
                    val envelopes: Seq[Geometry] = refiner.refine(geom, theta).map(e => geomFactory.toGeometry(e))
                    val env = geomFactory.toGeometry(geom.getEnvelopeInternal)
                    val unionEnv = UnaryUnionOp.union(envelopes.asJava)
                    val envelopesArea = unionEnv.getArea
                    envelopesArea <= env.getArea
                }
            )
        }
    }
}