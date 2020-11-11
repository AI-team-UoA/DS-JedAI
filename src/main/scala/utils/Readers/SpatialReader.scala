package utils.Readers

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.{Constants, DatasetConfigurations}

import scala.collection.JavaConverters._

case class SpatialReader(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) {

    lazy val gridType: GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case Constants.GridType.QUADTREE => GridType.QUADTREE
    }

    lazy val spatialRDD: SpatialRDD[Geometry] = load(sourceDc)

    lazy val spatialPartitioner: SpatialPartitioner = getSpatialPartitioner(spatialRDD)

    val partitioner = new HashPartitioner(partitions)

    lazy val partitionsZones: Array[MBB] = gridType match {
        case GridType.QUADTREE =>
            spatialRDD.partitionTree.getLeafZones.asScala.map(qr => MBB(qr.getEnvelope)).toArray
        case GridType.KDBTREE =>
            spatialRDD.getPartitioner.getGrids.asScala.map(e => MBB(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY)).toArray
        }

    def expandGeometryCollection(geom: Geometry): Seq[Geometry] = {
        if (geom.getGeometryType == "GeometryCollection") {
            val gc = geom.asInstanceOf[GeometryCollection]
            for (n <- 0 until gc.getNumGeometries) yield gc.getGeometryN(n)
        }
        else Seq(geom)
    }

    def load(dc: DatasetConfigurations): SpatialRDD[Geometry] ={
        val extension = dc.path.toString.split("\\.").last
        extension match {
            case "csv" =>
                loadCSV(dc.path, dc.realIdField, dc.geometryField, header = true )
            case "tsv" =>
                loadTSV(dc.path, dc.realIdField, dc.geometryField, header = true )
            case _ =>
                null
        }
    }

    def loadCSV(filepath: String, realIdField: String, geometryField: String, header: Boolean):SpatialRDD[Geometry] =
        load(filepath, realIdField, geometryField, ",", header)

    def loadTSV(filepath: String, realIdField: String, geometryField: String, header: Boolean): SpatialRDD[Geometry] =
        load(filepath, realIdField, geometryField, "\t", header)

    def load(filepath: String, realIdField: String, geometryField: String, delimiter: String, header: Boolean): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get

        GeoSparkSQLRegistrator.registerAll(spark)

        val inputDF = spark.read.format("csv")
            .option("delimiter", delimiter)
            .option("quote", "\"")
            .option("header", header)
            .load(filepath)
            .filter(col(realIdField).isNotNull)
            .filter(col(geometryField).isNotNull)

        inputDF.createOrReplaceTempView("GEOMETRIES")

        val geometryQuery = """SELECT ST_GeomFromWKT(GEOMETRIES.""" + geometryField + """) AS WKT,  GEOMETRIES.""" + realIdField + """ AS REAL_ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(geometryQuery)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }


    def getSpatialPartitioner(srdd: SpatialRDD[Geometry]): SpatialPartitioner ={
        srdd.analyze()
        if (partitions > 0) srdd.spatialPartitioning(gridType, partitions) else srdd.spatialPartitioning(gridType)
        srdd.getPartitioner
    }


    def load2PartitionedRDD(dc: DatasetConfigurations = sourceDc): RDD[(Int, SpatialEntity)] = {
        val srdd = if (dc == sourceDc) spatialRDD else load(dc)
        val sp = SparkContext.getOrCreate().broadcast(spatialPartitioner)
        srdd.rawSpatialRDD.rdd
            .flatMap{ geom =>
                val ids = geom.getUserData.asInstanceOf[String].split("\t")
                val realID = ids(0)
                expandGeometryCollection(geom).map((_, realID))
            }
            .filter{case (g, _) => !g.isEmpty && g.isValid}
            .map{ case(g, realId) =>  SpatialEntity(realId, g.asInstanceOf[Geometry])}
            .flatMap(se => sp.value.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(partitioner)
    }

}
