package utils.Readers

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryCollection}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle
import utils.Constants
import org.apache.spark.sql.functions.col

object SpatialReader extends TReader {

    var spatialPartitioner: SpatialPartitioner = _
    var partitionsZones: Array[MBB] = Array()
    var partitions: Int = 0
    var consecutiveId: Boolean = true
    var gridType: GridType = _

    def setPartitions(p: Int): Unit = partitions = p
    def setGridType(gt: Constants.GridType.GridType): Unit = {
        gt match {
            case Constants.GridType.KDBTREE =>
                gridType = GridType.KDBTREE
            case Constants.GridType.QUADTREE =>
                gridType = GridType.QUADTREE
        }
    }
    def noConsecutiveID(): Unit = consecutiveId = false

    def load(filepath: String, realIdField: String, geometryField: String): RDD[SpatialEntity] ={
        val extension = filepath.toString.split("\\.").last
        extension match {
            case "csv" =>
                loadCSV(filepath, realIdField, geometryField, header = true )
            case "tsv" =>
                loadTSV(filepath, realIdField, geometryField, header = true )
            case _ =>
                null
        }
    }

    def loadCSV(filepath: String, realIdField: String, geometryField: String, header: Boolean): RDD[SpatialEntity] =
        load(filepath, realIdField, geometryField, ",", header)

    def loadTSV(filepath: String, realIdField: String, geometryField: String, header: Boolean): RDD[SpatialEntity] =
        load(filepath, realIdField, geometryField, "\t", header)

    def load(filepath: String, realIdField: String, geometryField: String, delimiter: String, header: Boolean): RDD[SpatialEntity] ={

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

        val newSchema = StructType(inputDF.schema.fields ++ Array(StructField("ROW_ID", LongType, nullable = false)))

        // Zip on RDD level
        val rddWithId = if (consecutiveId) inputDF.rdd.zipWithIndex else inputDF.rdd.zipWithUniqueId()
        // Convert back to DataFrame
        val dfZippedWithId =  spark.createDataFrame(rddWithId.map{ case (row, index) => Row.fromSeq(row.toSeq ++ Array(index))}, newSchema)

        dfZippedWithId.createOrReplaceTempView("GEOMETRIES")

        val geometryQuery: String =  """SELECT ST_GeomFromWKT(GEOMETRIES.""" + geometryField + """) AS WKT,  GEOMETRIES.""" + realIdField + """ AS REAL_ID, GEOMETRIES.ROW_ID AS ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(geometryQuery)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)

        srdd.analyze()
        if (spatialPartitioner == null) {
            if (partitions > 0) srdd.spatialPartitioning(gridType, partitions) else srdd.spatialPartitioning(gridType)
            spatialPartitioner = srdd.getPartitioner
            partitionsZones =
            if (gridType == GridType.QUADTREE) {
                val zones = srdd.partitionTree.getLeafZones
                zones.toArray(Array.ofDim[QuadRectangle](zones.size())).map(qr => MBB(qr.getEnvelope))
            } else{
                val gridList = srdd.getPartitioner.getGrids
                val gridArray = gridList.toArray(Array.ofDim[Envelope](gridList.size))
                gridArray.map(e => MBB(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY))
            }
        }
        else
            srdd.spatialPartitioning(spatialPartitioner)

        srdd.spatialPartitionedRDD.rdd
            .flatMap{ geom =>
                    val ids = geom.getUserData.asInstanceOf[String].split("\t")
                    val realID = ids(0)
                    val id = ids(1).toInt
                    if (geom.getGeometryType == "GeometryCollection") {
                        val gc = geom.asInstanceOf[GeometryCollection]
                        for (n <- 0 until  geom.asInstanceOf[GeometryCollection].getNumGeometries) yield (gc.getGeometryN(n), realID, id)
                    }
                    else Seq((geom, realID, id))
            }
            .filter{case (g, _, _) => !g.isEmpty && g.isValid}
            .map{ case(g, realId, id) =>  SpatialEntity(id, realId, g.asInstanceOf[Geometry])}

    }
}
