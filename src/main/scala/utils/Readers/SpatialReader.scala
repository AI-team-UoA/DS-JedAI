package utils.Readers

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
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


object SpatialReader extends TReader {

    var spatialPartitioner: SpatialPartitioner = _
    var partitionsZones: Array[MBB] = Array()


    def load(filepath: String, realIdField: String, geometryField: String): RDD[SpatialEntity] ={
        val extension = filepath.toString.split("\\.").last
        extension match {
            case "csv" =>
                loadCSV(filepath, realIdField, geometryField, header = true)
            case "tsv" =>
                loadTSV(filepath, realIdField, geometryField, header = true)
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
            .option("header", header)
            .load(filepath)
            .filter(r =>  ! r.getAs(geometryField).asInstanceOf[String].contains("EMPTY"))


        val newSchema = StructType(inputDF.schema.fields ++ Array(StructField("ROW_ID", LongType, nullable = false)))

        // Zip on RDD level
        val rddWithId = inputDF.rdd.zipWithIndex
        // Convert back to DataFrame
        val dfZippedWithId =  spark.createDataFrame(rddWithId.map{ case (row, index) => Row.fromSeq(row.toSeq ++ Array(index))}, newSchema)


        dfZippedWithId.createOrReplaceTempView("GEOMETRIES")

        val geometryQuery: String =  """SELECT ST_GeomFromWKT(GEOMETRIES.""" + geometryField + """) AS WKT,  GEOMETRIES.""" + realIdField + """ AS REAL_ID, GEOMETRIES.ROW_ID AS ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(geometryQuery)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)

        srdd.analyze()
        if (spatialPartitioner == null) {
            srdd.spatialPartitioning(GridType.QUADTREE)
            spatialPartitioner = srdd.getPartitioner
            val zones = srdd.partitionTree.getLeafZones
            partitionsZones = zones.toArray(Array.ofDim[QuadRectangle](zones.size())).map(qr => MBB(qr.getEnvelope))

        }
        else
            srdd.spatialPartitioning(spatialPartitioner)

        srdd.spatialPartitionedRDD.rdd
            .map{ g =>
                    val ids = g.getUserData.asInstanceOf[String].split("\t")
                    val realID = ids(0)
                    val id = ids(1).toInt
                    (g, realID, id)
            }
            .map{ case(g, realID, id) => SpatialEntity(id, realID, g)}

    }
}
