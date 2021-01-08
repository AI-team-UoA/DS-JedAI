package utils

import DataStructures.{MBB, SpatialEntity}
import com.vividsolutions.jts.geom.Geometry
import org.apache.jena.query.ARQ
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.sql.functions.col
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import collection.JavaConverters._

case class SpatialReader(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType = Constants.GridType.QUADTREE) {

    lazy val gridType: GridType = gt match {
        case Constants.GridType.KDBTREE => GridType.KDBTREE
        case _ => GridType.QUADTREE
    }

    lazy val spatialRDD: SpatialRDD[Geometry] = loadSource(sourceDc)

    lazy val spatialPartitioner: SpatialPartitioner = getSpatialPartitioner(spatialRDD)

    lazy val partitioner = new HashPartitioner(spatialPartitioner.numPartitions)

    lazy val partitionsZones: Array[MBB] =
        spatialPartitioner.getGrids.asScala.map(e => MBB(e.getMaxX, e.getMinX, e.getMaxY, e.getMinY)).toArray


    def getSpatialPartitioner(srdd: SpatialRDD[Geometry]): SpatialPartitioner ={
        srdd.analyze()
        if (partitions > 0) srdd.spatialPartitioning(gridType, partitions) else srdd.spatialPartitioning(gridType)
        srdd.getPartitioner
    }


    def loadSource(dc: DatasetConfigurations): SpatialRDD[Geometry] ={
        val extension = dc.path.toString.split("\\.").last
        extension match {
            case "csv" =>
                loadCSV(dc.path, dc.realIdField.getOrElse("id"), dc.geometryField, header = true )
            case "tsv" =>
                loadTSV(dc.path, dc.realIdField.getOrElse("id"), dc.geometryField, header = true )
            case "shp" =>
                loadSHP(dc.path, dc.realIdField.getOrElse("id"), dc.geometryField)
            case "nt" =>
                loadRDF(dc.path, dc.geometryField, Lang.NTRIPLES)
            case "ttl" =>
                loadRDF(dc.path, dc.geometryField, Lang.TURTLE)
            case "rdf"|"xml" =>
                loadRDF(dc.path, dc.geometryField, Lang.RDFXML)
            case "rj" =>
                loadRDF(dc.path, dc.geometryField, Lang.RDFJSON)
            case _ =>
                null
        }
    }

    def loadCSV(filepath: String, realIdField: String, geometryField: String, header: Boolean):SpatialRDD[Geometry] =
        loadDelimitedFile(filepath, realIdField, geometryField, ",", header)

    def loadTSV(filepath: String, realIdField: String, geometryField: String, header: Boolean): SpatialRDD[Geometry] =
        loadDelimitedFile(filepath, realIdField, geometryField, "\t", header)

    /**
     * Loads a delimited file
     * @param filepath path to the delimited text file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @param delimiter delimiter
     * @param header if first row contains the headers
     * @return a spatial RDD
     */
    def loadDelimitedFile(filepath: String, realIdField: String, geometryField: String, delimiter: String, header: Boolean): SpatialRDD[Geometry] ={
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
            .filter(! col(geometryField).contains("EMPTY"))

        inputDF.createOrReplaceTempView("GEOMETRIES")

        val query = """SELECT ST_GeomFromWKT(GEOMETRIES.""" + geometryField + """) AS WKT,  GEOMETRIES.""" + realIdField + """ AS REAL_ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(query)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }

    /**
     * Loads a shapefile
     * @param filepath path to the SHP file
     * @param realIdField instances' unique id
     * @param geometryField geometry field
     * @return a spatial RDD
     */
    def loadSHP(filepath: String, realIdField: String, geometryField: String): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)

        val parentFolder = filepath.substring(0, filepath.lastIndexOf("/"))
        val srdd = ShapefileReader.readToGeometryRDD(sc, parentFolder)
        val idIndex = srdd.fieldNames.indexOf(realIdField)

        // keep only the id from user data
        srdd.rawSpatialRDD =  srdd.rawSpatialRDD.rdd.map{ g =>
            g.setUserData(g.getUserData.toString.split("\t")(idIndex))
            g
        }

        // filter records with valid geometries and ids
        srdd.rawSpatialRDD =  srdd.rawSpatialRDD.rdd.filter(g => ! (g.isEmpty || g == null || g.getUserData.toString == ""))
        srdd
    }

    /**
     * Loads RDF dataset into Spatial RDD
     * @param filepath path to the RDF file
     * @param geometryPredicate the predicate of the geometry
     * @param lang the RDF format (i.e. NTRIPLES, TURTLE, etc.)
     * @return a spatial RDD
     */
    def loadRDF(filepath: String, geometryPredicate: String, lang: Lang) : SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get

        GeoSparkSQLRegistrator.registerAll(spark)

        ARQ.init()
        val cleanGeomPredicate = if (geometryPredicate.head == '<' && geometryPredicate.last == '>') geometryPredicate.substring(1, geometryPredicate.length-1)
        val triplesRDD = spark.rdf(lang)(filepath)
            .filter(t => t.getPredicate.getURI == cleanGeomPredicate)
            .map(t => (t.getSubject.getURI, t.getObject.getLiteral.getLexicalForm))

        val triplesDF = spark.createDataFrame(triplesRDD).toDF("REAL_ID", "WKT")
        triplesDF.createOrReplaceTempView("GEOMETRIES")

        val query = """SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.REAL_ID FROM GEOMETRIES""".stripMargin
        val spatialDF = spark.sql(query)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }

    /**
     *  Loads a dataset into Spatial Partitioned RDD. The partitioner
     *  is defined by the first dataset (i.e. the source dataset)
     *
     * @param dc dataset configuration
     * @return a spatial partitioned rdd
     */
    def load(dc: DatasetConfigurations = sourceDc): RDD[(Int, SpatialEntity)] = {
        val srdd = if (dc == sourceDc) spatialRDD else loadSource(dc)
        val sp = SparkContext.getOrCreate().broadcast(spatialPartitioner)
        srdd.rawSpatialRDD.rdd
            .map{ geom =>
                val ids = geom.getUserData.asInstanceOf[String].split("\t")
                val realID = ids(0)
                (geom, realID)
            }
            .filter{case (g, _) => !g.isEmpty && g.isValid && g.getGeometryType != "GeometryCollection"}
            .map{ case(g, realId) =>  SpatialEntity(realId, g.asInstanceOf[Geometry])}
            .flatMap(se => sp.value.placeObject(se.geometry).asScala.map(i => (i._1.toInt, se)))
            .partitionBy(partitioner)
    }

}
