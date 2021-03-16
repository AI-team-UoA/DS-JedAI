package utils.readers

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.jena.query.ARQ
import org.apache.jena.riot.Lang
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import utils.{Constants, DatasetConfigurations, SparqlExecutor}
import net.sansa_stack.rdf.spark.io._
import org.apache.spark.rdd.RDD
import utils.Constants.FileTypes

import scala.collection.mutable

case class RDFGraphReader(sourceDc: DatasetConfigurations, partitions: Int, gt: Constants.GridType.GridType) extends Reader {

    def load(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
        val extension = dc.getExtension
        val lang: Lang = extension match {
            case FileTypes.NTRIPLES => Lang.NTRIPLES
            case FileTypes.TURTLE => Lang.TURTLE
            case FileTypes.RDFXML => Lang.RDFXML
            case FileTypes.RDFJSON => Lang.RDFJSON
            case _ => Lang.NTRIPLES
        }
        loadRdfAsTextual(dc.path, dc.geometryField)
    }

    def loadRdfAsTextual(filepath: String, geometryPredicate: String): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get
        GeoSparkSQLRegistrator.registerAll(spark)

        val cleanWKT = (wkt: String) => wkt.replaceAll("<\\S+>\\s?", "").replaceAll("\"", "")
        val rowRDD: RDD[Row]  = spark.read.textFile(filepath)
            .rdd.map(s => s.split(" ", 3))
            .filter(s => s(1) == geometryPredicate)
            .map(s => (s(0), cleanWKT(s(2))))
            .filter(s => s._1 != null && s._2 != null)
            .filter(s => !s._2.contains("EMPTY"))
            .map(s => Row(s._1, s._2))

        val schema = new StructType()
            .add(StructField("Subject", StringType, nullable = true))
            .add(StructField("WKT", StringType, nullable = true))

        val df = spark.createDataFrame(rowRDD, schema)
        df.createOrReplaceTempView("GEOMETRIES")
        val query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject FROM GEOMETRIES".stripMargin

        val spatialDF = spark.sql(query)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }

    def loadRDF(filepath: String, geometryPredicate: String, datePredicate: Option[String], lang: Lang): SpatialRDD[Geometry] ={
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get
        GeoSparkSQLRegistrator.registerAll(spark)
        ARQ.init()

        val allowedPredicates: mutable.Set[String] = mutable.Set()
        var sparqlQuery = s"SELECT ?Subject ?WKT WHERE { ?Subject $geometryPredicate ?WKT.}"
        var query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject FROM GEOMETRIES".stripMargin

        val cleanGeomPredicate: String =
            if (geometryPredicate.head == '<' && geometryPredicate.last == '>')
                geometryPredicate.substring(1, geometryPredicate.length-1)
            else geometryPredicate

        allowedPredicates.add(cleanGeomPredicate)

        if(datePredicate.isDefined){
            val datePredicateValue = datePredicate.get
            val cleanDatePredicate: String = if (datePredicateValue.head == '<' && datePredicateValue.last == '>')
                datePredicateValue.substring(1, datePredicateValue.length-1)
            else datePredicateValue
            allowedPredicates.add(cleanDatePredicate)
            sparqlQuery = s"SELECT ?Subject ?WKT ?Date WHERE { ?Subject ${datePredicate.get} ?Date. ?Subject $geometryPredicate ?WKT.}"
            query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject, GEOMETRIES.Date FROM GEOMETRIES".stripMargin
        }

        val triplesRDD = spark.rdf(lang)(filepath).filter(t => allowedPredicates.contains(t.getPredicate.getURI))
        var df = SparqlExecutor.query(spark, triplesRDD, sparqlQuery)

        val cleanWKT = udf( (wkt: String) => wkt.replaceAll("<\\S+>\\s?", ""), StringType)
        df = df.withColumn("WKT", cleanWKT(df.col("WKT")))
            .filter(col("WKT").isNotNull)
            .filter(! col("WKT").contains("EMPTY"))

        df.createOrReplaceTempView("GEOMETRIES")

        val spatialDF = spark.sql(query)
        val srdd = new SpatialRDD[Geometry]
        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
        srdd
    }

}
