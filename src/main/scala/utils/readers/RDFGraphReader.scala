package utils.readers

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.locationtech.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utils.DatasetConfigurations

object RDFGraphReader {

    def extract(dc: DatasetConfigurations): SpatialRDD[Geometry] = {
        val extension = dc.getExtension
//        val lang: Lang = extension match {
//            case FileTypes.NTRIPLES => Lang.NTRIPLES
//            case FileTypes.TURTLE => Lang.TURTLE
//            case FileTypes.RDFXML => Lang.RDFXML
//            case FileTypes.RDFJSON => Lang.RDFJSON
//            case _ => Lang.NTRIPLES
//        }
//        loadRDF(dc.path, dc.geometryField, dc.dateField, lang)
        loadRdfAsTextual(dc.path, dc.geometryField)
    }

    def loadRdfAsTextual(filepath: String, geometryPredicate: String): SpatialRDD[Geometry] = {
        val conf = new SparkConf()
        conf.set("spark.serializer", classOf[KryoSerializer].getName)
        conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.getActiveSession.get
        SedonaSQLRegistrator.registerAll(spark)

        val cleanWKT = (wkt: String) => wkt.replaceAll("<\\S+>\\s?", "").replaceAll("\"", "")
        val rowRDD: RDD[Row] = spark.read.textFile(filepath)
            .rdd.map(s => s.split(" ", 3))
            .filter(s => s(1) == geometryPredicate)
            .map(s => (s(0), cleanWKT(s(2))))
            .filter(s => s._1 != null && s._2 != null && !s._2.isEmpty)
            .filter(s => !s._2.contains("EMPTY"))
            .map(s => Row(s._1, s._2))

        val schema = new StructType()
            .add(StructField("Subject", StringType, nullable = true))
            .add(StructField("WKT", StringType, nullable = true))

        val df = spark.createDataFrame(rowRDD, schema)
        df.createOrReplaceTempView("GEOMETRIES")
        val query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT) AS WKT,  GEOMETRIES.Subject AS Subject FROM GEOMETRIES".stripMargin

        val spatialDF = spark.sql(query)
        Adapter.toSpatialRdd(spatialDF, "0",Seq("WKT", "Subject"))
    }

//    def loadRDF(filepath: String, geometryPredicate: String, datePredicate: Option[String], lang: Lang): SpatialRDD[Geometry] = {
//        val conf = new SparkConf()
//        conf.set("spark.serializer", classOf[KryoSerializer].getName)
//        conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
//        val sc = SparkContext.getOrCreate(conf)
//        val spark = SparkSession.getActiveSession.get
//        GeoSparkSQLRegistrator.registerAll(spark)
//
//        val allowedPredicates: mutable.Set[String] = mutable.Set()
//        var sparqlQuery = s"SELECT ?Subject ?WKT WHERE { ?Subject $geometryPredicate ?WKT.}"
//        var query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject FROM GEOMETRIES".stripMargin
//
//        val cleanGeomPredicate: String =
//            if (geometryPredicate.head == '<' && geometryPredicate.last == '>')
//                geometryPredicate.substring(1, geometryPredicate.length - 1)
//            else geometryPredicate
//
//        allowedPredicates.add(cleanGeomPredicate)
//
//        if (datePredicate.isDefined) {
//            val datePredicateValue = datePredicate.get
//            val cleanDatePredicate: String = if (datePredicateValue.head == '<' && datePredicateValue.last == '>')
//                datePredicateValue.substring(1, datePredicateValue.length - 1)
//            else datePredicateValue
//            allowedPredicates.add(cleanDatePredicate)
//            sparqlQuery = s"SELECT ?Subject ?WKT ?Date WHERE { ?Subject ${datePredicate.get} ?Date. ?Subject $geometryPredicate ?WKT.}"
//            query = "SELECT ST_GeomFromWKT(GEOMETRIES.WKT),  GEOMETRIES.Subject, GEOMETRIES.Date FROM GEOMETRIES".stripMargin
//        }
//
//        ARQ.init()
//        val triplesRDD = spark.rdf(lang)(filepath).filter(t => allowedPredicates.contains(t.getPredicate.getURI))
//        var df = SparqlExecutor.query(spark, triplesRDD, sparqlQuery)
//
//        val cleanWKT = udf((wkt: String) => wkt.replaceAll("<\\S+>\\s?", ""), StringType)
//        df = df.withColumn("WKT", cleanWKT(df.col("WKT")))
//            .filter(col("WKT").isNotNull)
//            .filter(!col("WKT").contains("EMPTY"))
//
//        df.createOrReplaceTempView("GEOMETRIES")
//
//        val spatialDF = spark.sql(query)
//        val srdd = new SpatialRDD[Geometry]
//        srdd.rawSpatialRDD = Adapter.toRdd(spatialDF)
//        srdd
//    }
}
