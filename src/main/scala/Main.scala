import Blocking.{BlockUtils, RADON}
import DataStructures.{Comparison, SpatialEntity}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Encoder, Encoders, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import utils.ConfigurationParser
import utils.Reader.CSVReader

import scala.reflect.ClassTag



object Main {

	def main(args: Array[String]): Unit = {

		Logger.getLogger("org").setLevel(Level.ERROR)
		Logger.getLogger("akka").setLevel(Level.ERROR)
		val log = LogManager.getRootLogger
		log.setLevel(Level.INFO)

		val sparkConf = new SparkConf()
			.setAppName("SD-JedAI")
			.set("spark.serializer",classOf[KryoSerializer].getName)
		val sc = new SparkContext(sparkConf)
		val spark: SparkSession = SparkSession.builder().getOrCreate()

		// Parsing the input arguments
		@scala.annotation.tailrec
		def nextOption(map: OptionMap, list: List[String]): OptionMap = {
			list match {
				case Nil => map
				case ("-c" |"-conf") :: value :: tail =>
					nextOption(map ++ Map("conf" -> value), tail)
				case _ :: tail=>
					log.warn("DS-JEDAI: Unrecognized argument")
					nextOption(map, tail)
			}
		}

		val arglist = args.toList
		type OptionMap = Map[String, String]
		val options = nextOption(Map(), arglist)

		if(!options.contains("conf")){
			log.error("DS-JEDAI: No configuration file!")
			System.exit(1)
		}

		val conf_path = options("conf")
		val conf = ConfigurationParser.parse(conf_path)

		val sourcePath = conf.source.path
		val sourceFileExtension = sourcePath.toString.split("\\.").last
		val sourceRDD =
			sourceFileExtension match {
				case "csv" => CSVReader.loadProfiles(sourcePath, conf.source.realIdField, conf.source.geometryField)
					.map(es => (es.id, es)).partitionBy(new org.apache.spark.HashPartitioner(8)).map(_._2)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}
		val sourceCount = sourceRDD.setName("SourceRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Source: " + sourceCount)
		val indexSeparator = sourceCount.toInt

		val targetPath = conf.target.path
		val targetFileExtension = targetPath.toString.split("\\.").last
		val targetRDD =
			targetFileExtension match {
				case "csv" => CSVReader.loadProfiles2(targetPath, conf.target.realIdField, conf.target.geometryField, startIdFrom=indexSeparator)
					.map(es => (es.id, es)).partitionBy(new org.apache.spark.HashPartitioner(8)).map(_._2)
				case _ =>
					log.error("DS-JEDAI: This filetype is not supported yet")
					System.exit(1)
					null
			}

		val targetCount = targetRDD.setName("TargetRDD").cache().count()
		log.info("DS-JEDAI: Number of ptofiles of Target: " + targetCount)

/*

		implicit def single[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
		implicit def singleSE[A](implicit c: ClassTag[A]): Encoder[A] = Encoders.kryo[A](c)
		implicit def tuple[String, SpatialEntity](implicit e1: Encoder[String], e2: Encoder[String], e3: Encoder[SpatialEntity]): Encoder[(String,String, SpatialEntity)] = Encoders.tuple[String,String, SpatialEntity](e1, e2, e3)

		val unifiedRDD = sourceRDD.map(se => (se.geometry.toText,"S", se))
			.union(targetRDD.map(se => (se.geometry.toText,"T", se)))
		val dt = spark.createDataset(unifiedRDD)
		dt.show()

		    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val geometryQuery =  """SELECT ST_GeomFromWKT(inputtable.GEOMETRY_COL) AS WKT FROM inputtable""".stripMargin

    def spatialLoad(filePath: String, realIdField: String, geometryField: String, partitions: Int = 8) : RDD[SpatialEntity] = {
        GeoSparkSQLRegistrator.registerAll(spark)
        val df = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(filePath)
        df.createOrReplaceTempView("inputtable")


        val spatialDf = spark.sql(geometryQuery.replace("GEOMETRY_COL", geometryField))
        val spatialRDD = new SpatialRDD[Geometry]
        spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)
        val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
        spatialRDD.analyze()
        spatialRDD.spatialPartitioning(GridType.QUADTREE, partitions)
        spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

        null
    }

*/


		val (source, target, relation) = BlockUtils.swappingStrategy(sourceRDD, targetRDD, conf.relation)
		val radon = new RADON(source, target, relation, conf.theta_measure)
		val blocks = radon.sparseSpaceTiling().persist(StorageLevel.MEMORY_AND_DISK)
		log.info("DS-JEDAI: Number of Blocks: " + blocks.count())

		val comparisons = BlockUtils.cleanBlocks(blocks).count
		log.info("Total comparisons " + comparisons)
	}
}
