package utils


import DataStructures.SpatialEntity
import com.vividsolutions.jts.geom.Geometry
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

import scala.collection.Map
import scala.reflect.ClassTag

/**
 * @author George MAndilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object Utils {

	Logger.getLogger("org").setLevel(Level.ERROR)
	Logger.getLogger("akka").setLevel(Level.ERROR)
	val log: Logger = LogManager.getRootLogger
	log.setLevel(Level.INFO)

	val spark: SparkSession = SparkSession.builder().getOrCreate()
	var spatialPartitionMapBD: Broadcast[Map[Int, Int]] = _
	implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
	implicit def singleInt[A](implicit c: ClassTag[Int]): Encoder[Int] = Encoders.scalaInt
	implicit def tuple[String, Int](implicit e1: Encoder[String], e2: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](e1, e2)

	/**
	 * Apply spatial partitioning...
	 *
	 * @param source source set
	 * @param target target set
	 * @param partitions number of partitions
	 * @return source and target as spatial partitioned
	 */
	def spatialPartition(source: RDD[SpatialEntity], target:RDD[SpatialEntity], partitions: Int = 8): (RDD[SpatialEntity], RDD[SpatialEntity]) ={

		GeoSparkSQLRegistrator.registerAll(spark)
		val geometryQuery =  """SELECT ST_GeomFromWKT(GEOMETRIES._1) AS WKT,  GEOMETRIES._2 AS ID FROM GEOMETRIES""".stripMargin

		// unifying source and target, convert their geometries and ids into Dataset -> SpatialRDD
		val unified = source.union(target).map(se => (se.geometry.toText, se.id))
		val dt = spark.createDataset(unified)
		dt.createOrReplaceTempView("GEOMETRIES")
		val spatialDf = spark.sql(geometryQuery)
		val spatialRDD = new SpatialRDD[Geometry]
		spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)

		// spatial partition RDD
		spatialRDD.analyze()
		spatialRDD.spatialPartitioning(GridType.KDBTREE)

		// map of each spatial entity to which partition belongs to
		val spatialPartitionMap = spatialRDD.spatialPartitionedRDD.rdd.mapPartitions {
				geometries =>
					val partitionKey = TaskContext.get.partitionId()
					geometries.map(g => (g.getUserData.asInstanceOf[String].toInt, partitionKey))
			}
			.collectAsMap()
		spatialPartitionMapBD = spark.sparkContext.broadcast(spatialPartitionMap)

		val spatialPartitionedSource = source.map(se => (spatialPartitionMapBD.value(se.id), se))
			.partitionBy(new HashPartitioner(partitions)).map(_._2)

		val spatialPartitionedTarget = target.map(se => (spatialPartitionMapBD.value(se.id), se))
			.partitionBy(new HashPartitioner(partitions)).map(_._2)

		//WARNING: Unbalanced Results
		log.info("DS-JEDAI: Spatial Partition Distribution")
		printPartitions(spatialRDD.spatialPartitionedRDD.rdd.asInstanceOf[RDD[Any]])

		(spatialPartitionedSource, spatialPartitionedTarget)
	}


	def printPartitions(rdd: RDD[Any]): Unit ={
		spark.createDataset(rdd.mapPartitionsWithIndex{ case (i,rows) => Iterator((i,rows.size))}).show(100)
	}
}
