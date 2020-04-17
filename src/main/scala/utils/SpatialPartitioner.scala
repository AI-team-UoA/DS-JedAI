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
import utils.Utils.printPartitions

import scala.collection.Map
import scala.reflect.ClassTag

object SpatialPartitioner {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val log: Logger = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    var spatialPartitionMapBD: Broadcast[Map[Long, Int]] = _
    implicit def singleSTR[A](implicit c: ClassTag[String]): Encoder[String] = Encoders.STRING
    implicit def singleLong[A](implicit c: ClassTag[Long]): Encoder[Long] = Encoders.scalaLong
    implicit def tuple[String, Int](implicit e1: Encoder[String], e2: Encoder[Int]): Encoder[(String,Int)] = Encoders.tuple[String,Int](e1, e2)
    val geometryQuery: String =  """SELECT ST_GeomFromWKT(GEOMETRIES._1) AS WKT,  GEOMETRIES._2 AS ID FROM GEOMETRIES""".stripMargin

    /**
     * Apply spatial partitioning for two RDD
     * First unifies the two RDD, and then applies spatial partitioning.
     * Then broadcasts a map of (ID, partitionID) and uses it in order to
     * repartition the input sets
     *
     * @param source source set
     * @param target target set
     * @param partitions number of partitions
     * @return source and target as spatial partitioned
     */
    def spatialPartition2(source: RDD[SpatialEntity], target:RDD[SpatialEntity], gridType: GridType = GridType.QUADTREE, partitions: Int = -1): (RDD[SpatialEntity], RDD[SpatialEntity]) ={

        GeoSparkSQLRegistrator.registerAll(spark)

        // unifying source and target, convert their geometries and ids into Dataset -> SpatialRDD
        val unified = source.union(target).map(se => (se.geometry.toText, se.id))
        val dt = spark.createDataset(unified)
        dt.createOrReplaceTempView("GEOMETRIES")
        val spatialDf = spark.sql(geometryQuery)
        val spatialRDD = new SpatialRDD[Geometry]
        spatialRDD.rawSpatialRDD = Adapter.toRdd(spatialDf)

        // spatial partition RDD
        spatialRDD.analyze()
        if (partitions > 0 ) spatialRDD.spatialPartitioning(gridType, partitions)
        else spatialRDD.spatialPartitioning(gridType)

        val noPartitions = spatialRDD.spatialPartitionedRDD.rdd.getNumPartitions
        // map of each spatial entity to which partition belongs to
        val spatialPartitionMap = spatialRDD.spatialPartitionedRDD.rdd.mapPartitions {
            geometries =>
                val partitionKey = TaskContext.get.partitionId()
                geometries.map(g => (g.getUserData.asInstanceOf[String].toLong, partitionKey))
        }
            .collectAsMap()
        spatialPartitionMapBD = spark.sparkContext.broadcast(spatialPartitionMap)

        val spatialPartitionedSource = source.map(se => (spatialPartitionMapBD.value(se.id), se))
            .partitionBy(new HashPartitioner(noPartitions))

        val spatialPartitionedTarget = target.map(se => (spatialPartitionMapBD.value(se.id), se))
            .partitionBy(spatialPartitionedSource.partitioner.get)

        log.info("DS-JEDAI: Spatial Partition Distribution")
        printPartitions(spatialRDD.spatialPartitionedRDD.rdd.asInstanceOf[RDD[Any]])

        //Utils.toCSV(spatialPartitionedSource.map(_._2), "/home/gmandi/Documents/Extreme-Earth/Entity-Resolution/Datasets/polar-uc-cm/tmp/source")
        //Utils.toCSV(spatialPartitionedTarget.map(_._2), "/home/gmandi/Documents/Extreme-Earth/Entity-Resolution/Datasets/polar-uc-cm/tmp/target")

        (spatialPartitionedSource.map(_._2), spatialPartitionedTarget.map(_._2))
    }


    def spatialPartitionT(mainRDD: RDD[SpatialEntity], secondaryRDD: RDD[SpatialEntity], gridType: GridType = GridType.QUADTREE, partitions: Int = -1): (RDD[SpatialEntity], RDD[SpatialEntity]) = {
        GeoSparkSQLRegistrator.registerAll(spark)

        val mainDT = spark.createDataset(mainRDD.map(se => (se.geometry.toText, se.id)))
        mainDT.createOrReplaceTempView("GEOMETRIES")
        val mainSpatialDF = spark.sql(geometryQuery)
        val mainSRDD = new SpatialRDD[Geometry]
        mainSRDD.rawSpatialRDD = Adapter.toRdd(mainSpatialDF)

        mainSRDD.analyze()
        if (partitions > 0 ) mainSRDD.spatialPartitioning(gridType, partitions)
        else mainSRDD.spatialPartitioning(gridType)

        val noPartitions = mainSRDD.spatialPartitionedRDD.rdd.getNumPartitions
        val mainPartitioningMap = mainSRDD.spatialPartitionedRDD.rdd.mapPartitions {
            geometries =>
                val partitionKey = TaskContext.get.partitionId()
                geometries.map(g => (g.getUserData.asInstanceOf[String].toLong, partitionKey))
        }.collectAsMap()
        val mainPartitioningMapBD= spark.sparkContext.broadcast(mainPartitioningMap)

        val spatialPartitionedMainRDD = mainRDD.map(se => (mainPartitioningMapBD.value(se.id), se))
            .partitionBy(new HashPartitioner(noPartitions))


        val secondadyDT = spark.createDataset(secondaryRDD.map(se => (se.geometry.toText, se.id)))
        secondadyDT.createOrReplaceTempView("GEOMETRIES")
        val secondaryDF = spark.sql(geometryQuery)
        val secondarySRDD = new SpatialRDD[Geometry]
        secondarySRDD.rawSpatialRDD = Adapter.toRdd(secondaryDF)

        secondarySRDD.analyze()
        secondarySRDD.spatialPartitioning(mainSRDD.getPartitioner)

        val secPartitioningMap = secondarySRDD.spatialPartitionedRDD.rdd.mapPartitions {
            geometries =>
                val partitionKey = TaskContext.get.partitionId()
                geometries.map(g => (g.getUserData.asInstanceOf[String].toLong, partitionKey))
        }.collectAsMap()
        val secPartitioningMapBD= spark.sparkContext.broadcast(secPartitioningMap)

        val spatialPartitionedSecRDD = secondaryRDD
            .filter(se => secPartitioningMapBD.value.contains(se.id))
            .map(se => (secPartitioningMapBD.value(se.id), se))
            .partitionBy(spatialPartitionedMainRDD.partitioner.get)

        //Utils.toCSV(spatialPartitionedMainRDD.map(_._2), "/home/gmandi/Documents/Extreme-Earth/Entity-Resolution/Datasets/polar-uc-cm/tmp/source")
        //Utils.toCSV(spatialPartitionedSecRDD.map(_._2), "/home/gmandi/Documents/Extreme-Earth/Entity-Resolution/Datasets/polar-uc-cm/tmp/target")

        (spatialPartitionedSecRDD.map(_._2), spatialPartitionedSecRDD.map(_._2))

    }


}
