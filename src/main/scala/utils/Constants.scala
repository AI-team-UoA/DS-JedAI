package utils

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object Constants {

	val DT_SOURCE = "source"
	val DT_TARGET = "target"

	val MAX_LAT: Double = 90.0
	val MIN_LAT: Double = -90.0
	val MAX_LONG: Double = 180.0
	val MIN_LONG: Double = -180.0
	val LAT_RANGE: Double = MAX_LAT - MIN_LAT
	val LONG_RANGE: Double = MAX_LONG - MIN_LONG

	/**
	 * Earth circumferences (in meters).
	 */
	val EARTH_CIRCUMFERENCE_EQUATORIAL = 40075160.0
	val EARTH_CIRCUMFERENCE_MERIDIONAL = 40008000.0

	/**
	 * Relations
	 */
	object Relation extends Enumeration {
		type Relation = Value
		val EQUALS: Relation.Value = Value("equals")
		val DISJOINT: Relation.Value = Value("disjoint")
		val INTERSECTS: Relation.Value = Value("intersects")
		val TOUCHES: Relation.Value = Value("touches")
		val CROSSES: Relation.Value = Value("crosses")
		val WITHIN: Relation.Value = Value("within")
		val CONTAINS: Relation.Value = Value("contains")
		val OVERLAPS: Relation.Value = Value("overlaps")
		val COVERS: Relation.Value = Value("covers")
		val COVEREDBY: Relation.Value = Value("coveredby")
		val DE9IM: Relation.Value = Value("DE9IM")

		def exists(s: String): Boolean = values.exists(_.toString == s)

		def swap(r: Relation): Relation = r match {
			case Relation.WITHIN => Relation.CONTAINS
			case Relation.CONTAINS => Relation.WITHIN
			case Relation.COVERS => Relation.COVEREDBY
			case Relation.COVEREDBY => Relation.COVERS;
			case _ => r
		}
	}

	object ThetaOption extends Enumeration{
		type ThetaOption = Value
		val MAX: Constants.ThetaOption.Value = Value("max")
		val MIN: Constants.ThetaOption.Value = Value("min")
		val AVG: Constants.ThetaOption.Value = Value("avg")
		val AVG_x2: Constants.ThetaOption.Value = Value("avg2")
		val NO_USE: Constants.ThetaOption.Value = Value("none")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	object FileTypes extends Enumeration{
		type FileTypes = Value
		val NTRIPLES: Constants.FileTypes.Value = Value("nt")
		val TURTLE: Constants.FileTypes.Value = Value("ttl")
		val RDFXML: Constants.FileTypes.Value = Value("xml")
		val RDFJSON: Constants.FileTypes.Value = Value("rj")
		val CSV: Constants.FileTypes.Value = Value("csv")
		val TSV: Constants.FileTypes.Value = Value("tsv")
		val SHP: Constants.FileTypes.Value = Value("shp")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * Weighting Strategies
	 */
	object WeightStrategy extends Enumeration {
		type WeightStrategy = Value
		val ARCS: Constants.WeightStrategy.Value = Value("ARCS")
		val CBS: Constants.WeightStrategy.Value = Value("CBS")
		val ECBS: Constants.WeightStrategy.Value = Value("ECBS")
		val JS: Constants.WeightStrategy.Value = Value("JS")
		val EJS: Constants.WeightStrategy.Value = Value("EJS")
		val PEARSON_X2: Constants.WeightStrategy.Value = Value("PEARSON_X2")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * YAML Configurations arguments
	 */
	object YamlConfiguration extends Enumeration {
		type YamlConfiguration = String
		val CONF_BLOCK_ALG = "blockingAlg"
		val CONF_PARTITIONS = "partitions"
		val CONF_SPATIAL_PARTITION = "spatialPartition"
		val CONF_THETA_MEASURE = "theta_measure"
		val CONF_STATIC_BLOCKING_DISTANCE = "static_blocking_distance"
		val CONF_SPATIAL_BLOCKING_FACTOR = "spatial_blocking_factor"
		val CONF_MATCHING_ALG = "matchingAlg"
		val CONF_WEIGHTING_STRG = "weighting_strategy"
		val CONF_BUDGET = "budget"
		val CONF_GRIDTYPE = "gridType"
		val CONF_PARTITION_BY = "partitionBy"
	}

	object GridType extends Enumeration{
		type GridType = Value
		val KDBTREE: Constants.GridType.Value = Value("KDBTREE")
		val QUADTREE: Constants.GridType.Value = Value("QUADTREE")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}


	/**
	 * Blocking Algorithms
	 */
	object BlockingAlgorithm extends Enumeration {
		type BlockingAlgorithm = Value
		val RADON: BlockingAlgorithm.Value = Value("RADON")
		val STATIC_BLOCKING: BlockingAlgorithm.Value = Value("STATIC_BLOCKING")

		def exists(s: String): Boolean = values.exists(_.toString == s)
	}

	/**
	 * Matching Algorithms
	 */
	object MatchingAlgorithm extends Enumeration {
		type MatchingAlgorithm = Value
		val GIANT: Constants.MatchingAlgorithm.Value = Value("GIANT")
		val BLOCK_CENTRIC: Constants.MatchingAlgorithm.Value = Value("BLOCK_CENTRIC")
		val PROGRESSIVE_GIANT: Constants.MatchingAlgorithm.Value = Value("PROGRESSIVE_GIANT")
		val GEOMETRY_CENTRIC: Constants.MatchingAlgorithm.Value = Value("GEOMETRY_CENTRIC")
		val ITERATIVE_GEOMETRY_CENTRIC: Constants.MatchingAlgorithm.Value = Value("ITERATIVE_GEOMETRY_CENTRIC")

		val TOPK: Constants.MatchingAlgorithm.Value = Value("TOPK")
		val RECIPROCAL_TOPK: Constants.MatchingAlgorithm.Value = Value("RECIPROCAL_TOPK")

		val LIGHT_RADON: Constants.MatchingAlgorithm.Value = Value("LIGHT_RADON")
		val PROGRESSIVE_LIGHT_RADON: Constants.MatchingAlgorithm.Value = Value("PROGRESSIVE_LIGHT_RADON")


		def exists(s: String): Boolean = values.exists(_.toString == s)
	}



}
