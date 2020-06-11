package utils

/**
 * @author George Mandilaras < gmandi@di.uoa.gr > (National and Kapodistrian University of Athens)
 */
object Constants {

	val DT_SOURCE = "source"
	val DT_TARGET = "target"

	val MAX = "max"
	val MIN = "min"
	val AVG = "avg"
	val AVG_x2 = "avg2"
	val RANDOM = "random"
	val NO_USE = "none"

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
	val EQUALS = "equals"
	val DISJOINT = "disjoint"
	val INTERSECTS = "intersects"
	val TOUCHES = "touches"
	val CROSSES = "crosses"
	val WITHIN = "within"
	val CONTAINS = "contains"
	val OVERLAPS = "overlaps"
	val COVERS = "covers"
	val COVEREDBY = "coveredby"

	/**
	 * Blocking Algorithms
	 */
	val RADON = "RADON"
	val STATIC_BLOCKING = "STATIC_BLOCKING"
	val LIGHT_RADON = "LIGHT_RADON"

	/**
	 * YAML Configurations arguments
	 */
	val CONF_BLOCK_ALG = "blockingAlg"
	val CONF_PARTITIONS = "partitions"
	val CONF_SPATIAL_PARTITION = "spatialPartition"
	val CONF_THETA_MEASURE = "theta_measure"
	val CONF_STATIC_BLOCKING_DISTANCE = "static_blocking_distance"
	val CONF_SPATIAL_BLOCKING_FACTOR = "spatial_blocking_factor"
	val CONF_MATCHING_ALG = "matchingAlg"
	val CONF_WEIGHTING_STRG = "weighting_strategy"
	val CONF_BUDGET = "budget"

	/**
	 * Matching Algorithms
	 */
	val SPATIAL = "SPATIAL"
	val BLOCK_CENTRIC = "BLOCK_CENTRIC"
	val COMPARISON_CENTRIC = "COMPARISON_CENTRIC"
	val ΕΝΤΙΤΥ_CENTRIC = "ΕΝΤΙΤΥ_CENTRIC"
	val ITERATIVE_ΕΝΤΙΤΥ_CENTRIC = "ITERATIVE_ΕΝΤΙΤΥ_CENTRIC"

	/**
	 * Weighting Strategies
	 */
	val ARCS = "ARCS"
	val CBS = "CBS"
	val ECBS = "ECBS"
	val JS = "JS"
	val EJS = "EJS"
	val PEARSON_X2 = "PEARSON_X2"

}
