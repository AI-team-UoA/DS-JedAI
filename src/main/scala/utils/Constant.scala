package utils

object Constant {

	val DT_SOURCE = "source"
	val DT_TARGET = "target"

	val MAX = "max"
	val MIN = "min"
	val AVG = "avg"
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
}
