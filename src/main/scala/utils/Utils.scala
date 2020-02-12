package utils

object Utils {

	def cantorPairing(a: Int, b: Int): Int =  (1 / 2) * (a + b) * (a + b + 1) + b
}
