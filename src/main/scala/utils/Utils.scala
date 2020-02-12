package utils

object Utils {

	def signedPairing(x: Int, y:Int): Int ={
		val a = if (x < 0) (-2)*x - 1 else 2*x
		val b = if (y < 0) (-2)*y - 1 else 2*y

		cantorPairing(a, b)
	}
	def cantorPairing(a: Int, b: Int): Int =  (((a + b) * (a + b + 1))/2) + b
}
