package model.approximations

import org.locationtech.jts.geom.{Envelope, Geometry}

case class MBR(env: Envelope) extends GeometryApproximationT


object MBR {
    def apply(geom: Geometry): MBR = MBR(geom.getEnvelopeInternal)
}