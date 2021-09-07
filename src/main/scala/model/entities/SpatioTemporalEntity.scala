package model.entities

import model.approximations.{GeometryApproximationT, MBR}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants

case class SpatioTemporalEntity(originalID: String, geometry: Geometry, dateStr: String, approximation: GeometryApproximationT)  extends EntityT {

    lazy val dateTime: DateTime = {
        val formatter = DateTimeFormat.forPattern(Constants.defaultDatePattern)
        formatter.parseDateTime(dateStr)
    }


    def temporalFiltering(targetDate: DateTime): Boolean ={
        val days = math.abs(Days.daysBetween(dateTime.toLocalDate, targetDate.toLocalDate).getDays)
        days < 2
    }

    override def approximateIntersection(se: EntityT): Boolean = {
        se match {
            case entity: SpatioTemporalEntity => super.approximateIntersection(se) && temporalFiltering(entity.dateTime)
            case _ =>  super.approximateIntersection(se)
        }
    }
    override def toString: String = s"SpatioTemporal($originalID, $dateStr, ${geometry.toString}, ${approximation.toString})"

}

object SpatioTemporalEntity{

    def apply(originalID: String, geometry: Geometry, dateStr: String): SpatioTemporalEntity =
        SpatioTemporalEntity(originalID, geometry, dateStr, MBR(geometry.getEnvelopeInternal))

    def apply(originalID: String, geometry: Geometry, dateStr: String, approximationTransformer: Geometry => GeometryApproximationT): SpatioTemporalEntity = {
        SpatioTemporalEntity(originalID, geometry, dateStr, approximationTransformer(geometry))
    }
}