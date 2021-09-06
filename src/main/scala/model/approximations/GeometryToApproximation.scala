package model.approximations

import model.TileGranularities
import org.locationtech.jts.geom.Geometry
import utils.configuration.Constants.GeometryApproximationENUM
import utils.configuration.Constants.GeometryApproximationENUM.GeometryApproximationENUM
import utils.geometryUtils.decompose.EnvelopeRefiner

object GeometryToApproximation {

    def getTransformer(approximationTypeOpt: Option[GeometryApproximationENUM], decompositionTheta: TileGranularities): Option[Geometry => GeometryApproximationT]={
        approximationTypeOpt match {
            case Some(GeometryApproximationENUM.FINEGRAINED_ENVELOPES) =>
                val refiner = EnvelopeRefiner(decompositionTheta)
                val transformer = (geometry: Geometry) => FineGrainedEnvelopes(geometry, refiner.decomposeGeometry(geometry))
                Some(transformer)
            case _ => None
        }
    }

}
