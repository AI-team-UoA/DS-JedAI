package EntityMatching.SemiDistributedMatching

import DataStructures.Entity
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import utils.Constants.{MatchingAlgorithm, WeightStrategy}
import utils.Configuration
import utils.Constants.WeightStrategy.WeightStrategy

object SDMFactory {

    val log: Logger = LogManager.getRootLogger

    def getMatchingAlgorithm(conf: Configuration, source: RDD[Entity], target: RDD[Entity],
                             budgetArg: Int = -1, wsArg: String = "", ma: String = ""): SDMTrait = {

        val algorithm = if(MatchingAlgorithm.exists(ma)) MatchingAlgorithm.withName(ma) else conf.getMatchingAlgorithm
        val budget = if(budgetArg > 0) budgetArg else conf.getBudget
        val ws: WeightStrategy = if(WeightStrategy.exists(wsArg.toString)) WeightStrategy.withName(wsArg) else conf.getWeightingScheme

        algorithm match {
            case MatchingAlgorithm.PROGRESSIVE_LIGHT_RADON =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.PROGRESSIVE_LIGHT_RADON)
                ProgressiveLightRADON(source, target, ws, budget)
            case MatchingAlgorithm.GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.GEOMETRY_CENTRIC)
                GeometryCentric(source, target, ws, budget)
            case MatchingAlgorithm.ITERATIVE_GEOMETRY_CENTRIC =>
                log.info("Matching Algorithm: " + MatchingAlgorithm.ITERATIVE_GEOMETRY_CENTRIC)
                IterativeGeometryCentric(source, target, ws, budget)
            case _=>
                log.info("Matching Algorithm: " + MatchingAlgorithm.LIGHT_RADON)
                LightRADON(source, target, budget)
        }
    }
}
