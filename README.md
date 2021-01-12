# DS-JedAI 

DS-JedAI (Distributed Spatial JedAI) is an extension of [JedAI](https://github.com/scify/JedAIToolkit) for discovering spatial
relations in Big Data. I runs on top of **Apache Spark** and it employees spatial partitioning and blocking
techniques to reduces the computational cost. Also provides prioritization algorithms for progressive link discovery, 
based on some weighting schemes.

## More information and implementation details will be published soon

## How to Use

To build execute 
    
    sbt assembly
    

To run requires **Spark 2.4.4**. This project provides multiple implementations for the algorithms, for semi-distributed and
fully-distributed executions, and a technique to ensure well balanced distribution. **CURRENTLY WE ONLY PROVIDE INFORMATION
FOR THE EXECUTION OF THE FULLY DISTRIBUTED METHODS**. 

As an input argument, it requires a configuration `.yaml` file like the ones in config, where user must specify information
about the two input datasets, the target relation, and some general configuration regarding the execution. Such Information are,

* `partitions`: The number of partitions, if we want to further repartition the loaded datasets. This will probably invoke
data shuffling. It's important to mention that both dataset are loaded with the same partitioner and hence they have the same
number of partitions.

* `gridType`: Defines the Spatial Partitioner. Accepted values are `QUADTREE` and `KDBTREE`

* `thetaGranularity`: Defines the longitude and latitude granularity used for indexing. We recommend to use `avg2`, but other
supported values are `max`, `min` and `avg`.

* `matchingAlg`: Set the matching algorithm. **MORE INFORMATION WILL BE PUBLISHED LATER**. Accepted values are:
        *GIANT*, *PROGRESSIVE_GIANT*, *TOPK* and *RECIPROCAL_TOPK*.

* `budget`: Defines the budget for the progressive algorithms. 

* `weighting_strategy`: Defines the weighting strategy for the progressive algorithms. Accepted values are: *JS*, *CBS*, *ECBS* and *PEARSON_X2*
To execute, run something like:

    spark-submit --master <yarn|local|spark> --deploy-mode <client|cluster>  <spark configurations>  --class experiments.IntersectionMatrixExp <path/to/jar> -conf <path/to/configuration.yaml>

Furthermore you can use command line arguments to overwrite the configuration set by the configuration file.
Such arguments are: 

* `-p` to se `partitions`
* `-ma` to set `matchingAlg`
* `-b` to set `budget`
* `-ws` to set `weightingStrategy`
         
 
    
     