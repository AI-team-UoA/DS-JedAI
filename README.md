# DS-JedAI   [<img src="https://img.shields.io/badge/dockerhub-images-important.svg?logo=dockerhub">](https://hub.docker.com/repository/docker/gmandi/ds-jedai)

DS-JedAI (Distributed Spatial JedAI) is a system for Holistic Geospatial Interlinking for big geospatial data.
In Holistic Geospatial Interlinking, we aim to discover all the topological relations between the geometries of two geospatial datasets, using the [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) topological model. DS-JedAI offers a novel batch algorithm for geospatial interlinking and several algorithms for progressive Geospatial Interlinking. All the algorithms have been parallelized based on the MapReduce Framework. 

All algorithms implement spatial partitioning, dynamic tiling and other techniques in order to reduce the number of redundant verifications. The **Progressive Algorithms** prioritize the verification of geometry pairs that are more likely to relate. These algorithms take as input a budget (*BU*) that indicates the total number of verifications that will be performed, 
and a weighting function *WF* that quantifies the probability of two geometries to relate. Furthermore, DS-JedAI allows temporal filtering in order to detect pairs that coincide not only spatially but also temporally.

DS-JedAI is implemented on top of **Apache Spark** and can run in any distributed or standalone environment that supports the execution of Apache Spark jobs. Currently, supports most of the RDF formats (i.e., N­Triples, Turtle, RDF/JSON and RDF/XML), as well as CSV, TSV, GeoJSON and ESRI shapefiles.

To build run: 
    
```bash
    $ sbt assembly
```
 
## Geospatial Interlinking At large (GIA.nt)

**GIA.nt** is a novel algorithm for batch Holistic Geospatial Interlinking designed to discover all the topological relations between two big geospatial datasets. To use GIA.nt run:

```bash
   $ spark-submit --master <master>  --class experiments.GiantExp  target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>
```

Some additional options are the following:

- **-p N**: specify the number of partitions
- **-gt type**: specify the grid type for the spatial partitioning. Accepted values are *KDBTREE* and *QUADTREE* (default)
- **-export**: Define a path to store the spatial relations

For the construction of the configuration file, advise the configuration template in `config/configurationTemplate.yaml`.

## Progressive Algorithms

Despite the filtering techniques of GIA.nt, many redundant verifications are performed. To further avoid verifications of unrelated geometry pairs, we introduce progressive algorithms that prioritize the verification of pairs, that are more likely to relate. The concept is to not perform all the verifications, but the top *budget* (i.e., *BU*) verifications, which is a number specified by the user. Hence, the goal of the progressive algorithms is to prioritize the *BU* most prominent verifications.

The implemented algorithms are the following:

- *Progressive GIA.nt*: Implements GIA.nt but prioritizes the *BU* most promising geometry pairs. 
- *Dynamic Progressive GIA.nt*: Extends Progressive GIA.nt by boosting the weight of the pairs that are associated with qualifying pairs. So, the Priority Queue that stores the geometry pairs, changes dynamically during the verification procedure.
- *Geometry Top-k* :  For each geometry, finds and verifies its top-k.
- *Geometry Reciprocal Top-k*: Verifies only the pairs *(s, t)* that *s* belongs to the top-k of *t* and *t* belongs to the top-k of *s*.
- *RandomScheduling*: Implements random prioritization.


Currently, the supported weighting functions (*WF*) are:


- *CF*: Co-occurrence Frequencies
- *JS*: Jaccard Similarity
- *PEARSON_x2*: Pearson's Chi-square test
- *MBRO*: Minimum Bounding Rectangle Overlap
- *ISP*: Inverse Sum of Points

The algorithms also support different Weighting Schemes (*WS*) that enables the combination of multiple weighting functions. Currently, the supported Weighting Schemes are:


- *SIMPLE*: use a simple Weighting Function to compute the weights (default)
- *COMPOSITE*: combine two  Weighting Functions in the sense that the second weighting function is for resolving the ties of the main one.
- *HYBRID*: the weight is defined as the product of two Weighting Functions.

In the Progressive Algorithms, the Weighting Function (*WF*), Scheme (*WS*), and the budget *BU* can be specified in the configuration file or as command-line arguments. Advise the configuration template in `config/configurationTemplate.yaml` to see how you can specify them. To execute, run:

```bash
   $ spark-submit --master <master>  --class experiments.PorgressiveExp  target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>
```


Some additional options are the following:

- **-p N**: specify the number of partitions
- **-gt type**: specify the grid type for the spatial partitioning. Accepted values are KDBTREE and QUADTREE.
- **-mwf WF**: specify the main weighting function - allowed values: *CF, JS, MBRO, PEARSON_X2, ISP*.
- **-swf WF**: specify the secondary weighting function (optional)- allowed values: *CF, JS, MBRO, PEARSON_X2, ISP*, MBRO is preferred.
-  **-ws WS**: specify the weighting scheme (optional)- allowed values: *SIMPLE, COMPOSITE, HYBRID*.
- **-pa PA**:  specify progressive algorithm - allowed values: *PROGRESSIVE_GIANT, DYNAMIC_PROGRESSIVE_GIANT, TOPK, RECIPROCAL_TOPK, RANDOM*
- **budget BU**: the input budget.


The command-line options overwrite the corresponding options of the configuration file. 

---
## Publication

*Progressive, Holistic Geospatial Interlinking. George Papadakis, Georgios Mandilaras, Nikos Mamoulis, Manolis Koubarakis. In Proceedings of The Web Conference  2021*
