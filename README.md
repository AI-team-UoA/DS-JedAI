# DS-JedAI 

DS-JedAI (Distributed Spatial JedAI) is a system for Holistic Geospatial Interlinking for big geospatial data.
In Holistic Geospatial Interlinking we aim to discover all the topological relations between two geospatial datasets, 
using the [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) topological model. DS-JedAI offers a novel batch algorithm for
geospatial interlinking and several algorithms for progressive Geospatial Interlinking. All the algorithms have been 
parallelized based on the MapReduce Framework. 

All algorithms implement spatial partitioning, dynamic tiling and other techniques in order to reduce the amount of redundant
verifications. The progressive methods prioritize the verification of geometry pairs that are more likely to relate. 
These algorithms take as input a budget (*BU*) that indicates the total number of verifications that will be performed, 
and a weighting scheme *W* that quantify the probability of two geometries to relate. Furthermore, DS-JedAI allows
temporal filtering in order to detect pairs that coincide not only spatially but also temporally.

DS-JedAI in implemented on top of Apache Spark and can run in any distributed or standalone environment that
supports the execution of Apache Spark jobs. Currently, supports most of the RDF formats (i.e., N­Triples, Turtle,
RDF/JSON and RDF/XML), as well as CSV, TSV, GeoJSON and ESRI shapefiles.

To build run: 
    
    sbt assembly

 
## Geospatial Interlinking At large (GIA.nt)

**GIA.nt** is a novel algorithm for batch Holistic Geospatial Interlinking and it is capable of discovering all the topological relations
between big geospatial datasets. To use GIA.nt run:

    spark-submit --master <master>  --class experiments.GiantExp  target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>

Some additional options are the following:

- **-p N**: specify the number of partitions
- **-gt type**: specify the grid type for the spatial partitioning. Accepted values are KDBTREE and QUADTREE

To create a configuration file, advise the configuration template in `config/configurationTemplate.yaml`.

## Progressive Algorithms

Despite the filtering techniques of GIA.nt, many redundant verifications are performed. In order to avoid verifications of
unrelated geometry pairs, we introduce progressive algorithms that prioritize the verification of pairs, that are more likely 
to relate. The idea is, that not all the verifications will be performed, but users provide a *budget (BU)* that denotes the number of verifications. 
Hence, the progressive algorithms prioritize the *BU* prominent verifications.
The implemented algorithms are the following:

- **Progressive Giant**: Implements GIA.nt but prioritizes the BU most promising geometry pairs. 
- **Geometry Top-k** :  For each geometry, finds and verifies its top-k.
- **Geometry Reciprocal Top-k**: Verifies only the pairs *(s, t)* that *s* belongs to the top-k of *t* and *t* belongs to the top-k of *s*.
- **Geometry-Centric**: The prioritization of *(s, t)* is based on the mean weight of all the pairs that *t* is part of.
- **RandomScheduling**: Implements random prioritization.

Currently, the supported weighting schemes are:

- Co-occurrence Frequencies (CF)
- Jaccard Similarity (JS)
- Pearson's chi square test (PEARSON_x2)
- MBR INTERSECTION
- GEOMETRY POINTS 

The progressive Algorithms, the weighting schemes and the budget *BU* are specified in the configuration file. Advise the 
configuration template in `config/configurationTemplate.yaml` to see how you can specify them. To execute, run:

    spark-submit --master <master>  --class experiments.PorgressiveExp  target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>
    
Some additional options are the following:

- **-p N**: specify the number of partitions
- **-gt type**: specify the grid type for the spatial partitioning. Accepted values are KDBTREE and QUADTREE.
- **ws WS**: specify weighting scheme - allowed values: *CF, JS, MBR_INTERSECTION, PEARSON_X2, POINTS*.
- **progressiveAlgorithm PA**:  specify progressive algorithm - allowed values: *PROGRESSIVE_GIANT, TOPK, RECIPROCAL_TOPK, GEOMETRY_CENTRIC, RANDOM*
- **budget** BU: the input budget.

The command line options will overwrite the corresponding options of the configuration file. 