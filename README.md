# DS-JedAI 

DS-JedAI (Distributed Spatial JedAI) is a system for Holistic Geospatial Interlinking for big geospatial data.
In Holistic Geospatial Interlinking, we aim to discover all the topological relations between the geometries of two geospatial datasets, 
using the [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) topological model. DS-JedAI offers a novel batch algorithm for
geospatial interlinking and several algorithms for progressive Geospatial Interlinking. All the algorithms have been 
parallelized based on the MapReduce Framework. 

All algorithms implement spatial partitioning, dynamic tiling and other techniques in order to reduce the amount of redundant
verifications. The progressive methods prioritize the verification of geometry pairs that are more likely to relate. 
These algorithms take as input a budget (*BU*) that indicates the total number of verifications that will be performed, 
and a weighting scheme *W* that quantify the probability of two geometries to relate. Furthermore, DS-JedAI allows
temporal filtering in order to detect pairs that coincide not only spatially but also temporally.

DS-JedAI in implemented on top of **Apache Spark** and can run in any distributed or standalone environment that
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

- **Progressive GIA.nt**: Implements GIA.nt but prioritizes the BU most promising geometry pairs. 
- **Dynamic Progressive GIA.nt**: Extends Progressive GIA.nt by boosting the weight of the pairs that are associated with qualifying pairs. So the Priority Queue,
    that stores the geometry pairs, dynamically changes during the verifications.
- **Geometry Top-k** :  For each geometry, finds and verifies its top-k.
- **Geometry Reciprocal Top-k**: Verifies only the pairs *(s, t)* that *s* belongs to the top-k of *t* and *t* belongs to the top-k of *s*.
- **RandomScheduling**: Implements random prioritization.

Currently, the supported weighting schemes are:

- Co-occurrence Frequencies (CF)
- Jaccard Similarity (JS)
- Pearson's chi square test (PEARSON_x2)
- Minimum Bounding Rectangle Overlap (MBRO)
- Inverse Sum of Points (ISP) 

The algorithms also supports composite schemes, where we combine two weighting schemes in the sense that the second weighting
scheme is for resolving the ties of the main one.

The progressive Algorithms, the weighting schemes and the budget *BU* are specified in the configuration file. Advise the 
configuration template in `config/configurationTemplate.yaml` to see how you can specify them. To execute, run:

    spark-submit --master <master>  --class experiments.PorgressiveExp  target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>
    
Some additional options are the following:

- **-p N**: specify the number of partitions
- **-gt type**: specify the grid type for the spatial partitioning. Accepted values are KDBTREE and QUADTREE.
- **mws WS**: specify the main weighting scheme - allowed values: *CF, JS, MBRO, PEARSON_X2, ISP*.
- **sws WS**: specify the secondary weighting scheme (optional)- allowed values: *CF, JS, MBRO, PEARSON_X2, ISP*, MBRO is preferred.
- **progressiveAlgorithm PA**:  specify progressive algorithm - allowed values: *PROGRESSIVE_GIANT, DYNAMIC_PROGRESSIVE_GIANT, TOPK, RECIPROCAL_TOPK, RANDOM*
- **budget** BU: the input budget.

The command line options will overwrite the corresponding options of the configuration file. 

---
## Publication

*Progressive, Holistic Geospatial Interlinking. George Papadakis, Georgios Mandilaras, Nikos Mamoulis, Manolis Koubarakis. In Proceedings of The Web Conference  2021*