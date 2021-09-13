echo "PROGRESSIVE_GIANT BU=5M"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml -ma PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2


echo "PROGRESSIVE_GIANT BU=10M"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml -ma PROGRESSIVE_GIANT -b 10000000 -ws PEARSON_X2


echo "DYNAMIC_PROGRESSIVE_GIANT BU=5M"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2


echo "DYNAMIC_PROGRESSIVE_GIANT BU=10M"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 100000000 -ws PEARSON_X2 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 100000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 100000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 100000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 100000000 -ws PEARSON_X2

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 50000000 -ws PEARSON_X2

