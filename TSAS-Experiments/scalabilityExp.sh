echo "PROGRESSIVE_GIANT"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 5000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 10000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 20000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 30000000


echo "DYNAMIC_PROGRESSIVE_GIANT"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 5000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 10000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 20000000

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma DYNAMIC_PROGRESSIVE_GIANT -b 30000000

