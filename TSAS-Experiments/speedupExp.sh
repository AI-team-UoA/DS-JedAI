echo "Cores 2"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 5000000

echo "Cores 4"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=4 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 5000000

echo "Cores 8"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=8 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 5000000

echo "Cores 16"
spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=16 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.ProgressiveExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml -ma PROGRESSIVE_GIANT -b 5000000
