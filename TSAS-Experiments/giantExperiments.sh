echo "GIANT"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.GiantExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml 



echo "GeoSpark"

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d1.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d2.yaml 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d3.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d4.yaml

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d5.yaml 

spark-submit --master spark://pyravlos3:7078 --conf spark.cores.max=32 --conf spark.executor.cores=2 --conf spark.executor.memory=7g --conf spark.memory.fraction=0.3 --class experiments.SedonaExp DS-JedAI/target/scala-2.11/DS-JedAI-assembly-0.1.jar -conf config/d6.yaml 

