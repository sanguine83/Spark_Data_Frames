# Steps to execute

# From Spark_DF_Operations Folder
spark_df.sh

Note: 
SPARK Commands Used:

Client Mode:
spark-submit --deploy-mode client --master yarn --class RDD_DF.Driver /home/suresh/IdeaProjects/Spark_DF_Operations/out/artifacts/Spark_DF_Operations_jar/Spark_DF_Operations.jar 100

Cluster Mode:
spark-submit --deploy-mode cluster --master yarn --class RDD_DF.Driver /home/suresh/IdeaProjects/Spark_DF_Operations/out/artifacts/Spark_DF_Operations_jar/Spark_DF_Operations.jar 100

