Apache Spark examples

Install and put bin in your PATH

Create jar:

sbt assembly

Run:

spark-submit --class SimpleSpark --master local[4] --driver-memory 4G --executor-memory 4G target/scala-2.11/simpleTest-assembly-1.0.jar


Debug: See spark-debug script