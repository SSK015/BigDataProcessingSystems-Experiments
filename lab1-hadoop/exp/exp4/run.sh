
# client
nc -l -p 9998

# server
sbt package
spark-submit --class "WordCountStructuredStreaming" ./target/scala-2.12/simple-project_2.12-1.0.jar