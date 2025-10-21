### These Commands are used to deploy and run the Spark processing job inside a Dockerized Spark cluster.
clean-remote:
	docker exec -u root spark-master rm -rf /processor

deploy-files:
	docker cp processor/ spark-master:/processor/

prepare-script:
	docker exec -it -u root spark-master sed -i 's/\r$$//' /processor/spark_processor.py

submit-job:
	docker exec -it -u 1001 spark-master spark-submit --master spark://spark-master:7077 --conf "spark.app.name=KafkaDataProcessor" --executor-memory 2g --num-executors 3 --conf spark.sql.streaming.concurrentJobs=3 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 /processor/spark_processor.py

run: clean-remote deploy-files prepare-script submit-job

setup-shell: 
	clean-remote deploy-files prepare-script shell

shell:
	docker exec -it -u 1001 spark-master /bin/bash

### This serves to run the data extraction script
extract-data:
	python -m extract.extractor