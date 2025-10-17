.PHONY: deleteOld copyNew adaptToLinux runCont run

deleteOld:
	docker exec -u root spark-master rm -rf /processor

copyNew:
	docker cp processor/ spark-master:/processor/

adaptToLinux:
	docker exec -it -u root spark-master sed -i 's/\r$$//' /processor/spark_processor.py

runCont:
	docker exec -it -u 1001 spark-master /bin/bash

run: deleteOld copyNew adaptToLinux runCont

extracting: 
	python -m extract.extractor


 