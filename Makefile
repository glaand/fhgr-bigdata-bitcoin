rsync:
	bash scripts/rsync.sh

compile-parser:
	bash scripts/compile_fast_dat_parser.sh

build-spark:
	bash apptainers/spark-cluster/build.sh

start-spark:
	bash apptainers/spark-cluster/start.sh

stop-spark:
	apptainer instance stop spark-cluster

spark-shell:
	bash apptainers/spark-cluster/spark-shell.sh

shell:
	bash apptainers/spark-cluster/shell.sh

root-shell:
	bash apptainers/spark-cluster/root-shell.sh

