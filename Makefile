rsync:
	bash scripts/rsync.sh

# Spark apptainer commands
spark/build:
	bash .apptainers/spark/build.sh
spark/start:
	bash .apptainers/spark/start.sh
spark/stop:
	apptainer instance stop spark
spark/spark-shell:
	bash .apptainers/spark/spark-shell.sh
spark/shell:
	bash .apptainers/spark/shell.sh
spark/root-shell:
	apptainer instance stop spark
	bash .apptainers/spark/root-shell.sh

# Rusty-blockparser apptainer commands
rusty/build:
        bash .apptainers/rusty/build.sh
rusty/start:
        bash .apptainers/rusty/start.sh
rusty/stop:
        apptainer instance stop rusty
rusty/shell:
        bash .apptainers/rusty/shell.sh
rusty/root-shell:
	apptainer instance stop rusty
        bash .apptainers/rusty/root-shell.sh
