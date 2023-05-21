# fhgr-bigdata-bitcoin
## Commands
In order to be able to execute the following commands, you need to change directory to `/scratch/fhgr-bigdata-bitcoin`.

**ATTENTION**: If you want to install new software which requires root permissions, you need to stop the apptainers and enter it via root-shell. the root-shell cannot start the apptainer as a backgroudn service, therefore once you finished installing your requirements, exit the shell and start the apptainer afterwards with the `/start` make command.

### General commands
Rsync bitcoin node data to the iridium server
```
make rsync
```
### Rusty apptainer commands
Build docker image `rust:latest` as a local-folder apptainer container
```
make rusty/build
```
     
Start the local-folder rusty apptainer container instance (runs in the background)
```
make rusty/start
``` 
  
Stop the local-folder rusty apptainer container instance
```
make rusty/stop
```
  
Opens a bash shell directly on your terminal (Apptainer instance must be running on background as an instance)
```
make rusty/shell
```
  
Start local-folder apptainer as root and enters it bounded to your shell (Not running on background) and also tries to stop the apptainer instance.
```
make rusty/root-shell
```

### Pyspark apptainer commands
Build docker image `apache/pyspark` as a local-folder apptainer container
```
make spark/build
```
     
Start the local-folder pyspark apptainer container instance (runs in the background)
```
make spark/start
``` 
  
Stop the local-folder pyspark apptainer container instance
```
make spark/stop
```
  
Open a pyspark shell directly on your terminal (Apptainer instance must be running on background as an instance)
```
make spark/spark-shell
```
  
Opens a bash shell directly on your terminal (Apptainer instance must be running on background as an instance)
```
make spark/shell
```
  
Start local-folder apptainer as root and enters it bounded to your shell (Not running on background) and also tries to stop the apptainer instance.
```
make spark/root-shell
```

## Blockchain data import
Following steps need to be done in order to have a working bitcoin blockchain data warehouse (hive table database) in apache spark (pyspark).

### Rusty-blockparser data processing
First its necessary to transform the raw blockchain data coming from a bitcoin full node.
We used rusty-blockparser for that to create several csv files containing human readable files with all the data from the bitcoin blockchain.
The script is available here: [./scripts/rusty-blockparser-export.sh](rusty-blockparser-export.sh).

### CSV import to apache spark
After the csv files are created, we can import the data to apache spark using pyspark to execute a import python script.
The python script creates internal hive tables from the csv files.
The tables are permanently stored in the spark-warehouse.
- Following python script can be used for the import of the csv files: [./scripts/spark_data_warehouse/create_dfs_and_database.py](create_dfs_and_database.py)
     - This is only necessary the first time the data gets imported
- Following python script can be used for the creation of dataframes from the tables after the import: [./scripts/spark_data_warehouse/load_dfs_from_warehouse.py](load_dfs_from_warehouse.py)
     - This is can be used when returning to pyspark to load the data to dataframes making it easier to work with in pyspark
