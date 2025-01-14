# MBD-Project
## Project overview
The project consists of shell files for importing data (```download_data.sh``` for train data, ```download_data_full.sh``` for the full data). These have been executed beforehand and thus do not need to be rerun. The data is available under ```/user/s2551055/NewsData/``` and ```/user/s2551055/NewsData_full/```. Both folders contain the same structure with parquet files in theri respective year folders. 

Furthermore python files handle the research questions by quering and aggregating the data, example being ```news.py```.

## Connecting
Assumption that eduvpn is running and connection to cluster is made, if not see:
- Linux: [install docs](https://docs.eduvpn.org/client/linux/installation.html)
- Windows: [installation exe](https://app.eduvpn.org/windows/eduVPNClient_latest.exe)
- Mac: [mac store](https://apps.apple.com/app/eduvpn-client/id1317704208)

## Setup for running python files
1. On local upload the respective file to the NFS: <br>```scp your_path/MBD-Project/news.py {snumber}@spark-head{number}.eemcs.utwente.nl:/home/snumber/```
2. Login to NFS shell: <br>Enter cluster envirmnemt ```ssh {snumber}@spark-head{number}.eemcs.utwente.nl```, check if file present ```ls```
3. Run file (don't forget custom setup in the case of executing on entire dataset!): <br> ```spark-submit news.py```

## Setup for downloading data to HDFS
There are various shell scripts present, one only downaloads the parquet files from the train repository (```download_data.sh``` => ```/user/s2551055/NewsData/```, about 25 parquet files) which is nice for quickly running queries without the need of large computational power (for most basic queries no additional executors are needed above the default).

Next there's the shell file which downloads the entire dataset (```download_data_full.sh``` => ```/user/s2551055/NewsData_full/```, about 478 parquet files). Runnig queries on this dataset is done to get the final results and requires a custom setup (figure out how many executors etc.).

### Running shell file
1. Upload shell file to NFS: ```scp your_path/MBD-Project/shell_file.sh {snumber}@spark-head{number}.eemcs.utwente.nl:/home/snumber```
2. Enter cluster envirmnemt ```ssh {snumber}@spark-head{number}.eemcs.utwente.nl```, check if file present ```ls```
3. Activate shell file: ```chmod +x shell_file.sh```
4. Execute: ```./shell_file.sh```,<br> if an error about illegal formats appears you most likely modified the shell file in an windows envirnement (adds different charachter artifacts) which can be fixed with ```sed -i 's/\r$//' shell_file.sh``` on the NFS, preferably fix it before uploading to your NFS (things on main are commited from Arch and should not have artifacts so you should always be able to revert).
5. Wait a bit, and then a bit more
6. Happy Happy Happy