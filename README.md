# MBD-Project

# Setup python
On local:
```scp scp your_path/MBD-Project/news.py s2551055@spark-head2.eemcs.utwente.nl:/home/s2551055/```

Login to NFS shell and submit python file:
Enter cluster envirmnemt ```ssh s2551055@spark-head2.eemcs.utwente.nl```, check if file present ```ls```
```spark-submit news.py```



# Setup for downloading data to HDFS
## Connecting
Assumption that eduvpn is running and connection to cluster is made, if not see:
- Linux: [install docs](https://docs.eduvpn.org/client/linux/installation.html)
- Windows: [installation exe](https://app.eduvpn.org/windows/eduVPNClient_latest.exe)
- Mac: [mac store](https://apps.apple.com/app/eduvpn-client/id1317704208)

## Running shell file
1. Upload shell file to NFS: ```scp your_path/MBD-Project/download_data.sh s2551055@spark-head2.eemcs.utwente.nl:/home/s2551055```
2. Enter cluster envirmnemt ```ssh s2551055@spark-head2.eemcs.utwente.nl```, check if file present ```ls```
3. Activate shell file: ```chmod +X download_data.sh```
4. Execute: ```./download_data.sh```
5. Happy Happy Happy
