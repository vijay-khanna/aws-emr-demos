# Sample AWS EMR examples.
> Feel free to leave comments and suggestions
</br>
* ** Getting Started**
Create EMR cluster using GUI, note the Master public DNS

```
# ssh to "Master public DNS".

# to List all users on hadoop
hadoop fs -ls /user

# login as hdfs
sudo su hdfs
 
 # create new folder
hadoop fs -mkdir -p /user/newuser
```
