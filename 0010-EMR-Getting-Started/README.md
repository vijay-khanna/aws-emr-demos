# Sample AWS EMR examples.
> Feel free to leave comments and suggestions
</br>
* ** Getting Started**
Create EMR cluster using GUI, note the Master public DNS

```
# ssh to "Master public DNS".



# login as hdfs
sudo su - hdfs


cd ; cd  /tmp ; mkdir tempwc; cd tempwc ; mkdir 0010wc ; cd 0010wc ; pwd ; mkdir -p build

## pwd : should show as /home/ec2-user/temp/0010wc

## wget files from << github>>
wget https://raw.githubusercontent.com/vijay-khanna/aws-emr-demos/master/0010-EMR-Getting-Started/emrWordCount.java
wget https://raw.githubusercontent.com/vijay-khanna/aws-emr-demos/master/0010-EMR-Getting-Started/file1.txt
wget https://raw.githubusercontent.com/vijay-khanna/aws-emr-demos/master/0010-EMR-Getting-Started/file2.txt
wget https://raw.githubusercontent.com/vijay-khanna/aws-emr-demos/master/0010-EMR-Getting-Started/file3.txt

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* emrWordCount.java -d build -Xlint 

jar -cvf emrWordCount.jar -C build/ . 


# create new folder
hadoop fs -mkdir -p /temp-wc-jobs

hadoop fs -mkdir -p /temp-wc-jobs/0010_Wordcount ; hadoop fs -mkdir -p /temp-wc-jobs/0010_Wordcount/input 

hadoop fs -ls /temp-wc-jobs/0010_Wordcount


#  Run the WordCount application from the JAR file, passing the paths to the input and output directories in HDFS.

hadoop fs -put file*.txt /temp-wc-jobs/0010_Wordcount

hadoop jar emrWordCount.jar emrWordCount /temp-wc-jobs/0010_Wordcount/input /temp-wc-jobs/0010_Wordcount/output










```



