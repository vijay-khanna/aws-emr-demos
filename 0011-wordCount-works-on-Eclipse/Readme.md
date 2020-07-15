# This One works fine on Eclipse Client. Getting Started First Steps.
# Works fine on AWS EMR Cluster emr-5.30.1 as well 

> Feel free to leave comments and suggestions
</br>
* ** Getting Started**

```
Used the POM.XML File attahed, and the Hadoop version 2.7.3. Need to copy the complete winutils folder to windows OS, and point PATH and HADOOP_HOME to that. 
choose the appropriate version folder, and point the path to it. 

https://github.com/vijay-khanna/hadoop-winutils

I followed the Video of "Creating a Java Program for Map/Reduce part 2/3"  to get this code working. Special thanks to the Prof who recorded this video
https://www.youtube.com/watch?v=G6kQ14AAzXQ

Many thanks Donald J. Patterson, for your excellent video and explanation to help us learn these concepts. 
```


Exporting from Eclipse 
Details of Export and Run in Prof's video : https://www.youtube.com/watch?v=JDk-LYJMzEU 

```
Export the Complete project (not the package)
> Chose "Runnable Jar" as option
> Select Launch Configuration 
> Export Destination 
> Library-Handling = Extract required libraries

Upload the Jar to S3, along with some sample text file. 

in EMR cluster, add Step
> Step Type and Name = "Custom JAR"
Jar location : S3
Arguments #1 = Input Folder on S3
Arguments #2 = Output Folder on S3








```




