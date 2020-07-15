# Sample AWS EMR examples.
> Feel free to leave comments and suggestions
</br>
* ** Getting Started**
Create EMR cluster using GUI, note the Master public DNS

```
# ssh to "Master public DNS".

# to List all users on hadoop
hadoop fs -ls /user


# input a username

read -p "Enter a unique User Name : " USER_NAME ; 
echo -e "\n * * \e[106m ...User Name to be used is... : "$USER_NAME"\e[0m \n"


echo "export USER_NAME=${USER_NAME}" >> ~/.bash_profile
cat ~/.bash_profile
export USER_NAME=${USER_NAME}
echo "export USER_NAME=${USER_NAME}" > /tmp/temp_profile_variable

# login as hdfs
sudo su hdfs
source /tmp/temp_profile_variable ; echo $USER_NAME

# create new folder
hadoop fs -mkdir -p /user/${USER_NAME}

hadoop fs -ls /user


## modify permissions to folder

hadoop fs -chmod 777 /user/${USER_NAME}
hadoop fs -chown ${USER_NAME} /user/${USER_NAME}

## make a folder for wordcount

hadoop fs -mkdir -p /user/${USER_NAME}/0010_Wordcount
hadoop fs -mkdir -p /user/${USER_NAME}/0010_Wordcount/input

hadoop fs -ls /user/${USER_NAME}/0010_Wordcount


exit
cd ; mkdir temp; cd temp ; mkdir 0010wc ; pwd

## pwd : should show as /home/ec2-user/temp/0010wc

## transfer the java file and sample files to /home/ec2-user/temp/0010wc folder using scp. 
OR
## wget from << github>>

mkdir -p build
jar -cvf emrWordCount.jar -C build/ . 

#  Run the WordCount application from the JAR file, passing the paths to the input and output directories in HDFS.

hadoop fs -put file* /user/${USER_NAME}/0010_Wordcount/input

hadoop jar emrWordCount.jar emrWordCount /user/${USER_NAME}/0010_Wordcount/input /user/${USER_NAME}/0010_Wordcount/output










```



