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
 

# input a username

read -p "Enter a unique User Name : " USER_NAME ; 
echo -e "\n * * \e[106m ...User Name to be used is... : "$USER_NAME"\e[0m \n"


echo "export USER_NAME=${USER_NAME}" >> ~/.bash_profile
cat ~/.bash_profile

 # create new folder
hadoop fs -mkdir -p /user/${USER_NAME}

hadoop fs -ls /user








```



