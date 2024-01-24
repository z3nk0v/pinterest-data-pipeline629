Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.


Project Title
Table of Contents, if the README file is long
A description of the project: what it does, the aim of the project, and what you learned
Installation instructions
Usage instructions
File structure of the project
License information


This project consists of 9 Milestones.

#1.Set up the environment
Let's set up your environment to get started!
    First we set up our github repository and then we use the aws details provided by aicore.


#2.Get Started
Sign in to AWS to start building the data pipeline.
    We obtain the user_posting_emulation.py script which contains our login credentials for our RDS database which has three tables resembling data recieved by Pinterest API.
        pinterest_data contains data about posts being updated to Pinterest
        geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
        user_data contains data about the user that has uploaded each post found in pinterest_data


#3.Batch Processing: Configure the EC2 Kafka client
You will configure an Amazon EC2 instance to use as an Apache Kafka client machine.
    Here we create a file with the .pem extension. I used the details provided by the aicore to find my key value pair for my EC2 instance in which i copied to my .pem file.

    On the MSK management console i acquired the Bootstrap servers string and the Plaintext Apache Zookeeper connection string.

    insert ssh instructions, using the chmod command i set permissions then used the ssh command 'ssh -i "0ea7b76ff169-key-pair.pem" ec2-user@ec2-35-153-180-121.compute-1.amazonaws.com' in order to connect to my ec2 instance.



#4.Batch Processing: Connect a MSK cluster to a S3 bucket
You will use MSK Connect to connect the MSK cluster to a S3 bucket, such that any data going through the cluster will be automatically saved and stored in the dedicated s3 bucket.


#5.Batch Processing: Configuring an API in API Gateway
To replicate the Pinterest's experimental data pipeline we will need to build our own API. This API will send data to the MSK cluster, which in turn will be stored in an s3 bucket, using the connector we have built in the previous milestone.


#6.Batch Processing: Databricks
You will set up your Databricks account and learn how to read data from AWS into Databricks.


#7.Batch Processing: Spark on Databricks
Learn how to perform data cleaning and computations using Spark on Databricks.


#8.Batch Processing: AWS MWAA
You will orchestrate Databricks Workloads on AWS MWAA


#9.Stream Processing: AWS Kinesis
Send streaming data to Kinesis and read this data in Databricks
