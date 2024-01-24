Pinterest crunches billions of data points every day to decide how to provide more value to their users. In this project, you'll create a similar system using the AWS Cloud.


Project Title
Table of Contents, if the README file is long
A description of the project: what it does, the aim of the project, and what you learned
Installation instructions
Usage instructions
File structure of the project
License information

# Project Dependencies

In order to run this project, the following modules need to be installed:

python-dotenv
sqlalchemy
requests
airflow
If you are using Anaconda and virtual environments (recommended), the Conda environment can be cloned by running the following command, ensuring that env.yml is present in the project:

conda create env -f env.yml -n $ENVIRONMENT_NAME

This project consists of 9 Milestones.

# 1.Set up the environment
Let's set up your environment to get started!
    First we set up our github repository and then we use the aws details provided by aicore.


# 2.Get Started
Sign in to AWS to start building the data pipeline.
    We obtain the user_posting_emulation.py script which contains our login credentials for our RDS database which has three tables resembling data recieved by Pinterest API.
        pinterest_data contains data about posts being updated to Pinterest
        geolocation_data contains data about the geolocation of each Pinterest post found in pinterest_data
        user_data contains data about the user that has uploaded each post found in pinterest_data


# 3.Batch Processing: Configure the EC2 Kafka client
You will configure an Amazon EC2 instance to use as an Apache Kafka client machine.
    Here we create a file with the .pem extension. I used the details provided by the aicore to find my key value pair for my EC2 instance in which i copied to my .pem file.

    On the MSK management console i acquired the Bootstrap servers string and the Plaintext Apache Zookeeper connection string.

    using the chmod command i set permissions then used the ssh command 'ssh -i "0ea7b76ff169-key-pair.pem" ec2-user@ec2-35-153-180-121.compute-1.amazonaws.com' in order to connect to my ec2 instance.

    Once connected I downloaded Kafka 2.12-2.8.1 and created 3 topics:
        -0ea7b76ff169.Pin
        -0ea7b76ff169.Geo
        -0ea7b76ff169.User

    i verified this by running

    ./kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVER_STRING --command-config client.properties --list using the bootstrap server string acquired previously.

    Then i created a connection to the MSK cluster to my s3 bucket by creating a custom plugin in MSK connect.

    The following configuration was used for our connector:
        connector.class=io.confluent.connect.s3.S3SinkConnector
            s3.region=us-east-1
            flush.size=1
            schema.compatibility=NONE
            tasks.max=3
            topics.regex=<userid>.*
            format.class=io.confluent.connect.s3.format.json.JsonFormat
            partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
            value.converter.schemas.enable=false
            value.converter=org.apache.kafka.connect.json.JsonConverter
            storage.class=io.confluent.connect.s3.storage.S3Storage
            key.converter=org.apache.kafka.connect.storage.StringConverter
            s3.bucket.name=user-<userid></userid>-bucket



# 4.Batch Processing: Connect a MSK cluster to a S3 bucket
You will use MSK Connect to connect the MSK cluster to a S3 bucket, such that any data going through the cluster will be automatically saved and stored in the dedicated s3 bucket.

    Batch Processing: Configuring an API in API Gateway
    
    In API Gateway, follow these steps to configure a resource:
    
    Create a /{proxy+} resource.
    Enable CORS and add an HTTP method.
    Set the Endpoint URL using the PublicDNS from the EC2 instance.
    Deploy the API using the Invoke URL, storing it for later use in the pipeline.
    Note: Specify the endpoint using http instead of https due to a default issue when copying from AWS.
    
    Install the Confluent package for the Kafka REST Proxy on the EC2 instance and modify the kafka-rest.properties file. Add the following configurations:
    
    properties
    Copy code
    # Sets up TLS for encryption and SASL for authN.
    client.security.protocol = SASL_SSL
    
    # Identifies the SASL mechanism to use.
    client.sasl.mechanism = AWS_MSK_IAM
    
    # Binds SASL client implementation.
    client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";
    
    # Encapsulates constructing a SigV4 signature based on extracted credentials.
    # The SASL client bound by "sasl.jaas.config" invokes this class.
    client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
    Start the REST proxy:
    
    bash
    Copy code
    ./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties


# 5.Batch Processing: Configuring an API in API Gateway
To replicate the Pinterest's experimental data pipeline we will need to build our own API. This API will send data to the MSK cluster, which in turn will be stored in an s3 bucket, using the connector we have built in the previous milestone.

    Use the user_posting_emulation_kafka.py script to communicate with the REST proxy. Adapt the script to send JSON messages:

    python
    Copy code
    invoke_url = "https://YourAPIInvokeURL/YourDeploymentStage/topics/YourTopicName"
    payload = json.dumps({
        "records": [
            {
                "value": {"index": df["index"], "name": df["name"], "age": df["age"], "role": df["role"]}
            }
        ]
    })
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    Handle datetime objects using a custom function:


# 6.Batch Processing: Databricks
You will set up your Databricks account and learn how to read data from AWS into Databricks.


# 7.Batch Processing: Spark on Databricks
Learn how to perform data cleaning and computations using Spark on Databricks.


# 8.Batch Processing: AWS MWAA



# 9.Stream Processing: AWS Kinesis
Send streaming data to Kinesis and read this data in Databricks

    Create Kinesis streams (streaming-0ea7b76ff169-geo, streaming-0ea7b76ff169-user, streaming-00ea7b76ff169-pin). Configure API Gateway to invoke Kinesis actions.
    
    Modify the user_posting_emulation.py script for Kinesis ingestion. Adapt the data cleaning and processing pipeline.
    





This project demonstrates a data pipeline for Pinterest, encompassing batch and stream processing using AWS services, API Gateway, S3 storage, Kafka, Kinesis, Spark SQL, and Databricks.
