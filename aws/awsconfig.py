import os
import json
import boto3
from botocore.exceptions import ClientError
import base64

class AWSConfig:
    """
    The Application Id and Configuration Id are assigned by AWS AppConfig. These can be received by calling list_applications 
    and list_configuration_profiles to determine what the values fro the application need to be. This should be done before
    deploying the application and should be setup as environment variables on deployment of the application.
    """
    def __init__(self, appId, configId, regionName):
        self.appId = appId
        self.configId = configId
        self.regionName = regionName
        self.initialize()

    """
    helper method to return the custom Application Configuration
    """
    def getAppConfig(self):
        return self.getConfigObject('AppConfig')

    """
    helper method to return the ECS Cluster information
    """
    def getECSClusterConfig(self):
        return self.getConfigObject('ECSCluster')

    """
    helper method to return the Database connection information
    """
    def getDatabaseConfig(self):
        return self.getConfigObject('Database')

    """
    helper method to return the Database connection information
    """
    def getReaderDatabaseConfig(self):
        return self.getConfigObject('ReaderDatabase')

    """
    helper method to return the IEXCloud API app token and base URL
    """
    def getIEXCloudAPIConfig(self):
        return self.getConfigObject('IEXCloud')

    """
    helper method to return the any section from the dictionary root section by key name. This is used to return other secret connection information
    like API tokens or any other specialized configuration from the dictionary root keys
    """
    def getConfigObject(self, secretKey):
        if secretKey in self.config:
            return self.config[secretKey]
        return None

    """
    Initialize retrieves all configuration information for the specified application and configuration profile id for AWS AppConfig.
    If the configuration has secrets stored in AWS Secret Manage, they will be pulled and stored in the configuration variable of the
    class. The current expected app configuration profile will contain the following keys:
        1. ECSCluster         - Dictionary containing the required information to launch Tasks on an AWS ECS Cluster using Fargate
        2. AppConfig          - Dictionary containing specific application configurations (not including Database connections, API token, etc. 
                                the will be retrieved from secrete manager)
        3. SecretManagerNames - is a list of key/values idenifying the secret name to retrieve and the key used to identify the secrete
                                These names will be used to populate the configuration dictionary to hold the connection secrets. The
                                primary database connection string will specify the key name 'database'.

    Should be noted that the application should have a role providing read access to AWS AppConfig
        Required methods:
            list_hosted_configuration_versions
            get_hosted_configuration_version

    Expected AppConfig Json Structure:
        {
            "ECSCluster": { optional if not launching tasks },
            "AppConfig": { special configuration information for the app that is not standard },
            "SecretManagerNames": [
                {"Database": "secrete name"},
                {"Additional Secrets keys": "secret name}
            ]
        }
    The resulting config dictionary structure:
        {
            "ECSCluster": { optional if not launching tasks },
            "AppConfig": { special configuration information for the app that is not standard },
            "Database": { database connection information },
            "Next Secret Key": { secret information for key },
            ...
            "Last Secret Key": { secret information for key },
        }
    """
    def initialize(self):
        session = boto3.session.Session()
        client = session.client(
            service_name='appconfig',
            region_name=self.regionName
        )

        response = client.list_hosted_configuration_versions(ApplicationId=self.appId, ConfigurationProfileId=self.configId, MaxResults=1)
        self.version = response["Items"][0]['VersionNumber']
        response = client.get_hosted_configuration_version(ApplicationId=self.appId, ConfigurationProfileId=self.configId, VersionNumber=self.version)
        data = response['Content']
        self.config = json.loads(data.read())
        if 'SecretManagerNames' in self.config:
            secrets = self.config['SecretManagerNames']
            for secret in secrets:
                for key in secret.keys():
                    secretName = secret[key]
                    data = self.getSecret(secretName=secretName)
                    self.config[key] = data
                
    """
    Retrieves the AWS Secret Manager secrete. 
    
    Should be noted that the role for this application must have read access to retrieve secretes
        Required Methods:
            get_secret_value
    """
    def getSecret(self, secretName):
        regionName = "us-east-1"
        secret = "Was not set"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=regionName
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            print("Calling get_secret_value for " + str(secretName) )
            response = client.get_secret_value(
                SecretId=secretName
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in response:
                secret = response['SecretString']
            else:
                secret = base64.b64decode(response['SecretBinary'])
            print("screate recieved")
        return json.loads(secret)

