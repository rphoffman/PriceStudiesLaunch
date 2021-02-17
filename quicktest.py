import json
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

from aws.awsconfig import AWSConfig


regionName = "us-east-1"
session = boto3.session.Session()
client = session.client(
    service_name='appconfig',
    region_name=regionName
)

response = client.list_applications(MaxResults=20)
#    ApplicationId='us6862v',
#    ConfigurationProfileId='us6862v',
#    VersionNumber=1)

print(response)

ApplicationId='6c5vgye'
ConfigurationProfileId='jj8i0nl'

response = client.list_configuration_profiles(ApplicationId=ApplicationId, MaxResults=1)
print(response)

response = client.get_configuration_profile(ApplicationId=ApplicationId, ConfigurationProfileId=ConfigurationProfileId)
print(response)

response = client.list_hosted_configuration_versions(ApplicationId=ApplicationId, ConfigurationProfileId=ConfigurationProfileId, MaxResults=1)
print(response)
items = response["Items"]
for item in items:
    print(item)
item = items[0]
version = item['VersionNumber']

response = client.get_hosted_configuration_version(ApplicationId=ApplicationId, ConfigurationProfileId=ConfigurationProfileId, VersionNumber=version)
print(response)
secret = response['Content']
data = json.loads(secret.read())
print(data)

config = AWSConfig(appId=ApplicationId, configId=ConfigurationProfileId, regionName=regionName)

print("AppConfig")
print(config.getAppConfig())
print("ECSConfig")
print(config.getECSClusterConfig())
print("Database")
print(config.getDatabaseConfig())
print("Reader Database")
print(config.getReaderDatabaseConfig())

