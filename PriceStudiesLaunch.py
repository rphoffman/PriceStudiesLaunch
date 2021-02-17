import os
import sys
import json
import time
import math
import boto3
from boto3.session import Session
from database.InvestmentManagerDB import InvestmentManagerDB
from aws.awsconfig import AWSConfig

class PriceStudiesLaunch:
    def __init__(self, appId, configId, regionName):
        print("action=constructor , state=start , appId=" + str(appId) + " , configId=" + configId)
        self.config = AWSConfig(appId=appId, configId=configId, regionName=regionName)
        self.ECSConfig = self.config.getECSClusterConfig()
        self.regionName = self.ECSConfig['RegionName']
        self.AppConfig = self.config.getAppConfig()
        self.maxNbrOfRunningTasks = self.AppConfig['MaxNbrOfRunningTasks']
        self.extractExecution = 'TargetPrice'
        self.limit = self.AppConfig['Limit']
        self.nbrOfDaysToUpdate = self.AppConfig['NbrOfDaysToUpdate']
        self.session = Session()
        self.client = self.session.client(service_name='ecs', region_name=self.regionName)
        self.doPreviousPrice = True

    def disableDoPreviousPrice(self):
        self.doPreviousPrice

    def setSelectLimit(self, limit):
        self.limit = limit

    def setNbrOfDaysToUpdate(self, days):
        self.nbrOfDaysToUpdate = days

    def getCompanyList(self):
        print("getting next set of symbols")
        im = InvestmentManagerDB(self.config)
        return im.getAllCompanies()

    def extractSymbols(self, companySymbols):
        symbols = []
        for symbol in companySymbols:
            symbols.append(symbol["symbol"])
        return symbols

    def getSymbolList(self):
        companySymbols = self.getCompanyList()
        return self.extractSymbols(companySymbols)

    #########################################################################################################
    # hELPER METHOD TO LAUNCH A cONTAINER tASK ON AN AWS Container Service cluster. This method will handle
    # retries and throtalling based on the AWS service response
    #
    # Parms:
    #   clusterName: Name of the cluster to launch the job
    #   taskDef: Job definition to use for each lanched job
    #   taskGrup - Task group to launch the task under
    #   containerName: Contianer name to override command arguments for each task,
    #   subnets - Array of cluster CID to be used for the tasks,
    #   securityGroups - array of cluster VPV security groups for the container task
    #   assignPublicIp - Public IP flag; Values: ENABLED/DISABLED
    #   command  - list of command arguments to be passed to the task when executed
    #   environment - List of enviroment variables to be passed to the task when executed
    #   retries - Number of current retries; If retry count is over 3 then the task is abandoned (Only if an Exception throw)
    #
    # returns nothing
    #########################################################################################################
    def submitTask(self, clusterName, taskDef, taskGroup, containerName, subnets, securityGroups, assignPublicIp, command, environment=[], startedBy = 'launch_all_symbols', retries=0):
        print('action=submitTask , state=start , clusterName=' + clusterName + ' , taskDef=' + taskDef + ' , taskGroup=' + taskGroup + ' , containerName=' + containerName + ' , subnets=' + str(subnets) + ' , securityGroups=' + str(securityGroups) + ' , assignPublicIp=' + assignPublicIp + ' , command=' + str(command) + ' , environment=' + str(environment) + ' , startedBy=' + startedBy + ' , retries=' + str(retries)) 
        try:
            self.waiteUntilLaunchAvailable(clusterName=clusterName, taskGroup=taskGroup)
            containerOverride = {'name':containerName, 'command':command, 'environment': environment}
            
            containerOverrides = []
            containerOverrides.append(containerOverride)
            overrides = {'containerOverrides':containerOverrides}
            networkConfiguration = {'awsvpcConfiguration':{'subnets': subnets, 'securityGroups': securityGroups, 'assignPublicIp': assignPublicIp}}
            job = self.client.run_task(
                cluster=clusterName,
                taskDefinition=taskDef,
                overrides=overrides,
                count=1,
                group=taskGroup,
                launchType='FARGATE',
                networkConfiguration=networkConfiguration,
                startedBy=startedBy,
                
            )
            retries += 1
            if 'failures' in job:
                if len(job['failures']) > 0:
                    print('action=ubmitBatch, state=submit_failure , response=' + str(job))
                    # Wait 5 minute until some of the task have completed. Task average between 3 minutes (sinle symbol) and 30 minutes multiple symbols
                    time.sleep(200)
                    self.submitTask(clusterName=clusterName, taskDef=taskDef, taskGroup=taskGroup, containerName=containerName, subnets=subnets, securityGroups=securityGroups, assignPublicIp=assignPublicIp, command=command, startedBy=startedBy, retries=retries)
        except Exception as e:
            print("action=ubmitBatch, state=exception , clusterName=" + clusterName + " , taskDef=" + taskDef + " , containerName=" + containerName + " , command=" + str(command) + ' , startedBy=' + startedBy)
            print(e)
            retries += 1
            if retries > 0:
                raise(e)
            else:
                time.sleep(200)
                self.submitTask(clusterName, taskDef, taskGroup, containerName, subnets, securityGroups, assignPublicIp, command, retries)
        time.sleep(30)
        print('action=submitTask , state=completed , clusterName=' + clusterName + ' , taskDef=' + taskDef + ' , containerName=' + containerName + ' , subnets=' + str(subnets) + ' , securityGroups=' + str(securityGroups) + ' , assignPublicIp=' + assignPublicIp + ' , command=' + str(command) + ' , startedBy=' + startedBy + ' , retries=' + str(retries))

    ########################################################################################################
    # Pauses the launch of ECS tasks until the cluster is available for additional tasks. This is to
    # make sure we also do not overload the Redshift database connection pool
    #
    # Parms:
    #    custerName : Name of the cluster to calculate the number of running tasks
    ########################################################################################################
    def waiteUntilLaunchAvailable(self, clusterName, taskGroup):
        while True:
            runningTasks = self.getRunningTasks(clusterName=clusterName, taskGroup=taskGroup)
            if runningTasks < self.maxNbrOfRunningTasks:
                return
            time.sleep(60)
            print('action=waiteUntilLaunchAvailable , state=wait , runningTasks=' + str(runningTasks))

    ########################################################################################################
    # Returns the number of running tasks on a cluster
    #
    # Parms:
    #    custerName : Name of the cluster to calculate the number of running tasks
    ########################################################################################################
    def getRunningTasks(self, clusterName, taskGroup):
        results = []
        response = self.client.list_tasks(cluster=clusterName)
        if 'taskArns' in response:
            taskArns = response['taskArns']
            if len(taskArns) > 0:
                response = self.client.describe_tasks(cluster=clusterName, tasks=taskArns)
                tasks = response['tasks']
                results = []
                for task in tasks:
                    if task['group'] == taskGroup:
                        results.append(task)
        return len(results)

    ########################################################################################################
    # Returns the number of running tasks on a cluster
    #
    # Parms:
    #    custerName : Name of the cluster to calculate the number of running tasks
    ########################################################################################################
    def nbrOfRunningTasks(self, clusterName):
        runningCount = 0
        response = self.client.list_tasks(cluster=clusterName)
        if 'taskArns' in response:
            runningCount = len(response['taskArns'])
        return runningCount

    def divideChunks(self, l, n): 
        # looping till length l 
        for i in range(0, len(l), n):  
            yield l[i:i + n] 

    #########################################################################################################
    # Launches all tasks based on the retrieved stock symbols for extracting and loading daily prices
    #########################################################################################################
    def launchAllSymbols(self):
        print("action=launchAllSymbols , state=start")
        startTime = int(time.time() * 1000)
        nbrOfTasksLaunched = 0
        clusterName = self.ECSConfig['ClusterName']
        taskDef = self.ECSConfig['TaskDef']
        containerName = self.ECSConfig['ContainerName']
        subnets = self.ECSConfig['Subnets']
        securityGroups = self.ECSConfig['SecurityGroups']
        assignPublicIp = self.ECSConfig['AssignPublicIp']
        environment = self.ECSConfig['EnvironmentVariables']
        taskGroup = 'multi-symbol-DailyPrice-launch'

        symbols = self.getSymbolList()
        maxSymbolsPerTask = math.ceil((len(symbols)/self.maxNbrOfRunningTasks))
        threadSymbols = list(self.divideChunks(symbols, maxSymbolsPerTask))

        # Make sure there are no other launches going on!!!!!!!!!!!!
        print('action=launchAllSymbols , state=waiting_for_all_tasks_to_complete')
        while self.getRunningTasks(clusterName, taskGroup) > 0:
            time.sleep(60)
        lanchSymbols = []
        symbolcnt = 0
        for lanchSymbols in threadSymbols:
            # Multiple symbol launch max 20 symbols at a time, average execution 1/2 hour
            command = []
            command.append('fromSymbol=' + lanchSymbols[0])
            command.append('toSymbol=' + lanchSymbols[len(lanchSymbols)-1])
            command.append('extractExecution=' + self.extractExecution)
            command.append('limit=' + str(self.limit))
            command.append('nbrOfDaysToUpdate=' + str(self.nbrOfDaysToUpdate))
            command.append('doPreviousPrice=' + str(self.doPreviousPrice))
            startedBy = lanchSymbols[0] + "-" + lanchSymbols[len(lanchSymbols)-1] + "_daily_price_launch" 
            self.submitTask(clusterName=clusterName, taskDef=taskDef, taskGroup=taskGroup, containerName=containerName, subnets=subnets, securityGroups=securityGroups, assignPublicIp=assignPublicIp, command=command, environment=environment, startedBy=startedBy, retries=0)
            nbrOfTasksLaunched += 1
            symbolcnt += len(lanchSymbols)

        endTime = int(time.time() * 1000)
        print('action=launchAllsymbols , state=completed , nbrOfsymbols=' + str(symbolcnt) + ' , nbrOfTasksLaunched=' + str(nbrOfTasksLaunched) + " , duration=" + str((endTime - startTime)/1000))
        return symbolcnt, nbrOfTasksLaunched

if __name__ == "__main__":
    test = False
    startTime = int(time.time() * 1000)
    if test:
        os.environ['ApplicationId'] = 'nawkh32'
        os.environ['ConfigurationProfileId'] = 'jtmybnr'
        os.environ['RegionName'] = 'us-east-1'

    ApplicationId = os.getenv("ApplicationId")
    ConfigurationProfileId = os.getenv("ConfigurationProfileId")
    RegionName = os.getenv("RegionName")
    app = PriceStudiesLaunch(appId=ApplicationId, configId=ConfigurationProfileId, regionName=RegionName)
    stocks_count, nbrOfTasksLaunched = app.launchAllSymbols()
    endTime = int(time.time() * 1000)
    print("action=PriceTargetLaunch , state=launchAllSymbols_completed , stockCount=" + str(stocks_count) +
          " , nbrOfTasksLaunched=" + str(nbrOfTasksLaunched) + " , duration=" + str((endTime - startTime)/1000))

