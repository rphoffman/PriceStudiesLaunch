import sys
import json
import pandas as pd
import pymysql
import time
import datetime
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from aws.awsconfig import AWSConfig
import base64
from base64 import b64decode
import random


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class LambdaExecutionError(Error):
    """Exception raised when a Lambda Function errored

    Attributes:
        function -- Function name that errored
        payload -- input expression in which the error occurred
        httpStatus -- HTTP Status of the Lambda call
        message -- explanation of the error
    """

    def __init__(self, function, payload, httpStatus, message):
        self.function = function
        self.payload = payload
        self.httpStatus = httpStatus
        self.message = message


class DBAccess:

    def dbClose(self):
        if self.conn != None:
            try:
                self.conn.close()
            except Exception as ignore:
                print(ignore)
        self.conn = None
        if self.readerConn != None:
            try:
                self.readerConn.close()
            except Exception as ignore:
                print(ignore)
        self.readerConn = None

    def getReaderDBConnect(self):
        if self.readerConn != None:
            #verify stable connection
            try:
                with self.readerConn.cursor() as cur:
                    cur.execute('SELECT VERSION()')
                    cur.fetchone()
            except:
                self.dbClose()
        if self.readerConnect == None:
            return self.getDBConnect()

        seed = int(time.time())
        sendToWriter = int(seed) % 3
        if sendToWriter == 0:
            return self.getDBConnect()

        if self.readerConn == None:
            while self.readerConn is None:
                try:
                    self.readerConn = pymysql.connect(
                        host=self.readerConnect["host"],
                        user=self.readerConnect["username"],
                        port=self.readerConnect["port"],
                        passwd=self.readerConnect["password"],
                        db=self.readerConnect["database"],
                        autocommit=True)
                except Exception as e:
                    print("action=dbConnect , state=exception , msg=" + str(e))
                    return self.getDBConnect()

        return self.readerConn

    def getDBConnect(self):
        if self.conn != None:
            #verify stable connection
            try:
                with self.conn.cursor() as cur:
                    cur.execute('SELECT VERSION()')
                    cur.fetchone()
            except:
                self.dbClose()

        if self.conn == None:
            cnt = 1
            while self.conn is None:
                try:
                    self.conn = pymysql.connect(
                        host=self.connection["host"],
                        user=self.connection["username"],
                        port=self.connection["port"],
                        passwd=self.connection["password"],
                        db=self.connection["database"],
                        autocommit=True)
                except Exception as e:
                    cnt += 1
                    print("action=dbConnect , state=exception , retries=" + str(cnt) + " , msg=" + str(e))
                    time.sleep(random.randint(5, 60))

        return self.conn

    def remove_non_sql_chars(self, data):
        trans_table = ''.join( [chr(i) for i in range(128)] + [' '] * 128 )
        if data != None:
            data = data.replace("\\", "\\\\")
            data = data.replace("'", "\\'")
            data = data.replace('"', '\\"')
            data = data.translate(trans_table)
        return data

    def __del__(self):
        self.dbClose()

    def __init__(self, awsConfig):
        self.my_config = Config(
            region_name='us-east-1',
            signature_version='v4',
            retries={
                'max_attempts': 10,
                'mode': 'standard'
            }
        )
        self.awsConfig = awsConfig
        self.connection = awsConfig.getDatabaseConfig()
        self.readerConnect = awsConfig.getReaderDatabaseConfig()
        self.conn = None
        self.readerConn = None
