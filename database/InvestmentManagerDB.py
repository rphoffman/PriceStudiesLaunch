import sys
import json
import pandas as pd
import pymysql
import time
import math
from database.dbaccess import DBAccess
import threading

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


class PriceHistoryThread (threading.Thread):
    def __init__(self, awsConfig, threadID, name, request):
        threading.Thread.__init__(self)
        self.awsConfig= awsConfig
        self.threadID = threadID
        self.name = name
        self.request = request
        self.failedData = []
        self.db = InvestmentManagerDB(awsConfig)

    def getFailedData(self):
        return self.failedData

    def run(self):
        print("action=PriceHistoryThread.run , state=start , threadName=" +
              self.name + " , nbrOfSymbols=" + str(len(self.request)))
        startTime = int(time.time() * 1000)
        try:
            self.failedData = self.db.updateStockDailyPrice(self.request)
        except Exception as e:
            print("action=PredictionThread.run , state=exceptioni")
            print(str(e))
        self.db.dbClose()
        endTime = int(time.time() * 1000)
        print("action=PriceHistoryThread.run , state=start , threadName=" + self.name + " , nbrOfSymbols=" +
              str(len(self.request)) + " , errorCount=" + str(len(self.failedData)) + " , duration=" + str((endTime - startTime)/1000))

class InvestmentManagerDB(DBAccess):

    def __init__(self, awsConfig):
        DBAccess.__init__(self, awsConfig)

    def insertStockInfo(self, stockData):
        print("class=InvestmentManagerDB , action=updateStockInfo , state=start , stockCount=" + str(len(stockData)))
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        failedData = []
        for data in stockData:
            try:
                self.execInsertStockInfo(cur, data)
            except Exception as e:
                print("class=InvestmentManagerDB , action=updateStockInfo , state=Exception , symbol=" + data["symbol"] + " , msg=" + str(e))
                
                retryCnt = 0
                if "retryCnt" in data:
                    retryCnt = data["retryCnt"]
                retryCnt += 1
                if retryCnt > 3:
                    print("class=InvestmentManagerDB , action=updateStockInfo , state=Exception , symbol=" + data["symbol"] + " , msg=Removing Data cannot process without errors")
                else:
                    data["retryCnt"] = retryCnt
                    failedData.append(data)
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=updateStockInfo , state=completed , stockCount=" + str(len(stockData)) + " , errorCount=" + str(len(failedData)) + " , duration=" + str((endTime - startTime)/1000))
        return failedData

    def execInsertStockInfo(self, cur, data):
        self.addIndustry(cur, data["sector"], data["industry"])
        sql = "'" + data["symbol"] + "', NULL, 'STOCK'"
        company_name = None
        if "companyName" in data:
            company_name = self.remove_non_sql_chars(data["companyName"])
        if company_name != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + company_name + "'"
        else:
            sql = sql + ",NULL"
        employees = None
        if "employees" in data:
            employees = data["employees"]
        if employees != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + str(employees)
        else:
            sql = sql + ",NULL"
        exchange = None
        if "exchange" in data:
            exchange = self.remove_non_sql_chars(data["exchange"])
        if exchange != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + exchange + "'"
        else:
            sql = sql + ",NULL"
        website = None
        if "website" in data:
            website = data["website"]
        if website != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + website + "'"
        else:
            sql = sql + ",NULL"
        description = None
        if "description" in data:
            description = self.remove_non_sql_chars(data["description"])
        if description != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + description + "'"
        else:
            sql = sql + ",NULL"
        ceo = None
        if "CEO" in data:
            ceo = self.remove_non_sql_chars(data["CEO"])
        if ceo != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + ceo + "'"
        else:
            sql = sql + ",NULL"
        security_name = None
        if "securityName" in data:
            security_name = self.remove_non_sql_chars(data["securityName"])
        if security_name != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + security_name + "'"
        else:
            sql = sql + ",NULL"
        issue_type = None
        if "issueType" in data:
            issue_type = data["issueType"]
        if issue_type != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + issue_type + "'"
        else:
            sql = sql + ",NULL"
        primary_sic_code = None
        if "primarySicCode" in data:
            primary_sic_code = data["primarySicCode"]
        if primary_sic_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + str(primary_sic_code) + "'"
        else:
            sql = sql + ",NULL"
        address_1 = None
        if "address" in data:
            address_1 = self.remove_non_sql_chars(data["address"])
        if address_1 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + address_1 + "'"
        else:
            sql = sql + ",NULL"
        address_2 = None
        if "address2" in data:
            address_2 = self.remove_non_sql_chars(data["address2"])
        if address_2 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + address_2 + "'"
        else:
            sql = sql + ",NULL"
        city = None
        if "city" in data:
            city = self.remove_non_sql_chars(data["city"])
        if city != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + city + "'"
        else:
            sql = sql + ",NULL"
        state = None
        if "state" in data:
            state = data["state"]
        if state != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + state + "'"
        else:
            sql = sql + ",NULL"
        postal_code = None
        if "zip" in data:
            postal_code = data["zip"]
        if postal_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + postal_code + "'"
        else:
            sql = sql + ",NULL"
        country = None
        if "country" in data:
            country = data["country"]
        if country != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + country + "'"
        else:
            sql = sql + ",NULL"
        phone = None
        if "phone" in data:
            phone = data["phone"]
        if phone != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + phone + "'"
        else:
            sql = sql + ",NULL"
        industry_name = None
        if "industry" in data:
            industry_name = data["industry"]
        if industry_name != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + industry_name + "'"
        else:
            sql = sql + ",NULL"
        sql = sql + ",NULL,NULL,1"

        sql = "INSERT INTO `investment_management`.`stocks` (`stock_symbol`,`company_id`,`symbol_type`,`company_name`,`employees`,`exchange`,`website`,`description`,`ceo`,`security_name`,`issue_type`,`primary_sic_code`,`address_1`,`address_2`,`city`,`state`,`postal_code`,`country`,`phone`,`industry_name`,`region`,`reference_date`,`is_iex_enabled`) " + \
            " VALUES(" + sql + ")"
        try:
            cur.execute(sql)
        except Exception as e:
            print("class=InvestmentManagerDB , action=execUpdateStockInfo , state=Exception , symbol=" + data["symbol"] + " sql=" + sql + " , msg=" + str(e))
            self.execUpdateStockInfo(cur=cur, data=data)            
        else:
            self.addTags(cur, data["symbol"], data["tags"])
            self.addPeers(cur, data["symbol"], data["peers"])
            self.addPriceTarget(cur, data["symbol"], data["priceTarget"])

    def execUpdateStockInfo(self, cur, data):
        self.addIndustry(cur, data["sector"], data["industry"])
        sql = ""
        company_name = None
        if "companyName" in data:
            company_name = self.remove_non_sql_chars(data["companyName"])
        if company_name != None:
            sql = sql + "company_name = '" + company_name + "'"
        employees = None
        if "employees" in data:
            employees = data["employees"]
        if employees != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "employees = " + str(employees)
        exchange = None
        if "exchange" in data:
            exchange = self.remove_non_sql_chars(data["exchange"])
        if exchange != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "`exchange` = '" + exchange + "'"
        website = None
        if "website" in data:
            website = data["website"]
        if website != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "website = '" + website + "'"
        description = None
        if "description" in data:
            description = self.remove_non_sql_chars(data["description"])
        if description != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "description = '" + description + "'"
        ceo = None
        if "CEO" in data:
            ceo = self.remove_non_sql_chars(data["CEO"])
        if ceo != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "ceo = '" + ceo + "'"
        security_name = None
        if "securityName" in data:
            security_name = self.remove_non_sql_chars(data["securityName"])
        if security_name != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "security_name = '" + security_name + "'"
        issue_type = None
        if "issueType" in data:
            issue_type = data["issueType"]
        if issue_type != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "issue_type = '" + issue_type + "'"
        primary_sic_code = None
        if "primarySicCode" in data:
            primary_sic_code = data["primarySicCode"]
        if primary_sic_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "primary_sic_code = '" + str(primary_sic_code) + "'"
        address_1 = None
        if "address" in data:
            address_1 = self.remove_non_sql_chars(data["address"])
        if address_1 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "address_1 = '" + address_1 + "'"
        address_2 = None
        if "address2" in data:
            address_2 = self.remove_non_sql_chars(data["address2"])
        if address_2 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "address_2 = '" + address_2 + "'"
        city = None
        if "city" in data:
            city = self.remove_non_sql_chars(data["city"])
        if city != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "city = '" + city + "'"
        state = None
        if "state" in data:
            state = data["state"]
        if state != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "state = '" + state + "'"
        postal_code = None
        if "zip" in data:
            postal_code = data["zip"]
        if postal_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "postal_code = '" + postal_code + "'"
        country = None
        if "country" in data:
            country = data["country"]
        if country != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "country = '" + country + "'"
        phone = None
        if "phone" in data:
            phone = data["phone"]
        if phone != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "phone = '" + phone + "'"
        industry_name = None
        if "industry" in data:
            if len(sql) > 0:
                sql = sql + ", "
            industry_name = data["industry"]
        if industry_name != None:
            sql = sql + "industry_name = '" + industry_name + "'"

        sql = "UPDATE `investment_management`.`stocks` SET " + \
            sql + " WHERE stock_symbol = '" + data["symbol"] + "'"
        try:
            cur.execute(sql)
        except Exception as e:
            print("class=InvestmentManagerDB , action=execUpdateStockInfo , state=Exception , symbol=" + data["symbol"] + " sql=" + sql + " , msg=" + str(e))
            
            raise e
        else:
            self.addTags(cur, data["symbol"], data["tags"])
            self.addPeers(cur, data["symbol"], data["peers"])
            self.addPriceTarget(cur, data["symbol"], data["priceTarget"])
            
    def updateStockInfo(self, stockData):
        print("class=InvestmentManagerDB , action=updateStockInfo , state=start , stockCount=" + str(len(stockData)))
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        failedData = []
        for data in stockData:
            try:
                self.execUpdateStockInfo(cur, data)
            except Exception as e:
                print("class=InvestmentManagerDB , action=updateStockInfo , state=Exception , symbol=" + data["symbol"] + " , msg=" + str(e))
                failedData.append(data)
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=updateStockInfo , state=completed , stockCount=" + str(len(stockData)) + " , errorCount=" + str(len(failedData)) + " , duration=" + str((endTime - startTime)/1000))
        return failedData

    def deleteUnknownSymbols(self):
        cur = self.getDBConnect().cursor()
        sql = "DELETE FROM stocks WHERE company_name is NULL"
        try:
            cur.execute(sql)
            cur.execute("commit")
        except Exception as ignor:
            print("class=InvestmentManagerDB , action=deleteUnknownSymbols , state=Exception , sql=" + sql + " , msg=" + str(ignor))

        cur.close()


    def execUpdateStockInfo2(self, cur, data):
        self.addIndustry(cur, data["sector"], data["industry"])
        sql = ""
        company_name = None
        if "companyName" in data:
            company_name = self.remove_non_sql_chars(data["companyName"])
        if company_name != None:
            sql = sql + "company_name = '" + company_name + "'"
        employees = None
        if "employees" in data:
            employees = data["employees"]
        if employees != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "employees = " + str(employees)
        exchange = None
        if "exchange" in data:
            exchange = self.remove_non_sql_chars(data["exchange"])
        if exchange != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "`exchange` = '" + exchange + "'"
        website = None
        if "website" in data:
            website = data["website"]
        if website != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "website = '" + website + "'"
        description = None
        if "description" in data:
            description = self.remove_non_sql_chars(data["description"])
        if description != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "description = '" + description + "'"
        ceo = None
        if "CEO" in data:
            ceo = self.remove_non_sql_chars(data["CEO"])
        if ceo != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "ceo = '" + ceo + "'"
        security_name = None
        if "securityName" in data:
            security_name = self.remove_non_sql_chars(data["securityName"])
        if security_name != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "security_name = '" + security_name + "'"
        issue_type = None
        if "issueType" in data:
            issue_type = data["issueType"]
        if issue_type != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "issue_type = '" + issue_type + "'"
        primary_sic_code = None
        if "primarySicCode" in data:
            primary_sic_code = data["primarySicCode"]
        if primary_sic_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "primary_sic_code = '" + str(primary_sic_code) + "'"
        address_1 = None
        if "address" in data:
            address_1 = self.remove_non_sql_chars(data["address"])
        if address_1 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "address_1 = '" + address_1 + "'"
        address_2 = None
        if "address2" in data:
            address_2 = self.remove_non_sql_chars(data["address2"])
        if address_2 != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "address_2 = '" + address_2 + "'"
        city = None
        if "city" in data:
            city = self.remove_non_sql_chars(data["city"])
        if city != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "city = '" + city + "'"
        state = None
        if "state" in data:
            state = data["state"]
        if state != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "state = '" + state + "'"
        postal_code = None
        if "zip" in data:
            postal_code = data["zip"]
        if postal_code != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "postal_code = '" + postal_code + "'"
        country = None
        if "country" in data:
            country = data["country"]
        if country != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "country = '" + country + "'"
        phone = None
        if "phone" in data:
            phone = data["phone"]
        if phone != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "phone = '" + phone + "'"
        industry_name = None
        if "industry" in data:
            if len(sql) > 0:
                sql = sql + ", "
            industry_name = data["industry"]
        if industry_name != None:
            sql = sql + "industry_name = '" + industry_name + "'"

        sql = "UPDATE `investment_management`.`stocks` SET " + \
            sql + " WHERE stock_symbol = '" + data["symbol"] + "'"
        try:
            cur.execute(sql)
        except Exception as e:
            print("class=InvestmentManagerDB , action=execUpdateStockInfo , state=Exception , symbol=" + data["symbol"] + " sql=" + sql + " , msg=" + str(e))
            
            raise e
        else:
            self.addTags(cur, data["symbol"], data["tags"])
            self.addPeers(cur, data["symbol"], data["peers"])
            self.addPriceTarget(cur, data["symbol"], data["priceTarget"])

    def addIndustry(self, cur, sector, industry):
        self.addSector(cur, sector)
        sql = "INSERT IGNORE INTO `investment_management`.`industry` (`industry_name`, `sector_name`) VALUES ('" + \
            industry + "', '" + sector + "')"
        try:
            cur.execute(sql)
        except pymysql.err.IntegrityError as ignor:
            if ignor.args[0] != 1062:
                print("class=InvestmentManagerDB , action=addIndustry , state=Exception , symbol=" + sector + " , industry=" + industry + " , sql=" + sql + " , msg=" + str(ignor))

    def addSector(self, cur, sector):
        sql = "INSERT IGNORE INTO `investment_management`.`sectors` (`sector_name`) VALUES ('" + \
            sector + "')"
        try:
            cur.execute(sql)
        except pymysql.err.IntegrityError as ignor:
            if ignor.args[0] != 1062:
                print("class=InvestmentManagerDB , action=addSector , state=Exception , sector=" + sector + " , sql=" + sql + " , msg=" + str(ignor))

    def addTags(self, cur, symbol, tags):
        for tag in tags:
            sql = "INSERT IGNORE INTO `investment_management`.`tags` (`stock_symbol`, `tag`) VALUES('" + \
                symbol + "', '" + tag + "')"
            try:
                cur.execute(sql)
            except pymysql.err.IntegrityError as ignor:
                if ignor.args[0] != 1062:
                    print("class=InvestmentManagerDB , action=addTags , state=Exception , symbol=" + symbol + " , sql=" + sql + " , msg=" + str(ignor))

    def addPeers(self, cur, symbol, peers):
        sql = "DELETE FROM `investment_management`.`peer_group` WHERE primary_stock_symbol = '" + symbol + "'"
        try:
            cur.execute(sql)
        except Exception as ignor:
            print("class=InvestmentManagerDB , action=addPeers , state=Exception , symbol=" + symbol + " , sql=" + sql + " , msg=" + str(ignor))
            
        for peer in peers:
            sql = "INSERT INTO `investment_management`.`peer_group` (`primary_stock_symbol`, `peer_stock_symbol`) VALUES('" + \
                symbol + "', '" + peer + "')"
            try:
                cur.execute(sql)
            except pymysql.err.IntegrityError as ignor:
                if ignor.args[0] == 1452:
                    # Need to add stock to stock table....
                    try:
                        missingSQL = "INSERT INTO investment_management.stocks (stock_symbol, symbol_type) VALUES('" + \
                            peer + "', 'STOCK')"
                        cur.execute(missingSQL)
                        cur.execute(sql)
                        cur.execute("commit")
                    except pymysql.err.IntegrityError as ignorAgain:
                        print("class=InvestmentManagerDB , action=addPeers , state=Exception , symbol=" + symbol + " , missingStockSql=" + missingSQL + " , sql=" + sql + " , msg=" + str(ignorAgain))
                        
                else:
                    if ignor.args[0] != 1062:
                        print("class=InvestmentManagerDB , action=addPeers , state=Exception , symbol=" + symbol + " , sql=" + sql + " , msg=" + str(ignor))
                        

    def addPriceTarget(self, cur, symbol, priceTarget):
        if priceTarget == None:
            return
        sql = "'" + symbol + "'"
        updated_date = None
        if "updatedDate" in priceTarget:
            updated_date = priceTarget["updatedDate"]
        if updated_date != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + updated_date + "'"
        price_target_average = None
        if "priceTargetAverage" in priceTarget:
            price_target_average = priceTarget["priceTargetAverage"]
        if price_target_average != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + str(price_target_average)
        price_target_high = None
        if "priceTargetHigh" in priceTarget:
            price_target_high = priceTarget["priceTargetHigh"]
        if price_target_high != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + str(price_target_high)
        price_target_low = None
        if "priceTargetLow" in priceTarget:
            price_target_low = priceTarget["priceTargetLow"]
        if price_target_low != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + str(price_target_low)
        number_of_analysts = None
        if "numberOfAnalysts" in priceTarget:
            number_of_analysts = priceTarget["numberOfAnalysts"]
        if number_of_analysts != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + str(number_of_analysts)
        currency = None
        if "currency" in priceTarget:
            currency = priceTarget["currency"]
        if currency != None:
            if len(sql) > 0:
                sql = sql + ", "
            sql = sql + "'" + currency + "'"

        sql = "INSERT IGNORE INTO `investment_management`.`price_target` (`stock_symbol`,`update_date`,`price_target_average`,`price_target_high`,`price_target_low`,`number_of_analysts`,`currency`)" + \
            "VALUES(" + sql + ")"
        try:
            cur.execute(sql)
        except pymysql.err.IntegrityError as ignor:
            if ignor.args[0] != 1062:
                print("class=InvestmentManagerDB , action=addPriceTarget , state=Exception , symbol=" + symbol + " , sql=" + sql + " , msg=" + str(ignor))

    def updateStockDailyPrice(self, stockData):
        print("action=updateStockDailyPrice , state=start , stockCount=" + str(len(stockData)))
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        failedData = []
        for data in stockData:
            try:
                self.execUpdateStockDailyPrice(cur, data)
            except Exception as e:
                print(e)
                
                retryCnt = 0
                if "retryCnt" in data:
                    retryCnt = data["retryCnt"]
                retryCnt += 1
                if retryCnt > 3:
                    print("Removing Data cannot process without errors")
                else:
                    data["retryCnt"] = retryCnt
                    failedData.append(data)
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("action=updateStockDailyPrice , state=completed , stockCount=" + str(len(stockData)) + " , errorCount=" + str(len(failedData)) + " , duration=" + str((endTime - startTime)/1000))
        return failedData

    def execUpdateStockDailyPrice(self, cur, data):
        if "symbol" in data:
            symbol = data["symbol"]
            if "quotes" in data:
                records = []
                quotes = data["quotes"]
                for quote in quotes:
                    records.append(self.addQuote(symbol=symbol, price=quote))
                        
                sql = "INSERT IGNORE INTO `investment_management`.`daily_prices` " + \
                      "(`symbol`,`price_date`,`day_open`,`day_close`,`day_low`,`day_high`,`volume`,`unadj_open`,`unadj_close`,`unadj_low`,`unadj_high`,`unadj_volume`,`change`,`change_percent`,`change_over_time`) " + \
                      "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cur.executemany(sql, records)

    def addQuote(self, symbol, price):
        sql = (symbol, price["date"],
               price["open"], price["close"], price["low"], price["high"], price["volume"],
               price["uOpen"], price["uClose"], price["uLow"], price["uHigh"], price["uVolume"],
               price["change"], price["changePercent"], price["changeOverTime"])
        return sql

    def updateStockStudy(self, studies):
        print("action=updateStockStudy , state=start , stockCount=" + str(len(studies)))
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        records = []
        for data in studies:
            try:
                #self.addUpdateStudy(cur, data)
                #records.append(data)
                record = self.addStudy(study=data)
                #self.deleteStudy(cur=cur, study=record)
                records.append(record)
            except Exception as e:
                print("action=updateStockStudy , state=exception , typeOdData=" + str(type(data)) + " , error=" + str(e))

        cur.execute("commit")        
        
        sql = "INSERT IGNORE INTO `investment_management`.`daily_price_studies` (`price_date`,`symbol`,`study_id`, " + \
                                                                                "`primary_label`,`primary_days`,`primary_value`, " + \
                                                                                "`secondary_label`,`secondary_days`,`secondary_value`," + \
                                                                                "`third_label`,`third_days`,`third_value`) " + \
                                                                                "VALUES(%s,%s,%s," + \
                                                                                       "%s,%s,%s," + \
                                                                                       "%s,%s,%s," + \
                                                                                       "%s,%s,%s)"
        cur.executemany(sql, records)
        
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("action=updateStockStudy , state=completed , records=" + str(len(records)) + " , duration=" + str((endTime - startTime)/1000))

    def addStudy(self, study):
        sql = (study["priceDate"], study["symbol"], study["studyId"],
               study["primaryLabel"], study["primaryDays"], study["primaryValue"],
               study["secondaryLabel"], study["secondaryDays"], study["secondaryValue"],
               study["thirdLabel"], study["thirdDays"], study["thirdValue"])
        return sql


    def deleteStudy(self, cur, study):
        sql = "delete from investment_management.daily_price_studies " + \
              "where price_date = '" + study[0] + "' " + \
              "and symbol = '" + study[1] + "' " + \
              "and study_id = '" + study[2] + "'" + \
              "and primary_days = " + str(study[4]) + ";"
        cur.execute(sql)

    def addUpdateStudy(self, cur, study):
        try:
            self.insertStudies(cur, study)
        except Exception as ignor:
            print(ignor)
            self.updateStudy(cur, study)

    def updateStudy(self, cur, study):
        sql = "UPDATE investment_management.daily_price_studies SET " + \
              "`primary_label` = '" + study["primaryLabel"] + "', " + \
              "`primary_value` = " + str(study["primaryValue"]) + " "
        if study["secondaryLabel"] != None:
            sql += ", `secondary_label` = '" + study["secondaryLabel"] + "', " + \
                   "`secondary_days` = " + str(study["secondaryDays"]) + " "
            if study["secondaryValue"] != None:
                sql += ", `secondary_value` = " + str(study["secondaryValue"]) + " "
            else:
                sql += ", `secondary_value` = null "
            if study["thirdLabel"] != None:
                sql += ", `third_label` = '" + study["thirdLabel"] + "', " + \
                    "`third_days` = " + str(study["thirdDays"]) + " "
                if study["thirdValue"] != None:
                    sql += ", `third_value` = " + str(study["thirdValue"]) + " "
                else:
                    sql += ", `third_value` = null "
            else:
                sql += ", `third_label` = null, " + \
                    "`third_days` = null, " + \
                    "`third_value` = null "
        else:
            sql += ", `secondary_value` = null, `secondary_days`= null, `secondary_days` = null, " + \
                   " `third_label` = null, `third_days` = null, `third_value` = null "

        sql += "WHERE price_date = '" + str(study["priceDate"]) + "' " + \
               "AND symbol = '" + study["symbol"] + "' " + \
               "AND study_id = '" + study["studyId"] + "' " + \
               "AND primary_days = " + str(study["primaryDays"])
        cur.execute(sql)
        
    def insertStudies(self, cur, study):
        sql = "INSERT INTO investment_management.daily_price_studies (`price_date`,`symbol`,`study_id`, " + \
                                                                     "`primary_label`,`primary_days`,`primary_value`, " + \
                                                                     "`secondary_label`,`secondary_days`,`secondary_value`," + \
                                                                     "`third_label`,`third_days`,`third_value`) " + \
                                                                     "values('" + str(study["priceDate"]) + "','" + study["symbol"] + "','" + study["studyId"] + "'," + \
                                                                     "'" + study["primaryLabel"] + "'," + str(study["primaryDays"]) + "," + str(study["primaryValue"]) + ","
        if study["secondaryLabel"] == None:
             sql += "NULL, NULL, NULL, NULL, NULL, NULL)"
        else:
            sql +=  "'" + study["secondaryLabel"] + "'," + str(study["secondaryDays"])
            if study["secondaryValue"] != None:
                sql += "," + str(study["secondaryValue"]) + ","
            else:
                sql += ",NULL,"
            if study["thirdLabel"] == None:
                sql += "NULL, NULL, NULL)"
            else:
                sql += "'" + study["thirdLabel"] + "'," + str(study["thirdDays"]) + "," + str(study["thirdValue"]) + ")"
        cur.execute(sql)
        

    def getIndexRecordsQuery(self, maxReturn=100, lastSymbol=None):
        print("class=InvestmentManagerDB , action=getIndexRecordsQuery , state=start , maxReturn=" +
              str(maxReturn) + " , lastSymbol=" + str(lastSymbol))
        startTime = int(time.time() * 1000)
        try:
            companies = self.executeIndexRecordsQuery(
                self.getReaderDBConnect(), maxReturn, lastSymbol)
        except Exception as e:
            print("class=InvestmentManagerDB , action=getIndexRecordsQuery , state=Exception , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , msg=" +
                  str(e))
            
            raise e

        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=getIndexRecordsQuery , state=completed , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , companyCnt=" + str(len(companies)) + " , duration=" + str((endTime - startTime)))
        return companies

    def getCompanies(self, maxReturn=100, lastSymbol=None):
        print("class=InvestmentManagerDB , action=getCompanies , state=start , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol))
        startTime = int(time.time() * 1000)
        try:
            companies = self.executeGetCompanyQuery(
                self.getReaderDBConnect(), maxReturn, lastSymbol)
        except Exception as e:
            print("class=InvestmentManagerDB , action=getCompanies , state=Exception , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , msg=" + str(e))
            raise e

        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=getCompanies , state=completed , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , companyCnt=" + str(len(companies)) + " , duration=" + str((endTime - startTime)))
        return companies

    def getCompany(self, symbol):
        sql = "SELECT stock_symbol as symbol, company_name as companyName FROM investment_management.stocks WHERE stock_symbol = '" + symbol + "'"
        try:
            df = pd.read_sql(sql, self.getReaderDBConnect())
        except Exception as e:
            print("class=InvestmentManagerDB , action=getCompany , state=Exception , symbol=" + str(symbol) + " , sql=" + sql + " , msg=" + str(e))
            raise e
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def getNewCompanies(self):
        try:
            companies = self.executeNewRecordsQuery(self.getReaderDBConnect())
        except Exception as e:
            print("class=InvestmentManagerDB , action=getNewCompanies , state=Exception , msg=" + str(e))
            
            raise e

        return companies

    def getCompaniesBetween(self, fromSymbol, toSymbol):
        print("class=InvestmentManagerDB , action=getCompanies , state=start , fromSymbol=" + str(fromSymbol) + " , toSymbol=" + str(toSymbol))
        startTime = int(time.time() * 1000)
        try:
            companies = self.executeGetCompanyBetween(self.getReaderDBConnect(), fromSymbol, toSymbol)
        except Exception as e:
            print("class=InvestmentManagerDB , action=getCompanies , state=Exception , fromSymbol=" + str(fromSymbol) + " , lastSymbol=" + str(toSymbol) + " , msg=" + str(e))
            raise e

        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=getCompanies , state=completed , fromSymbol=" + str(fromSymbol) + " , lastSymbol=" + str(toSymbol) + " , companyCnt=" + str(len(companies)) + " , duration=" + str((endTime - startTime)))
        return companies

    def executeGetCompanyBetween(self, conn, fromSymbol, toSymbol):
        sql = "SELECT stock_symbol as symbol, company_name as companyName FROM stocks " + \
            "WHERE (is_iex_enabled = true or is_iex_enabled is null) "
        if fromSymbol != None:
            sql += "and stock_symbol >= '" + fromSymbol + "' "
        if toSymbol != None:
            sql += "and stock_symbol <= '" + toSymbol + "' "
        sql += "ORDER BY stock_symbol ASC"
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def getAllCompanies(self):
        print("class=InvestmentManagerDB , action=getCompanies , state=start")
        startTime = int(time.time() * 1000)
        try:
            companies = self.executeGetAllCompany(self.getReaderDBConnect())
        except Exception as e:
            print("class=InvestmentManagerDB , action=getCompanies , state=Exception , msg=" + str(e))
            raise e

        endTime = int(time.time() * 1000)
        print("class=InvestmentManagerDB , action=getCompanies , state=completed , companyCnt=" + str(len(companies)) + " , duration=" + str((endTime - startTime)))
        return companies

    def executeGetAllCompany(self, conn):
        sql = "SELECT stock_symbol as symbol, company_name as companyName FROM stocks " + \
            "WHERE (is_iex_enabled = true or is_iex_enabled is null) " + \
            "ORDER BY stock_symbol ASC"
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def executeGetCompanyQuery(self, conn, maxReturn=100, lastSymbolReturned=None):
        sql = "SELECT stock_symbol as symbol, company_name as companyName FROM stocks " + \
            "WHERE (is_iex_enabled = true or is_iex_enabled is null) "
        if lastSymbolReturned != None:
            sql = sql + " and stock_symbol > '" + lastSymbolReturned + "' "
        sql = sql + " ORDER BY stock_symbol ASC LIMIT " + str(maxReturn)
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def getCompanyMissingHistory(self, maxReturn=100, lastSymbol=None):
        print("class=getCompanyMissingHistory , action=getCompanies , state=start , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol))
        startTime = int(time.time() * 1000)
        try:
            companies = self.executeGetCompanyMissingHistoryQuery(
                self.getReaderDBConnect(), maxReturn, lastSymbol)
        except Exception as e:
            print("class=getCompanyMissingHistory , action=getCompanies , state=Exception , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , msg=" + str(e))
            raise e

        endTime = int(time.time() * 1000)
        print("class=getCompanyMissingHistory , action=getCompanies , state=completed , maxReturn=" + str(maxReturn) + " , lastSymbol=" + str(lastSymbol) + " , companyCnt=" + str(len(companies)) + " , duration=" + str((endTime - startTime)))
        return companies

    def executeGetCompanyMissingHistoryQuery(self, conn, maxReturn=100, lastSymbolReturned=None):
        sql = "SELECT stock_symbol as symbol, company_name as companyName FROM stocks " + \
            "WHERE (is_iex_enabled = true or is_iex_enabled is null) " + \
            "and stock_symbol not in (SELECT DISTINCT symbol FROM daily_prices) "
        if lastSymbolReturned != None:
            sql = sql + " and stock_symbol > '" + lastSymbolReturned + "' "
        sql = sql + " ORDER BY stock_symbol ASC LIMIT " + str(maxReturn)
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)


    def executeNewRecordsQuery(self, conn):
        sql = "SELECT stock_symbol as symbol FROM stocks WHERE company_name is NULL"
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def executeIndexRecordsQuery(self, conn, maxReturn=100, lastSymbolReturned=None):
        sql = "SELECT distinct stock_symbol as symbol FROM index_list "
        if lastSymbolReturned != None:
            sql = sql + "WHERE stock_symbol > '" + lastSymbolReturned + "' "
        sql = sql + " ORDER BY stock_symbol ASC LIMIT " + str(maxReturn)
        df = pd.read_sql(sql, conn)
        jsonStr = df.to_json(orient='records')
        return json.loads(jsonStr)

    def updateAIXSymbol(self, symbols):
        cur = self.getDBConnect().cursor()
        for symbol in symbols:
            self.insertSymbol(cur, symbol)
        cur.execute("commit")
        cur.close()

    def isSymbolTrackable(self, symbol):
        # This is to determine if the stock is still be tracked
        # if we have a EOD price within the last 14 days then
        # we need to keep tracking it
        sql = "select count(*) as value from daily_prices WHERE symbol  = '" + symbol + "' and price_date > date_add(now(), interval -14 DAY)"
        df = pd.read_sql(sql, self.getReaderDBConnect())
        cnt = df['value'].iloc(0)
        return cnt[0] > 0


    def disableAIXSymbol(self, symbol):
        cur = self.getDBConnect().cursor()
        sql = "UPDATE stocks SET is_iex_enabled = False WHERE stock_symbol = '" + symbol + "'"
        cur.execute(sql)
        cur.execute("commit")
        cur.close()


    def insertSymbol(self, cur, symbol):
        sql = "insert into stocks (stock_symbol, symbol_type, `exchange`, company_name, reference_date, is_iex_enabled, issue_type, region, company_id) VALUES("
        sql += "'" + symbol['symbol'] + "', 'STOCK' "
        exchange = None
        if 'exchange' in symbol:
            exchange = symbol['exchange']
        if exchange != None:
            if len(sql) > 0:
                sql += ", "
            sql += "'" + exchange + "'"
        else:
            sql += ",NULL"
        name = None
        if 'name' in symbol:
            name = self.remove_non_sql_chars(symbol['name'])
        if name != None:
            if len(sql) > 0:
                sql += ", "
            sql += "'" + name + "'"
        else:
            sql += ",NULL"
        referenceDate = None
        if 'date' in symbol:
            referenceDate = symbol['date']
        if referenceDate != None:
            if len(sql) > 0:
                sql += ", "
            sql += "'" + referenceDate + "'"
        else:
            sql += ",NULL"
        isEnabled = None
        if 'isEnabled' in symbol:
            isEnabled = symbol['isEnabled']
        if isEnabled != None:
            if len(sql) > 0:
                sql += ", "
            if isEnabled:
                sql += "1"
            else:
                sql += "0"
        else:
            sql += ",1"
        companyType = None
        if 'type' in symbol:
            companyType = symbol['type']
        if companyType != None:
            if len(sql) > 0:
                sql += ", "
            sql += "'" + companyType + "'"
        else:
            sql += ",NULL"
        region = None
        if 'region' in symbol:
            region = symbol['region']
        if region != None:
            if len(sql) > 0:
                sql += ", "
            sql += "'" + region + "'"
        else:
            sql += ",NULL"
        cik = None
        if 'cik' in symbol:
            cik = symbol['cik']
        if cik != None:
            if len(sql) > 0:
                sql += ", "
            sql += str(cik)
        else:
            sql += ",NULL"
        sql += ")"
        try:
            print(sql)
            if cik != None:
                self.updateUFCompany(cur, cik, name)
            cur.execute(sql)
        except Exception as e:
            print(e)
            self.updateSymbol(cur=cur, symbol=symbol)



    def updateSymbol(self, cur, symbol):
        update = ""
        ticker = None
        if 'symbol' in symbol:
            ticker = symbol['symbol']
        exchange = None
        if 'exchange' in symbol:
            exchange = symbol['exchange']
        if exchange != None:
            if len(update) > 0:
                update += ", "
            update += "`exchange` = '" + exchange + "'"
        name = None
        if 'name' in symbol:
            name = self.remove_non_sql_chars(symbol['name'])
        if name != None:
            if len(update) > 0:
                update += ", "
            update += "`company_name` = '" + name + "'"
        referenceDate = None
        if 'date' in symbol:
            referenceDate = symbol['date']
        if referenceDate != None:
            if len(update) > 0:
                update += ", "
            update += "`reference_date` = '" + referenceDate + "'"
        isEnabled = None
        if 'isEnabled' in symbol:
            isEnabled = symbol['isEnabled']
        if isEnabled != None:
            if len(update) > 0:
                update += ", "
            if isEnabled:
                update += "`is_iex_enabled` = 1"
            else:
                update += "`is_iex_enabled` = 0"
        companyType = None
        if 'type' in symbol:
            companyType = symbol['type']
        if companyType != None:
            if len(update) > 0:
                update += ", "
            update += "`issue_type` = '" + companyType + "'"
        region = None
        if 'region' in symbol:
            region = symbol['region']
        if region != None:
            if len(update) > 0:
                update += ", "
            update += "`region` = '" + region + "'"
        cik = None
        if 'cik' in symbol:
            cik = symbol['cik']
        if cik != None:
            if len(update) > 0:
                update += ", "
            update += "`company_id` = " + str(cik)
            sql = "UPDATE stocks SET "
        sql = "UPDATE stocks SET " + update + " WHERE stock_symbol = '" + ticker + "'"
        try:
            print(sql)
            if cik != None:
                self.updateUFCompany(cur, cik, name)
            cur.execute(sql)
        except Exception as e:
            print(e)

    def updateUFCompany(self, cur, cik, companyName):
        sql = "INSERT IGNORE INTO `investment_management`.`usf_companies` (`company_id`, `name`) VALUES(" + str(
            cik) + ", '" + companyName + "')"
        print(sql)
        cur.execute(sql)

    def runThreads(self, threads, maxThreadCount=5):
        cnt = 0
        runningThreads = []
        for thread in threads:
            thread.start()
            runningThreads.append(thread)
            cnt = cnt + 1
            if cnt >= maxThreadCount:
                # Waite until these threads complete
                for running in runningThreads:
                    running.join()
                cnt = 0
                runningThreads = []
        # Waite until final threads complete
        for running in runningThreads:
            running.join()

    def updatePriceHistory(self, priceData, maxThreadCount=5):
        print("action=updatePriceHistory , state=start , stockCount=" +
            str(len(priceData)) + " , maxThreadCount=" + str(maxThreadCount))
        startTime = int(time.time() * 1000)
        threads = []
        nbrOfPrices = len(priceData)
        nbrOfPricesPerThread = math.ceil(nbrOfPrices/maxThreadCount)
        # Break Price Data into Chuncks
        threadData = []
        firstSymbol = None
        lastSymbol = None
        threadName = None
        i = 0
        for data in priceData:
            if firstSymbol == None:
                firstSymbol = data['symbol']
            lastSymbol = data['symbol']
            threadName = firstSymbol + "-" + lastSymbol
            if len(threadData) == nbrOfPricesPerThread:
                runner = PriceHistoryThread(awsConfig=self.awsConfig, threadID=i, name=threadName, request=threadData)
                threads.append(runner)
                i += 1
                threadData = []
                firstSymbol = None
            threadData.append(data)

        runner = PriceHistoryThread(awsConfig=self.awsConfig, threadID=i, name=threadName, request=threadData)
        threads.append(runner)
        self.runThreads(threads, maxThreadCount)
        allFailedData = []
        for runner in threads:
            allFailedData += runner.getFailedData()
        endTime = int(time.time() * 1000)
        print("action=updatePriceHistory , state=completed , stockCount=" +
            str(len(priceData)) + " , maxThreadCount=" + str(maxThreadCount) + " , errorCount=" + str(len(allFailedData)) + " , duration=" + str((endTime - startTime)/1000))
        return allFailedData
    
    def updateDailyPrice(self, dailyPrices):
        print("action=updateDailyPrice , state=start")
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        failedData = []
        records = []
        for price in dailyPrices:
            priceQuotes = price['priceQuotes']
            for data in priceQuotes:
                try:
                    record = self.getStockDailyPrice(data)
                    records = records + record
                except Exception as e:
                    print(e)
                    retryCnt = 0
                    if "retryCnt" in data:
                        retryCnt = data["retryCnt"]
                    retryCnt += 1
                    if retryCnt > 3:
                        print("Removing Data cannot process without errors")
                    else:
                        data["retryCnt"] = retryCnt
                        failedData.append(data)
        sql = "INSERT IGNORE INTO `investment_management`.`daily_prices` (`symbol`,`price_date`,`day_open`,`day_close`,`day_low`,`day_high`,`volume`,`unadj_open`,`unadj_close`,`unadj_low`,`unadj_high`,`unadj_volume`,`change`,`change_percent`,`change_over_time`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(sql, records)
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("action=updateDailyPrice , state=completed , stockCount=" +
            str(len(dailyPrices)) + " , errorCount=" + str(len(failedData)) + " , duration=" + str((endTime - startTime)/1000))
        return failedData, records
       
            
    """
    Method extracts the IEXCloud quote structure for the payload and inserts it into the daily_prices table

    Json Structure:
        { "symbol": "Stock Symbol",
        "quotes": [{quote json structure see IEXCloud documentation}]
    """
    def getStockDailyPrice(self, data):
        records = []
        print("action=execUpdateStockDailyPrice , state=start")
        if "symbol" in data:
            symbol = data["symbol"]
            if "quotes" in data:
                quotes = data["quotes"]
                print("action=execUpdateStockDailyPrice , state=processing  , symbol=" +
                    symbol + " , nbrOfQuotes=" + str(len(quotes)))
                for quote in quotes:
                    try:
                        records.append(self.addQuote(symbol=symbol, price=quote))
                    except Exception as e:
                        print(str(e))

        return records

    def updateTargetPrice(self, records):
        print("action=updateTargetPrice , state=start , targetPrice=" + str(len(records)))
        startTime = int(time.time() * 1000)
        cur = self.getDBConnect().cursor()
        failedData = []
        for record in records:
            priceTargets = record['priceTargets']
            records = []
            for data in priceTargets:
                try:
                    print(data)
                    record = self.AddPriceTargetRecord(data)
                    print(record)
                    records.append(record)
                except Exception as e:
                    print("action=updateTargetPrice , state=exception , msg=" + str(e))
                    print(data)
                    retryCnt = 0
                    if "retryCnt" in data:
                        retryCnt = data["retryCnt"]
                    retryCnt += 1
                    if retryCnt > 3:
                        print("Removing Data cannot process without errors")
                    else:
                        data["retryCnt"] = retryCnt
                        failedData.append(data)
            sql = "INSERT ignore INTO `investment_management`.`price_target` " + \
            "(stock_symbol, update_date,price_target_average,price_target_high,price_target_low,number_of_analysts,currency)" + \
            "VALUES(%s, %s, %s, %s, %s, %s, %s)"
            cur.executemany(sql, records)
        cur.execute("commit")
        cur.close()
        endTime = int(time.time() * 1000)
        print("action=updateTargetPrice , state=completed , records=" +
            str(len(records)) + " , errorCount=" + str(len(failedData)) + " , duration=" + str((endTime - startTime)/1000))
        return failedData

    def AddPriceTargetRecord(self, price):
        record = (price['symbol'], price["updatedDate"],
                price["priceTargetAverage"], price["priceTargetHigh"], price["priceTargetLow"], 
                price["numberOfAnalysts"], price["currency"])
        return record

    def addPreviousEOD(self, quotes):
        records = []
        for quote in quotes:
            records.append(self.addEOD(quote))
        sql = "INSERT IGNORE INTO `investment_management`.`daily_prices` (`symbol`,`price_date`,`day_open`,`day_close`,`day_low`,`day_high`,`volume`,`unadj_open`,`unadj_close`,`unadj_low`,`unadj_high`,`unadj_volume`,`change`,`change_percent`,`change_over_time`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cur = self.getDBConnect().cursor()
        cur.executemany(sql, records)
        cur.execute("commit")
        cur.close()

    def addEOD(self, price):
        sql = (price['symbol'], price["date"],
               price["open"], price["close"], price["low"], price["high"], price["volume"],
               price["uOpen"], price["uClose"], price["uLow"], price["uHigh"], price["uVolume"],
               price["change"], price["changePercent"], price["changeOverTime"])
        return sql

    def addTargetPrice(self, prices):
        records = []
        for price in prices:
            records.append(self.AddPriceTargetRecord(price))
        sql = "INSERT ignore INTO `investment_management`.`price_target` " + \
            "(stock_symbol, update_date,price_target_average,price_target_high,price_target_low,number_of_analysts,currency)" + \
            "VALUES(%s, %s, %s, %s, %s, %s, %s)"
        cur = self.getDBConnect().cursor()
        cur.executemany(sql, records)
        cur.execute("commit")
        cur.close()
