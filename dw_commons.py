import django
import os
from collections import OrderedDict
import sys
#sys.path.insert(0, '/apollo/env/AWSDBMetrics')
#from helpers import *
from collections import namedtuple
from decimal import Decimal
import datetime
import time
from datetime import date
from datetime import timedelta
from db_services_metrics import helpers

def main():

    print('########## START DYNAMODB DAILY AT: ', datetime.datetime.now(),' ##########')
    startdate = datetime.datetime.strptime('2016-01-21', "%Y-%m-%d")
    #startdate = datetime.datetime(2013,6,10)
    startdate_display = startdate.strftime("%Y-%m-%d")
    service_name = 'dynamodb'
    k = get_service_goals_dbsm(startdate_display,service_name)
    print(k[0]["revenue_goal"])
    
def get_service_goals_dbsm(startdate,service_name):
    print('Getting DDB Data from Datamart')
    sql = get_service_goal_sql()
    print(sql)
    goal_results = helpers.get_cookie_data(sql, {"startdate": startdate,"service_name": service_name})
    return goal_results    


def get_service_goal_sql():

    sql = """ select
              revenue_goal
              FROM dbsm_monthly_goal_dm 
              where date_value = last_day(%(startdate)s) 
              and engine = %(service_name)s
              and rec_active_flag = 'Y' 
          """
    return sql   

    
if __name__ == "__main__":
    main()


