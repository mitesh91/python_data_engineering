import django
from django.http import HttpResponse
import datetime
from db_services_metrics import helpers
from datetime import timedelta
import argparse
from django.utils.html import strip_tags
from django.template.loader import get_template
from django.conf import settings
from django.template import Context, loader
import sys, logging, os, getopt, ast, csv, tempfile, gzip
from string import Template
from django.template.defaulttags import register
import json
from sets import Set
import xlwt
from xlwt import *
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)

@register.filter
def get_list_item(list,idx):
    return list[idx]

@register.filter
def to_titlecase(value):
    return value.replace("_"," ").title().replace('.', '')

@register.filter
def to_uppercase(value):
    return str(value).replace("_"," ").upper().replace('.', '')

@register.filter
def to_lowercase(value):
    return str(value).replace("_"," ").lower().replace('.', '')

@register.filter
def to_currency(value):
    return '${:,.0f}'.format(value)

def main():
    logging.info('########## START WEEKLY METRIC REVIEW REPORT ############# ')
    args = arg_parser()
    startdate = datetime.datetime.strptime(args.startdate, "%Y-%m-%d")
    startdate_display = startdate.strftime("%Y-%m-%d")
    export_xls(startdate, startdate_display, args.product,  args.jsonfilename, args.variables)
    logging.info(datetime.datetime.now())
    logging.info('\n########## WEEKLY METRIC REVIEW REPORT FINISHED SUCCESSFULLY #############')

def arg_parser():

    parser = argparse.ArgumentParser(description="Send metric emails.")

    parser.add_argument('-e', action="append", dest='emails', default=[])
    parser.add_argument('-d', action="store", dest='startdate')
    parser.add_argument('-p', action="store", dest='product')
    parser.add_argument('-n', action="store", dest='reportname')
    parser.add_argument('-f', action="store", dest='jsonfilename')
    parser.add_argument('-c', action="store", dest='database')
    parser.add_argument('-v', action="store", dest='variables')

    args = parser.parse_args()

    if not args.emails:
        args.emails = ['dbsmetricsgenerator@amazon.com']
    if args.startdate is None:
        startdate = datetime.datetime.now()
        if startdate.hour < 6:
            startdate += -timedelta(days=2)
        else:
            startdate += -timedelta(days=1)
        args.startdate = startdate.strftime("%Y-%m-%d")
    if not args.reportname:
        args.reportname = 'Default'
    if not args.variables:
        args.variables = "{}"
    if not args.jsonfilename:
	assert False, "unhandled option"
    if not args.product:
        assert False, "unhandled option"

    global_config = helpers.getGlobalOverrideConfig()

    run_mode = global_config.get('run-mode', 'REGULAR')

    if run_mode == 'MAINTENANCE':
        logging.info("Running in the maintenance mode: reports configuration will be overwritten")
        args.emails = global_config.get('email-list', ['dbsmetricsgenerator@amazon.com'])
    else:
        logging.info("Global configuration processed. Proceeding with run mode [{run_mode}]".format(run_mode = run_mode))

    logging.info('Running with arguments: [{args}]'.format(args = args))
    return args

######################################################################
# get the sql query givent e query opath in s3
#####################################################################

def getSqlQuery(dsnName,s3path):
    return helpers.getS3FileString(s3path, dsnName, 'dbs-wbr-reporter')

##################################################################################
# get the json string that contains the sql files
#################################################################################

def getJSONString(file_name):
    return helpers.getS3FileString(file_name,'DBS.METRICS.ETL.S3.PRD.DAILY','dbs-wbr-reporter')

####################################################################################
# parse the s3 path for sql file
####################################################################################

def parseJSONString(json_str):

    sql_s3_path = {}
    sql_section_heading = {}
    sql_database = {}
    sql_pivot = {}
    table_border_present = {}
    sql_transform = {}
    transform_rows = {}
    transform_cols = {}
    transform_datum = {}
    template_features = {}
    formatter = {}

    s3_json_dict = json.loads(json_str)

    for data in s3_json_dict["file"]:
        sql_s3_path[ "seq" + str(data["sectional-order"]) ] = data["s3-sql-path"]
        sql_section_heading[ "seq"+ str(data["sectional-order"]) ] = data["sectional-header"]
        sql_database["seq"+ str(data["sectional-order"])] = data.get("database",'datamart')
        sql_pivot["seq"+ str(data["sectional-order"])] = data.get("pivot",'no')
        table_border_present["seq"+ str(data["sectional-order"])] = data.get("border",'no')

        # new section for transformation
        sql_transform["seq"+ str(data["sectional-order"])] =  'yes' if data.get("transform",'no') !='no' else 'no'
        transform = data.get("transform",{})
        transform_rows["seq"+ str(data["sectional-order"])] = transform.get("rows",[])
        transform_cols["seq"+ str(data["sectional-order"])] = transform.get("cols",[])
        transform_datum["seq"+ str(data["sectional-order"])]= transform.get("datum",{})

        #formatter
        formatter["seq"+ str(data["sectional-order"])] = data.get("format",{})

    template_features["column_case"] = s3_json_dict.get("column-case","title")

    logging.info(sql_s3_path)
    return sql_s3_path,sql_section_heading,sql_database,sql_pivot,table_border_present,sql_transform,transform_rows,transform_cols,transform_datum,template_features,formatter

##############################################################################

# upload to archieve the reports
##########################################################################
def archieve(htmlcontent,startdate_display,service_name):
    uploadBucketName = 'dbsm-daily-reports'
    dsnName = 'DBS.METRICS.ETL.S3.PRD.DAILY'
    helpers.uploadToS3(startdate_display,dsnName,uploadBucketName,service_name,htmlcontent)

################### End of archieve #################

#######################################################################################
# format data
######################################################################################

def format_output(sql_output,formatter):

    for _idx, _tdata in enumerate(sql_output):
        for k,v in _tdata.items():
            if k in formatter.keys():
                 sql_output[_idx][k] = format(v,formatter[k])
    return  sql_output

#################################################
# adding formats
#################################################
def format(data, format_type):
    try:
        data = format_type.format(data)
    except ValueError:
        print("Could not format the data as format currency needs decimal.")
    except:
        print("Unexpected error:", sys.exc_info()[0])

    return data

########################################################
# title headers
###########################################################
def title_header(headers):
    title_headers = []
    for header in headers:
        title_headers.append(header.replace('_' ,' ').title())
    return title_headers
#############################################
# title string
########################################
def title_string(str):
    return str.replace('_' ,'').title()

####################################################
# substitute variables in sql
####################################################
def substitute_variables(sql,variables):

    substituted_sql = sql

    try:
        variable_map = json.loads(variables)
    except:
        logging.error("Unexpected error:", sys.exc_info()[0])
        raise
    try:
        for key,val in variable_map.iteritems():
            subs_key = "{"+str(key) + "}"
            sql = sql.replace(subs_key,str(val))
    except:
        logging.info("not able to substitute values")

    substituted_sql =sql
    return substituted_sql

###  Email and Reporting ###
def get_percentages_increase(numerator,denominator,substitute = None):

    if substitute is None:
        substitute = '-'

    if denominator != 0:
        percentage = 100*(numerator-denominator)/denominator
        percentage = str(round(percentage,2))+ "%"
    else:
        percentage = substitute

    return percentage

##############################################################################
# form the excel sheet content
##############################################################################
def export_xls(startdate, startdate_display, product_code, file_name, variable):
    logging.info("Running the Weekly report for: {date}".format(date = startdate_display))

    _dir = '/tmp/WBR/{}'.format(product_code)

    if not os.path.exists(_dir):
        os.makedirs(_dir)
    print ("Creating directory : "+(_dir))

    file = '/tmp/WBR/{}/{}.xls'.format(product_code, startdate_display)

    print ("Contents will be written in " +(file))

    wb = xlwt.Workbook(encoding='utf-8')
    ws = wb.add_sheet("Main_sheet")

    styles = {'datetime': xlwt.easyxf(num_format_str='yyyy-mm-dd hh:mm:ss'),
                'date': xlwt.easyxf(num_format_str='yyyy-mm-dd'),
                'time': xlwt.easyxf(num_format_str='hh:mm:ss'),
                'header': xlwt.easyxf('font: name Times New Roman, color-index red, bold on', num_format_str='#,##0.00'),
                'default': xlwt.Style.default_style}

    ws.write(0, 0, product_code, xlwt.easyxf('font: name Times New Roman, bold on, height 320', num_format_str='#,##0.00'))

    json_str = getJSONString(file_name)

    sql_paths,sectional_headings,sql_database,sql_pivot,table_border,sql_transform,transform_rows,transform_cols,transform_datum,template_features,formatter = parseJSONString(json_str)

    count_sql_requests = len(sql_paths)
    sql_list = range(0,count_sql_requests)

    row, col = 2, 0
    for idx in sql_list:

        sql_s3_path = sql_paths["seq" + str(idx+1)]
        database = sql_database["seq" + str(idx+1)]

        sql = getSqlQuery('DBS.METRICS.ETL.S3.PRD.DAILY',sql_s3_path)
        sql = substitute_variables(sql,variable)

        logging.info("Substituted SQL...........")
        logging.info(sql)

        sql_output = get_data(database,sql,startdate_display)
	#print(sql_output)
	#print(len(sql_output))

	#####################################################
        grouped_key_dict = defaultdict(list)
        for weeks in sql_output:
            for key, value in sorted(weeks.items()):
                grouped_key_dict[key].append(value)
	print("\n")
        print(grouped_key_dict)
	print("\n")
	#row, col = 2, 0
        for key, values in grouped_key_dict.items():
            #print(row, col)
            #print(key)
	    row +=1
	    col = 0
            ws.write(row, col, key, xlwt.easyxf('font: name Times New Roman, bold on, height 320', num_format_str='#,##0.00'))
            for value in values:
                #print(value)
                if isinstance(value, datetime.datetime):
                    cell_style = styles['datetime']
                elif isinstance(value, datetime.date):
                    cell_style = styles['date']
                elif isinstance(value, datetime.time):
                    cell_style = styles['time']
                else:
                    cell_style = styles['default']
                col +=1
                ws.write(row, col, value, cell_style)
                #print(row, col)
        row +=1
        col = 0


    wb.save(file)
    logging.info('end time : {time}'.format(time = datetime.datetime.now()))

#export_xls.short_description = u"Export XLS"
###################################################################

def get_data(database,sql,startdate):
    result = helpers.get_data(database,sql,{'startdate' : startdate})
    return result

if __name__ == "__main__":
    main()
