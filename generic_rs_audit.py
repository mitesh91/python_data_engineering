import django
import datetime
from db_services_metrics import helpers
from datetime import timedelta
import argparse
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.template.loader import get_template
from django.conf import settings
from django.template import Context, loader
import sys,  logging, os, getopt, ast, csv, tempfile, gzip
from string import Template
from django.template.defaulttags import register
import json
from sets import Set
import S3handle

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

@register.filter
def to_str(value):
    return str(value)

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

    logging.info('########## START DAILY EMAIL REPORT ############# ')
    args = arg_parser()
    startdate = datetime.datetime.strptime(args.startdate, "%Y-%m-%d")
    startdate_display = startdate.strftime("%Y-%m-%d")
    daily_email(args, startdate, startdate_display,args.reportname,args.filename)  
    logging.info(datetime.datetime.now())
    logging.info('\n##########DAILY EMAIL REPORT FINISHED SUCCESSFULLY #############')
    #publish to s3 confidence fikle
    #publish to s3 archieve

def arg_parser():

    parser = argparse.ArgumentParser(description="Send metric emails.")

    parser.add_argument('-e', action="append", dest='emails', default=[])
    parser.add_argument('-d', action="store", dest='startdate')
    parser.add_argument('-p', action="store", dest='product')
    parser.add_argument('-n', action="store", dest='reportname')
    parser.add_argument('-f', action="store", dest='filename')
    parser.add_argument('-c', action="store", dest='database')
    parser.add_argument('-v', action="store", dest='variables')
    parser.add_argument('-t', action="store", dest='template', default = 'generic_audit_table.html')
    parser.add_argument('-k', action="store", dest='confidence_filename')

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
    if not args.filename:
        assert False, "unhandled option"    
    if not (args.product or args.confidence_filename):
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
    return helpers.getS3FileString(s3path, dsnName, 'dbs-data-excavator-reporter')

##################################################################################
# get the json string that contains the sql files
#################################################################################

def getJSONString(file_name):
    return helpers.getS3FileString(file_name, 'DBS.METRICS.ETL.S3.PRD.DAILY', 'dbs-data-excavator-reporter')

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
    #audit the column data
    audit = {}

    s3_json_dict = json.loads(json_str)
    
    for data in s3_json_dict["file"]:
        sql_s3_path[ "seq" + str(data["sectional-order"]) ] = data["s3-sql-path"]
        sql_section_heading[ "seq"+ str(data["sectional-order"]) ] = data["sectional-header"]
        sql_database["seq"+ str(data["sectional-order"])] = data.get("database",'datamart')
        sql_pivot["seq"+ str(data["sectional-order"])] = data.get("pivot",'no')
        table_border_present["seq"+ str(data["sectional-order"])] = data.get("border",'no')

        # audit
        audit["seq"+ str(data["sectional-order"])] = data.get("audit",{})

    template_features["column_case"] = s3_json_dict.get("column-case","title") 

    logging.info(sql_s3_path)
    return sql_s3_path,sql_section_heading,sql_database,sql_pivot,table_border_present,template_features,audit

##############################################################################
# form the daily email
##############################################################################

def daily_email(args, startdate, startdate_display,report_name,file_name):

    logging.info("Running the Daily report for: {date}".format(date = startdate_display))
    html_params = {}    
    headers = []     
    html_params['output'] = []
    html_params['header'] = []
    html_params['section_heading'] = []
    html_params['pivot'] = []
    html_params['table_border'] = []
    html_params['variables'] = args.variables
    html_params['audit'] = []

    json_str = getJSONString(file_name)

    sql_paths,sectional_headings,sql_database,sql_pivot,table_border,template_features,audit=parseJSONString(json_str)

    count_sql_requests = len(sql_paths)
    sql_list = range(0,count_sql_requests)
    
    for idx in sql_list:
        
        sql_s3_path = sql_paths["seq" + str(idx+1)]
        database = sql_database["seq" + str(idx+1)]
        
        sql = getSqlQuery('DBS.METRICS.ETL.S3.PRD.DAILY',sql_s3_path)
        sql = substitute_variables(sql,args.variables)
        
        logging.info("Substituted SQL...........")
        logging.info(sql)
        
        sql_output = get_data(database,sql,startdate_display)


        if len(sql_output) >0:

            html_params['output'].append(sql_output) 
            #get the headers for the output    
            headers = helpers.get_sql_header(database,sql,{'startdate' :startdate_display})

            html_params['header'].append(headers)
            html_params['section_heading'].append(title_string(sectional_headings["seq" + str(idx+1)]))
            html_params['table_border'].append(table_border["seq" + str(idx+1)])
            
            # audit the results
            audit_evaluation = auditor(audit["seq" + str(idx+1)],sql_output)
            html_params['audit'].append(audit_evaluation) 
            
        else:
            count_sql_requests = count_sql_requests  - 1

    request_list = range(0,count_sql_requests)
    
    html_params['sql_list'] = request_list
    html_params['column-case'] = template_features["column_case"]

    html_content = create_html_content(startdate_display, html_params, args.template)
    
    #archieve
    archieve(html_content,startdate_display,args.product)
    
    # consolidate audit
    final_audit_result = consolidate_audit(html_params['audit'])
    print("Audit results have: " +final_audit_result)

    #upload confidence file to s3
    handle = S3handle.S3handle(args.confidence_filename)
    handle.deleteFile()
    if final_audit_result == 'passed':
        handle.postFile()

    #print html_content
    helpers.send_html_email(args.emails, html_content, report_name) 
    logging.info('end time : {time}'.format(time = datetime.datetime.now()))

##################################################################################
# consolidate audit from the queries
##################################################################################
def consolidate_audit(audit_records):
    
    audit_outcome = ""
    for records in audit_records:
        for result in records:
            if result.get('result') != 'failed' and audit_outcome in ["","passed"]:
                audit_outcome = "passed"
            else:
                audit_outcome = "failed"
                 
    return audit_outcome 
    
###################################################################################
# audit the data
####################################################################################
def auditor(audit_map,sql_output):
    
    audit_evaluation = []
    comparator = ['>','>=','<','<=','=','!=','in','not in','like','count_rows']   
    string_comparator = ['in','not in','like']
    full_data_comparator = ['count_rows']

    for condition in audit_map:
        audit_result = {}
        audit_result['condition'] = str(condition)

        #check for adherence params
        if condition.get('adherence') not in ['required','optional']:
            audit_result['result'] = 'failed'
            audit_result['reason'] = audit_result.get('reason','') + ':Incorrect parameters for adherence'
        
        #check comparators
        for operator in condition.get('operation'):
            if operator.keys()[0] not in comparator:
                audit_result['result'] = 'failed'
                audit_result['reason'] = audit_result.get('reason','') + ':Incorrect parameters for operation'
        
        # need to check that math and string operators are not applied on key at sametime
        
        if sql_output[0].get(condition['key']) == None: 
            audit_result['result'] = 'failed'
            audit_result['reason'] = audit_result.get('reason','') + ':Key not present in sql output'
        
        #full data auditor
        if condition.get('operation')[0].keys()[0] == 'count_rows':
            if len(sql_output) != condition.get('operation')[0].values()[0]:
               audit_result['result'] = 'failed'
               audit_result['reason'] = audit_result.get('reason','') + ':Row count check failed'
            else:
                audit_result['reason'] = audit_result.get('reason','') + ':Row count check passed'
        
        if audit_result.get('result') != 'failed' and condition.get('operation')[0].keys()[0] not in full_data_comparator:
            
            for elem in sql_output:
                audit_data = elem.get(condition['key'])
                evaluation_string = build_evaluation_string(audit_data,condition.get('operation'))
                
                #print(evaluation_string)

                if eval(str(evaluation_string)) == False:
                    #print( eval(str(evaluation_string)))
                    if condition.get('adherence') == 'optional':
                        audit_result['result'] = 'warning'
                        audit_result['reason'] = "Warning:"+ str(audit_data)+ " not within the threshold."
                    else:
                        audit_result['result'] = 'failed'
                        audit_result['reason'] = "Failure: "+ str(audit_data) +" not within the threshold."
                else:
                    if audit_result.get('result') not in ['warning','failed']:
                        #print(audit_result.get('result'))
                        audit_result['result'] = 'passed'
                        audit_result['reason'] = "Passed: "+ str(audit_data) + " within the threshold."
            
        #finally append the results
        audit_evaluation.append(audit_result)

    return audit_evaluation

#################################################################
# build evaluation string
###############################################################

def build_evaluation_string(audit_data,operators):

    evaluation_str = ""
    
    for operator in operators:
        if operator not in ['not in','in','like']: 
            if evaluation_str == "":
                evaluation_str = str(audit_data) + " " + operator.keys()[0] + " " + str( operator.values()[0])
            else:
                evaluation_str = evaluation_str + " and " + str(audit_data) + " " + operator.keys()[0] + " " + str( operator.values()[0])
        else:
            if operator in ['not in','in']:
                evaluation_str = str(audit_data) + " " + operator.keys()[0] + " " + str( operator.values()[0])
    
    return evaluation_str

###########################################################################
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

########################################################################################
# transform data
########################################################################################
def transform(sql_output,row,col,datum):
    
    row_set = Set([])
    col_set = Set([])
    transformed_data = []
    dtx = {}
    sorted_str_cols = [] 

    for data in sql_output:
        row_set.add(data[row])
        col_set.add(data[col])
    
    rows = list(row_set)
    cols = list(col_set)
    sorted_cols = sorted(cols)
    
    for _col in sorted_cols:
        sorted_str_cols.append(str(_col))

    for elem in rows:
        dtx[elem] = {row:elem}

    for data in sql_output:
        
        placeholder = data[row] 
        for col_val in cols:
            if data[col] == col_val:
                dtx[placeholder][col_val] = data[datum]

            #data.pop(datum,'None')
            #data.pop(col,'None')

        # add modified data to transformed data list
        
    for elem in rows:
        transformed_data.append(dtx[elem])

    return transformed_data,sorted_str_cols

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

#############################################
# title headers
########################################
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
         
         
def create_html_content(startdate_display, html_params, template):
    
    sys.path.insert(0, 'templates/django_settings')
    os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
    django.setup()
    
    html_content = render_to_string(template, {
                                    'daily_date': startdate_display,
                                    'headers': html_params['header'],
                                    'output': html_params['output'],
                                    'sql_list': html_params['sql_list'],
                                    'section_headings': html_params['section_heading'],
                                    'pivots': html_params['pivot'],
                                    'table_borders': html_params['table_border'],
                                    'variables': html_params['variables'],
                                    'column_case': html_params['column-case'],
                                    'audit' :html_params['audit']
                                   })
    return html_content

def get_data(database,sql,startdate):
    result = helpers.get_data(database,sql,{'startdate' : startdate})
    return result
    

if __name__ == "__main__":
    main()

