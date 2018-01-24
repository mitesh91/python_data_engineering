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

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key)

@register.filter
def get_list_item(list,idx):
    return list[idx]

@register.filter
def to_titlecase(value):
    m_value = value
    if isinstance(value,datetime.datetime):
        m_value = value.strftime("%Y-%m-%d")
    return str(m_value).replace("_"," ").title().replace('.', '')

@register.filter
def to_uppercase(value):
    m_value = value
    if isinstance(value,datetime.datetime):
          m_value = value.strftime("%Y-%m-%d")
    return str(m_value).replace("_"," ").upper().replace('.', '')

@register.filter
def to_lowercase(value):
    m_value = value
    if isinstance(value,datetime.datetime):
          m_value = value.strftime("%Y-%m-%d")  
    return str(m_value).replace("_"," ").lower().replace('.', '')

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
    parser.add_argument('-t', action="store", dest='template', default = 'generic_table.html')
        
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
    diff_transform = {}

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
        
        #diff transform
        diff_transform["seq"+ str(data["sectional-order"])] = data.get("diff-transform",{})
        #formatter
        formatter["seq"+ str(data["sectional-order"])] = data.get("format",{})

    template_features["column_case"] = s3_json_dict.get("column-case","title") 

    logging.info(sql_s3_path)
    return sql_s3_path,sql_section_heading,sql_database,sql_pivot,table_border_present,\
           sql_transform,transform_rows,transform_cols,transform_datum,template_features,formatter,diff_transform

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

    json_str = getJSONString(file_name)

    sql_paths,sectional_headings,sql_database,sql_pivot,table_border,sql_transform,\
    transform_rows,transform_cols,transform_datum,template_features,formatter,diff_transform = parseJSONString(json_str)

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

        # transform output if needed
        is_transform = sql_transform["seq" + str(idx+1)]
        rows = transform_rows["seq" + str(idx+1)] 
        cols = transform_cols["seq" + str(idx+1)]
        datum = transform_datum["seq" + str(idx+1)].get("col","")
        datum_format = transform_datum["seq" + str(idx+1)].get("format","")

        new_columns = []

        if len(sql_output) > 0 and is_transform == 'yes' and len(rows) ==1 and len(cols) ==1 and datum != ""  :
            sql_output,new_columns = transform(sql_output,rows[0],cols[0],datum)
            _elem  = new_columns[-1]
            # sort the data
            sql_output = sorted(sql_output,key=lambda item:item.get(_elem,'None') ,reverse=True)
            # format the data
            for _idx, _tdata in enumerate(sql_output):
                for k,v in _tdata.items():
                    if k!= rows[0] and datum_format!= "":
                        sql_output[_idx][k] = format(v,datum_format)
                        
        else:
            is_transform = 'no'
        
        # diff transform
        sql_output,transform_diff_columns =  get_diff_transform( diff_transform["seq" + str(idx+1)],sql_output )

        if len(sql_output) >0:
            #format the data
            _format = formatter["seq" + str(idx+1)] 
            if _format!= {}:
                format_output(sql_output,_format)

            html_params['output'].append(sql_output)

            #get the headers for the output    
            if is_transform == 'yes':
                headers = [rows[0]]
                headers.extend(new_columns)
            #any transform difff applied?
            else:
                if transform_diff_columns!= None and len(transform_diff_columns)!= 0:
                    headers = transform_diff_columns
                else:
                    headers = helpers.get_sql_header(database,sql,{'startdate' :startdate_display})

            html_params['header'].append(headers)
            html_params['section_heading'].append(title_string(sectional_headings["seq" + str(idx+1)]))
            
            #we allow pivots for one row datasets only at this time.
            is_pivot = sql_pivot["seq" + str(idx+1)]
            
            if len(sql_output) > 1 and is_pivot == 'yes':
                is_pivot = 'no'
             
            html_params['pivot'].append(is_pivot)
            html_params['table_border'].append(table_border["seq" + str(idx+1)])
            
        else:
            count_sql_requests = count_sql_requests  - 1

    request_list = range(0,count_sql_requests)
    html_params['sql_list'] = request_list
    html_params['column-case'] = template_features["column_case"]
    html_content = create_html_content(startdate_display, html_params, args.template)
    #archieve
    archieve(html_content,startdate_display,args.product)
    #print html_content
    helpers.send_html_email(args.emails, html_content, report_name) 
    logging.info('end time : {time}'.format(time = datetime.datetime.now()))

######################################################################
#  is it diff transform?
#######################################################################
def get_diff_transform(diff_transform,sql_output):
    
    new_sql_output = []
    sorted_str_columns = []

    if len(diff_transform) !=0:
         #load variables
         date_column = diff_transform["date-cols"]
         rows= diff_transform["rows"]
         show_diff = diff_transform["show-diff"]
         show_diff_pct = diff_transform["show-diff-pct"]
         formatter = diff_transform["format"]

         date_set = set()
         #create set of dates available
         for data in sql_output:
             #get dates
             date_set.add(data[date_column])
         
         
         date_list = sorted(list(date_set))
         # diff cannot be shown if there are more than 2 dates however transform
         # can be done.
         if len(date_list) >2:
             show_diff = "no"
             show_diff_pct = "no"
         
         for metric in rows:
             inner_data = {"metric": str(metric)}
             for dates in date_list:
                 for elem in sql_output:
                     #for the date in row that matches
                     if elem[date_column] == dates:
                         inner_data[dates] = elem.get(metric,0)
             new_sql_output.append(inner_data)
         
         
         if len(new_sql_output) >0:
             sorted_str_columns = sorted(date_list, reverse=True)
             for op in new_sql_output:
                  diff = ""
                  if show_diff == "yes":
                      d1,d2 = sorted_str_columns[0],sorted_str_columns[1]
                      diff = op.get(d1,0) - op.get(d2,0)
                      op["difference"] = diff
                      if show_diff_pct == "yes":
                          diff = "(" + str(round(100*diff/op.get(d2,0),2)) + "%)"
                      op["difference(%)"]= diff
             
             # apply format 
             for _idx,recs in enumerate(new_sql_output):
                 for k,v in recs.items():
                     # no need for formatting metric name here
                     if k not in ['metric','difference(%)']:
                         new_sql_output[_idx][k] =  format(v,formatter.get(recs.get('metric')))
                 # make metric name look nice.
                 recs['metric'] = recs['metric'].replace('_',' ').title()
                 # update the diff% as formatting is now applied too
                 recs["difference(%)"] = str(recs.get("difference")) + str(recs.get("difference(%)"))

             sql_output = new_sql_output
             
             sorted_str_columns.insert(0,'metric')
             if show_diff == "yes" and show_diff_pct == "yes":
                 sorted_str_columns.append('difference(%)')
             else:
                 if show_diff == "yes":
                     sorted_str_columns.append('difference')
         
    return sql_output,sorted_str_columns 

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
                dtx[placeholder][col_val] = data.get(datum,None)

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
                                    'column_case': html_params['column-case']
                                   })
    return html_content

def get_data(database,sql,startdate):
    result = helpers.get_data(database,sql,{'startdate' : startdate})
    return result
    

if __name__ == "__main__":
    main()

