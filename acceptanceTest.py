import cx_Oracle
import datetime
import os
from collections import OrderedDict
from decimal import Decimal
import sys
from datetime import timedelta
import json
from pprint import pprint
from json import JSONDecoder
from functools import partial
import os,sys, getopt, logging, os, ast, csv, tempfile, gzip
from db_services_metrics import helpers
from itertools import permutations
import itertools,random,uuid,shutil
import pprint
import pg
from pg import DB

# declar global varialbles
operators_t1 = ["+","-","/","*"]
operators_t2 = ["=","<","<=",">",">=","!=","<>"]
operators_t3 = ["=","!=","<>"]
operators_t4 = ["IS NULL", "IS NOT NULL"]
query_limitation = 100

#####################################################
 # main
#####################################################
def main():

    print('########## START PROCESS')
    print(uuid.uuid4())
    print(datetime.datetime.now())
    args = arg_parser()
    startdate = datetime.datetime.strptime(args.startdate, "%Y-%m-%d")

    # define acceptable functions
    functions = ["SUM","MIN","MAX","COUNT"]
    
    #get sample tables to build the  
    tables,table_hash,table_columns,table_sample = get_tables()

    #clean the data
    clean_dirs(tables)
 
    #get pivot queries:
    for table in tables:
        try:
            dir = '/local/'+table+ '/queries'
            os.makedirs(dir)
            os.makedirs('/local/'+table+'/schema')
        except OSError:
            pass

        manifest = {}
        manifest["queries"] = list()
        
        templates = get_templates(table)
        
        for key,value in templates.iteritems():
            queries = get_queries(table,table_hash[table],functions,table_columns[table],value,key,table_sample)
            fname = key + '.json'
            out_file = dir + '/'+ fname
            manifest["queries"].append(add_manifest_query(fname))
            print("Writing to file %s" % out_file)
            with open(out_file,'w') as fp:
                json.dump(queries,fp,sort_keys=True, indent=4, separators=(',', ': '))

        # add variety to date columns
        for key,value in templates.iteritems():
            for date_type in ['day','month','year']:
                queries = get_queries_with_dates(table,table_hash[table],functions,table_columns[table],date_type,value,key,table_sample)
                fname =  date_type + "_"+ key+ '-query.json'
                out_file = dir+ '/' + fname
                manifest["queries"].append(add_manifest_query(fname))
                print("Writing to file %s" % out_file)
                with open(out_file,'w') as fp:
                    json.dump(queries,fp,sort_keys=True, indent=4, separators=(',', ': '))

        #build manifest
        build_manifest(manifest,table)
    
    print(datetime.datetime.now())
    print('\n########## FINISHED SUCCESSFULLY #############')

###############################################################################
# clean old data
###############################################################################
def clean_dirs(tables):
     for table in tables:
        try:
            dir = '/local/'+table
            if os.path.exists(dir):
                 shutil.rmtree(dir,ignore_errors=True)
        except OSError:
             pass

##############################################################################
# add query to manifest
##############################################################################

def add_manifest_query(out_file):
    return {"file_name": out_file, "strategy": "deep", "type":"sql", "qp":"PL1"}

#####################################################################
#build manifest
###################################################################
def build_manifest(manifest,table):
    
    sql = "select FILENAME, SCHEMANAME,SCHEMA  from FILEMAP where TABLENAME = '" + table +"'"
    conn = create_ora_connection()
    curs = conn.cursor()
    curs.execute(sql)

    for row_data in curs:
        filename,schemaname,schema = row_data
    conn.close()

    manifest["data_files"] = [{"file_name": filename, "type": "csv","table":table}]
    manifest["schema"]= [{"file_name":schemaname, "type":"json"}]

    manifest_file = '/local/'+table+ '/Manifest'
    with open(manifest_file,'w') as fp:
        json.dump(manifest,fp,sort_keys=True, indent=4, separators=(',', ': '))
    print("Manifest\n")
    print(manifest) 
    # build schema here too
    with open('/local/'+table+ '/schema/'+ schemaname,'w') as fp:
        fp.write(schema)
 
###################33 build meta data ##################################
#######################################################################
def get_tables():
    
    tables = []
    table_hash = {}
    table_details = []
    table_columns = {}
    table_sample = {}
    sql = """
             SELECT table_name,column_name,data_type,data_scale
             from (
               select table_name,column_name,data_type,data_scale,MAX(COLUMN_ID) OVER (PARTITION BY TABLE_NAME) COL_CNT 
               from all_tab_cols 
               where owner ='MASTERUSER'
               )
             WHERE COL_CNT<=25 and table_name in ('NYCRIMESDIVISION') and table_name not like '%%_STG%%' 
             and table_name not in ('WINDERY_DATA_RAW_ORDERS','DEVICE_GROSS_NET','YEAR_2015_OPP_TEAM_SCORING','FILEMAP','TEST')
          """

    conn = create_ora_connection()
    curs = conn.cursor()
    curs.execute(sql) 

    for row_data in curs:
        #print(row_data)
        table_name,column_name,data_type,data_scale = row_data
        if not table_name in tables:
            print('############# processing table %s ################' % table_name)
            tables.append(table_name)
            table_hash[table_name] ={"NUMBER":[],"DATE":[],"STRING":[],"DECIMAL":[]}
            table_columns[table_name] = []
        #add columns by type
        if data_type == "NUMBER" and data_scale > 0: 
            table_hash[table_name]["DECIMAL"].append(column_name)
        elif data_type == "DATE":
            table_hash[table_name]["DATE"].append(column_name)
        elif data_type == "VARCHAR2":
            table_hash[table_name]["STRING"].append(column_name)
        elif data_type == "NUMBER":
            table_hash[table_name]["NUMBER"].append(column_name)
        
        #add all columns for the table
        table_columns[table_name].append(column_name)
    
    #print(tables)
    print(table_hash)
    #collect sample data 
    for table in tables:
        sql = " select * from %s sample(4)" % table
        curs.execute(sql)
        table_sample[table] = {}
        for col in curs.description:
            table_sample[table][col[0]] = []
        
        for row_data in curs:
            for idx,col in enumerate(curs.description):
                table_sample[table][col[0]].append(row_data[idx])
        
            
    conn.close() 
 
    return tables,table_hash,table_columns,table_sample
    

##################################################################
# build groups of columns
##################################################################
def build_groups(table,table_hash,table_columns,group_size_allowed):
     #pairings
    groups = []
    total_cols = len(table_columns)
    # first pass:
    for elem in table_columns:
        groups.append([elem])
    
    endpoint = min(total_cols,group_size_allowed)

    if endpoint >1:
        for pointer in range(1,endpoint):#decides the max numbe rof elems in one array
            for i,elem in enumerate(groups):
                 if len(elem) == pointer: #pick up the correct elem else we will see repeatation
                            last_elem_idx = table_columns.index(elem[-1]) # get the last element of the array
                            if(last_elem_idx+1<=total_cols-1):
                                  for dat in range(last_elem_idx+1,total_cols):
                                       temp = list(elem)                # it is important to do a deep copy
                                       temp.append(table_columns[dat])  # append the next elem to temp
                                       groups.append(temp)              # append array to groups
    return groups

##########################################################################3
# build all t templates
########################3##################################################

def get_templates(table):
    
    templates = {}
    ##########################  inner join templates #########################################
    templates = merge_dicts(templates,get_regular_queries(table))
    #templates = merge_dicts(templates,get_groupby_templates(table))
    #templates = merge_dicts(templates,get_whereclause_templates(table))
    #templates = merge_dicts(templates,get_innerjoin_templates(table))
    #templates = merge_dicts(templates,get_twodim_other_bucket_templates(table))
    #templates = merge_dicts(templates,get_onedim_other_bucket_templates(table))
    return templates

def get_regular_queries(table):
    templates = {}
       # no group by just columns
    regular_sql = "select $col_select from (SELECT $cols from \"" + table+ "\")  as \"temp1\" order by $col_order limit 1000;"
    templates["regular_sql"] = regular_sql

    # no group by just columns
    where_regular_sql = "select $col_select from ( SELECT $cols from \"" + table+ "\" where $filter_col ) as \"temp1\"  order by $col_order limit 1000;"
    templates["where_regular_sql"] = where_regular_sql

    return templates

###########################################################################################
# group by templates
##########################################################################################

def get_groupby_templates(table):

    templates = {}

    no_row_col_groupby_sql = "SELECT $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\" from \""+table+"\";"
    templates["no_row_col_groupby_sql"] = no_row_col_groupby_sql

    colonly_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from (SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    colonly_groupby_sql += " from \"" + table+ "\" group by $col_grp) as \"temp1\" order by $col_order  limit 200;"
    templates["colonly_groupby_sql"]= colonly_groupby_sql

    rowonly_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from (SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\" from \"" + table+ "\" group by $col_grp)  as \"temp1\" order by $col_order limit 1000;"
    templates["rowonly_groupby_sql"] =  rowonly_groupby_sql

    #pivot group by
    row_col_groupby_sql =  "SELECT $col_select, \"$first_arg_$first_func_name\",\"count\" from"
    row_col_groupby_sql += " ( SELECT  $col_select, \"$first_arg_$first_func_name\",\"count\" ,DENSE_RANK() OVER (ORDER BY $colgroup) AS \"$RANK_2\" FROM"
    row_col_groupby_sql += " (SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\","
    row_col_groupby_sql += "DENSE_RANK() OVER (ORDER BY $rowgroup) AS \"$RANK_1\", COUNT(*) as \"count\" from \"" + table+ "\" group by  $col_grp) AS \"t3\" "
    row_col_groupby_sql += "WHERE \"$RANK_1\" <= 1000) AS \"t6\" WHERE \"$RANK_2\" <= 200 order by $col_order;"
    templates["row_col_groupby_sql"] = row_col_groupby_sql

    #regular group by
    singlecol_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from (SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\",COUNT(*) as \"count\" from \"" + table+ "\" group by $col_grp)  as \"temp1\" order by $col_order limit 1000;"
    templates["singlecol_groupby_sql"]= singlecol_groupby_sql

    #multi group by
    mcol_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"$second_arg_$second_func_name_1\",\"count\" from (SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" ,$second_func_name(\"$second_arg\") as \"$second_arg_$second_func_name_1\" , COUNT(*) as \"count\" from \""
    mcol_groupby_sql +=  table+ "\" group by  $col_grp)  as \"temp1\" order by $col_order limit 1000;"
    templates["mcol_groupby_sql"] = mcol_groupby_sql

    return templates


########################################################################################
# where clause tmeplates 
#######################################################################################
def get_whereclause_templates(table):

    templates = {}

    where_no_row_col_groupby_sql = "SELECT $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\" from \""+table+"\" where $filter_col;"
    #templates["where_no_row_col_groupby_sql"] = where_no_row_col_groupby_sql

    where_singlecol_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from ( SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\",COUNT(*) as \"count\" from \"" + table
    where_singlecol_groupby_sql += "\" where $filter_col group by $col_grp ) as \"temp1\" order by $col_order limit 1000;"
    templates["where_singlecol_groupby_sql"]= where_singlecol_groupby_sql

    #multi group by
    where_mcol_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"$second_arg_$second_func_name_1\",\"count\" from ( SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" ,$second_func_name(\"$second_arg\") as \"$second_arg_$second_func_name_1\" , COUNT(*) as \"count\" from \""
    where_mcol_groupby_sql +=  table+ "\" where $filter_col group by  $col_grp ) as \"temp1\" order by $col_order limit 1000;"
    templates["where_mcol_groupby_sql"] = where_mcol_groupby_sql

    where_colonly_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from (SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    where_colonly_groupby_sql += " from \"" + table+ "\" where $filter_col group by $col_grp ) as \"temp1\" order by $col_order  limit 200;"
    templates["where_colonly_groupby_sql"]= where_colonly_groupby_sql

    where_rowonly_groupby_sql = "select $col_select, \"$first_arg_$first_func_name\",\"count\" from (SELECT $cols,$first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\" from \"" + table
    where_rowonly_groupby_sql += "\" where $filter_col group by $col_grp) as \"temp1\" order by $col_order limit 1000;"
    templates["where_rowonly_groupby_sql"] =  where_rowonly_groupby_sql
    
    return templates

################################################################
#  inner join templates
################################################################
def get_innerjoin_templates(table):

    templates = {}

    create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    create_table += " from \"" + table+ "\" group by $col_grp;"
    
    cluster_sql =  "select $col_select, \"$first_arg_$first_func_name\",\"count\" from"
    cluster_sql += " (select $col_select, \"$first_arg_$first_func_name\",\"count\" from ("
    cluster_sql += "select \"$create_table\".$colgroup, \"t\".$rowgroup, \"$create_table\".\"count\", \"$create_table\".\"$first_arg_$first_func_name\""
    cluster_sql += ",DENSE_RANK() OVER (ORDER BY \"t\".\"$first_arg_$first_func_name\" DESC, \"t\".$rowgroup) AS \"$RANK_2\""
    cluster_sql += ",DENSE_RANK() OVER (PARTITION BY \"t\".$rowgroup ORDER BY \"$create_table\".\"$first_arg_$first_func_name\" DESC,\"$create_table\".$colgroup ) AS \"$RANK_3\""
    cluster_sql += " from (SELECT $rowgroup, $first_func_name(\"$first_arg_$first_func_name\") as \"$first_arg_$first_func_name\" FROM \"$create_table\" GROUP BY $rowgroup) AS \"t\" INNER JOIN \"$create_table\" on \"t\".$rowgroup = \"$create_table\".$rowgroup"
    cluster_sql += ") as \"temp1\" WHERE \"$RANK_2\" <= 50 AND \"$RANK_3\" <= 20) as \"t3\" order by $col_order"
    
    templates["cluster_sql"] = create_table + " " + cluster_sql
     
    where_create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    where_create_table += " from \"" + table+ "\" where $filter_col group by $col_grp;"
    
    templates["where_cluster_sql"] = where_create_table + " " + cluster_sql  
    
    return templates
#####################################################################
# other buckets 
#################################################################
def get_twodim_other_bucket_templates(table):
    templates = {}

    create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    create_table += ", DENSE_RANK() OVER (ORDER BY $rowgroup) AS \"$RANK_1\",DENSE_RANK() OVER (PARTITION BY $rowgroup ORDER BY $colgroup DESC) AS \"$RANK_2\""
    create_table += " from \"" + table+ "\" group by $col_grp;"
    
    where_create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    where_create_table += ", DENSE_RANK() OVER (ORDER BY $rowgroup) AS \"$RANK_1\",DENSE_RANK() OVER (PARTITION BY $rowgroup ORDER BY $colgroup DESC) AS \"$RANK_2\""
    where_create_table += " from \"" + table+ "\" where $filter_col group by $col_grp;"

    regular_data = "SELECT $col_select,\"count\", \"$first_arg_$first_func_name\" FROM (SELECT  $col_select,\"count\", \"$first_arg_$first_func_name\" FROM \"$create_table\" WHERE \"$RANK_1\" <= 1000 AND \"$RANK_2\" <= 20) AS \"t0\""
    regular_data += " ORDER BY $col_order;"

    other_bucket1 = "SELECT $first_func_name(\"$first_arg_$first_func_name\") as \"otherbucket_$first_arg_$first_func_name_1\", SUM(\"count\") AS \"otherbucket_row_count_1\" ,COUNT(*) AS \"otherbucket_group_count_1\""
    other_bucket1 += " FROM \"$create_table\" WHERE \"$RANK_1\" > 1000;"
    
    other_bucket2 = "SELECT $spl_rowgroup,$first_func_name(\"$first_arg_$first_func_name\") as \"otherbucket_$first_arg_$first_func_name_2\", SUM(\"count\") AS \"otherbucket_row_count_2\" ,COUNT(*) AS \"otherbucket_group_count_2\""
    other_bucket2 += " FROM \"$create_table\" WHERE \"$RANK_2\" > 20 AND \"$RANK_1\" <= 1000 GROUP BY $spl_rowgroup;"

    templates["other_bucket_2dim"] = create_table + " " + regular_data + " "  + other_bucket1 + " " + other_bucket2
    templates["where_other_bucket_2dim"] = where_create_table + " " + regular_data + " "  + other_bucket1 + " " + other_bucket2

    return templates
####################################################################
# 1 dim other bucket
###################################################################
def get_onedim_other_bucket_templates(table):
    templates = {}
    
    create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    create_table += ", DENSE_RANK() OVER (ORDER BY $rowgroup) AS \"$RANK_1\" from \"" + table+ "\" group by $col_grp;"
    
    where_create_table = "CREATE TABLE \"$create_table\" AS SELECT $cols , $first_func_name(\"$first_arg\") as \"$first_arg_$first_func_name\" , COUNT(*) as \"count\""
    where_create_table += ", DENSE_RANK() OVER (ORDER BY $rowgroup) AS \"$RANK_1\" from \"" + table+ "\" where $filter_col group by $col_grp;"

    regular_data = "SELECT $col_select,\"count\", \"$first_arg_$first_func_name\" FROM (SELECT  $col_select,\"count\", \"$first_arg_$first_func_name\" FROM \"$create_table\" WHERE \"$RANK_1\" <= 1000 ) AS \"t0\""
    regular_data += " ORDER BY $col_order;"
    
    other_bucket1 = "SELECT $first_func_name(\"$first_arg_$first_func_name\") as \"otherbucket_$first_arg_$first_func_name_1\", SUM(\"count\") AS \"otherbucket_row_count_1\" ,COUNT(*) AS \"otherbucket_group_count_1\""
    other_bucket1 += " FROM \"$create_table\" WHERE \"$RANK_1\" > 1000;"
    
    templates["other_bucket_1dim"] = create_table + " " + regular_data + " "  + other_bucket1
    templates["where_other_bucket_1dim"] = where_create_table + " " + regular_data + " "  + other_bucket1 

    return templates
################################################################
# merge dicts 
################################################################
def merge_dicts(a,b):
    z = a.copy()
    z.update(b)
    return z     

########################################################################3
# pivot logic 
#########################################################################

def get_queries(table,table_hash,functions,table_columns,template_data,template_name,table_sample):

    print('######################################## get_queries ###################################')
    templates = {}
    templates["templates"] = []
    file = {}
    file["queries"] = []
    column_order = {"1": "ASC","2": "DESC"}
    #get lengths of each array
    len_number,len_decimal,len_string,len_date = len(table_hash["NUMBER"]), len(table_hash["DECIMAL"]),len(table_hash["STRING"]), len(table_hash["DATE"])
    
    #pairings
    groups = build_groups(table,table_hash,table_columns,3)
    
    # populate templates 
    for cols in groups:
        col_str,col_order_str,col_select = "" , "",""
        for idx, col in enumerate(cols):
            col_str += "\"" + col + "\","
            col_select += "\""+col+"\","
            col_order_str += "\"" + col + "\" " + column_order[str(random.randint(1,2))] + " ,"
                    
        #replace the last quote
        col_str,col_order_str,col_select = col_str[:-1], col_order_str[:-1],col_select[:-1]
        
        # add the columns to the template and add them to them query file
        if not ( template_name in ['where_other_bucket_1dim', 'where_other_bucket_2dim','other_bucket_1dim', 'other_bucket_2dim', 'row_col_groupby_sql', 'cluster_sql' , 'where_cluster_sql']):
            templates["templates"].append(template_data.replace('$col_select',col_select).replace("$cols",col_str).replace("$col_order",col_order_str).replace("$col_grp",col_str))

        elif template_name in ['row_col_groupby_sql']:
        #special handling for pivot
            if len(cols)>=2:
                perms = itertools.permutations(cols)
                str_len = len(cols)
                for perm in perms:
                    perm_list = list(perm)
                    for i in range(1,str_len):
                        row_group = ""
                        col_group = ""
                        for idx, rowdata in enumerate(perm_list[0:i]):
                            row_group += "\"" + rowdata + "\","
                    
                        for idx, coldata in enumerate(perm_list[i:str_len]):
                            col_group += "\"" + coldata + "\","
 
                        #replace the last quote
                        row_group,col_group,tempstr = row_group[:-1],col_group[:-1],""

                        tempstr= template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group).replace("$colgroup",col_group).replace("$col_order",col_order_str).replace("$col_grp",col_str) 
                        templates["templates"].append(tempstr)
 
        elif template_name in [ 'cluster_sql','where_cluster_sql','other_bucket_2dim','where_other_bucket_2dim']: 
             #only two items will be presented in this case
             if len(cols) == 2:
                perms = itertools.permutations(cols)
                for perm in perms:
                    perm_list = list(perm)
                    row_group,col_group,rowdata,coldata = "","",perm_list[0],perm_list[1]
                    row_group += "\"" + rowdata + "\""
                    col_group += "\"" + coldata + "\""
                    tempstr = ""
                    tempstr = template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group)
                    tempstr = tempstr.replace("$colgroup",col_group).replace("$col_order",col_order_str).replace("$col_grp",col_str).replace("$spl_rowgroup",row_group)
                    templates["templates"].append(tempstr)
        elif template_name in [ 'other_bucket_1dim','where_other_bucket_1dim']:
             if len(cols) == 1:
                 row_group = ""
                 row_group += "\"" + cols[0] + "\""
                 tempstr = ""
                 tempstr = template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group)
                 tempstr = tempstr.replace("$col_order",col_order_str).replace("$col_grp",col_str).replace("$spl_rowgroup",row_group)
                 templates["templates"].append(tempstr)

    #remove duplicate entries. it is better to do that here instead of doing that in the loops
    queries_after_functions_changes = build_functions(templates["templates"],functions,table_hash)
    queries_after_where_changes = build_where(queries_after_functions_changes,table,table_hash,table_columns,table_sample)
      
    file["queries"] = list(set(queries_after_where_changes))
    print("Total number of queries generated: ", len(file["queries"]))
    holder = file["queries"][:query_limitation]
    file["queries"] = []
 
    for qry in holder:
        inter_table = str(uuid.uuid4())
        file["queries"].append(qry.replace("$create_table",inter_table))

    return file

#################################################################################
# make quries with different date formats 
#################################################################################

def get_queries_with_dates(table,table_hash,functions,table_columns,date_type,template_data,template_name,table_sample):

    print('######################################## get_queries_with_dates ###################################')
    templates = {}
    templates["templates"] = []
    file = {}
    file["queries"] = []
    column_order = {"1": "ASC","2": "DESC"}

    #get lengths of each array
    len_number,len_decimal,len_string,len_date = len(table_hash["NUMBER"]), len(table_hash["DECIMAL"]),len(table_hash["STRING"]), len(table_hash["DATE"])

    #pairings
    groups = build_groups(table,table_hash,table_columns,5)

    # populate templates
    for cols in groups:
        col_select,col_str,col_order_str,col_grp = "","","",""
        for idx, col in enumerate(cols):
            
            if col in table_hash["DATE"]:
                col_str += "date_trunc('"+ date_type +"',\""+col+"\") as \""+ col + "\","
                col_select += "\""+col+"\","
                col_grp += "date_trunc('"+ date_type +"',\""+col+"\"),"
                #if template_name != 'row_col_groupby_sql': 
                #    col_order_str += "date_trunc('"+ date_type +"',\""+col+"\") " + column_order[str(random.randint(1,2))] + " ,"
                #elif template_name == 'row_col_groupby_sql':
                col_order_str += "\""+col+"\" " + column_order[str(random.randint(1,2))] + " ,"
            else:    
                col_str += "\"" + col + "\","
                col_select += "\""+col+"\","
                col_grp += "\"" + col + "\","
                col_order_str += "\"" + col + "\" " + column_order[str(random.randint(1,2))] + " ,"

        #replace the last quote
        col_str , col_order_str,col_grp,col_select = col_str[:-1] , col_order_str[:-1],col_grp[:-1],col_select[:-1]
        # add the columns to the template and add them to them query file
        if not ( template_name in ['where_other_bucket_1dim', 'where_other_bucket_2dim','other_bucket_1dim', 'other_bucket_2dim', 'row_col_groupby_sql', 'cluster_sql' , 'where_cluster_sql']):
            templates["templates"].append(template_data.replace('$col_select',col_select).replace("$cols",col_str).replace("$col_order",col_order_str).replace("$col_grp",col_grp))
        elif template_name in ['row_col_groupby_sql']:
            #special handling for pivot
            if len(cols)>=2:
                perms = itertools.permutations(cols)
                str_len = len(cols)
                for perm in perms:
                    perm_list = list(perm)
                    for i in range(1,str_len):
                        row_group = ""
                        col_group = ""
                        for idx, rowdata in enumerate(perm_list[0:i]):
                            if rowdata in table_hash["DATE"]:
                                row_group +=  "date_trunc('"+ date_type +"',\""+rowdata+"\"),"
                            else:
                                row_group += "\"" + rowdata + "\","

                        for idx, coldata in enumerate(perm_list[i:str_len]):
                            if coldata in table_hash["DATE"]:
                                col_group += "date_trunc('"+ date_type +"',\""+coldata+"\"),"
                            else: 
                                col_group += "\"" + coldata + "\","

                        #replace the last quote
                        row_group,col_group,tempstr = row_group[:-1],col_group[:-1],""
                        
                        tempstr = template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group).replace("$colgroup",col_group)
                        tempstr = tempstr.replace("$col_order",col_order_str).replace("$col_grp",col_grp)
                        templates["templates"].append(tempstr)
        elif template_name in [ 'where_other_bucket_2dim','cluster_sql','where_cluster_sql','other_bucket_2dim']:
             #only two items will be presented in this case
             if len(cols) == 2:
                perms = itertools.permutations(cols)
                str_len = len(cols)
                for perm in perms:
                    perm_list = list(perm)
                    row_group,row_group_spl,col_group = "","",""
                    rowdata,coldata = perm_list[0],perm_list[1]
                    
                    if rowdata in table_hash["DATE"] and template_name == 'other_bucket_2dim':
                        row_group +=  "date_trunc('"+ date_type +"',\""+rowdata+"\")"
                    else:
                        row_group += "\"" + rowdata + "\""
                    row_group_spl += "\"" + rowdata + "\""    
                    
                    if coldata in table_hash["DATE"] and template_name == 'other_bucket_2dim':
                        col_group += "date_trunc('"+ date_type +"',\""+coldata+"\")"
                    else:
                        col_group += "\"" + coldata + "\""
                    #replace the last quote
                    tempstr = ""
                    tempstr = template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group)
                    tempstr = tempstr.replace("$colgroup",col_group).replace("$col_order",col_order_str).replace("$col_grp",col_grp).replace("$spl_rowgroup",row_group_spl)
                    templates["templates"].append(tempstr)
        elif template_name in ['other_bucket_1dim','where_other_bucket_1dim']:
             if len(cols) == 1:
                 
                 row_group,rowdata = "",cols[0]
                 
                 if rowdata in table_hash["DATE"]:
                     row_group +=  "date_trunc('"+ date_type +"',\""+rowdata+"\")"
                 row_group_spl = "\"" + rowdata + "\""
                 
                 tempstr = ""
                 tempstr = template_data.replace("$col_select",col_select).replace("$cols",col_str).replace("$rowgroup",row_group)
                 tempstr = tempstr.replace("$col_order",col_order_str).replace("$col_grp",col_grp).replace("$spl_rowgroup",row_group)
                 templates["templates"].append(tempstr)

    #remove duplicate entries. it is better to do that here instead of doing that in the loops
    queries_after_functions_changes = build_functions(templates["templates"],functions,table_hash)
    queries_after_where_changes = build_where(queries_after_functions_changes,table,table_hash,table_columns,table_sample)

    file["queries"] = list(set(queries_after_where_changes))
    print("Total number of queries generated: ", len(file["queries"]))
    file["queries"] = file["queries"][:query_limitation]   

    holder = file["queries"][:query_limitation]
    file["queries"] = []
 
    for qry in holder:
        inter_table = str(uuid.uuid4())
        file["queries"].append(qry.replace("$create_table",inter_table))
 
    return file

########################################################################################################
# where clause
########################################################################################################
def build_where(templates,table,table_hash,table_columns,table_sample):
    
    import time
    file = {}
    file["queries"] = []
    groups = build_groups(table,table_hash,table_columns,2)
    # currently we only use and predicate
    for template in templates:
        for cols in groups:
            filter_string = ""
            for idx, col in enumerate(cols):
                # randomly pick one operator
                sample   = table_sample[table][col][random.randint(0,len(table_sample[table][col])-1)]
                
                if col in table_hash["STRING"]:
                   operator = operators_t3[random.randint(0,len(operators_t3)-1)]
                   filter_string += "\""+col +"\" " + str(operator) + " '"+str(sample)+"' and "
                elif col in table_hash["NUMBER"] + table_hash["DECIMAL"]:
                    operator = operators_t2[random.randint(0,len(operators_t2)-1)]
                    filter_string += "\""+col +"\" " + str(operator) + " "+ str(sample) + " and " 
                elif col in table_hash["DATE"]:
                    operator = operators_t3[random.randint(0,len(operators_t3)-1)]
                    filter_string += "\""+col +"\" " + str(operator) + " " + str(int(time.mktime(time.strptime(sample.strftime('%d-%b-%Y'),'%d-%b-%Y'))) - time.timezone) + " and "

            filter_string = filter_string[:-4] # remove extra  and                        
            file["queries"].append(template.replace("$filter_col",filter_string))
    # is null and not null section
    for template in templates:
        for cols in groups:
            filter_string = ""
            for idx, col in enumerate(cols):
                # randomly pick one operator
                operator = operators_t4[random.randint(0,len(operators_t4)-1)]
                filter_string += "\""+col +"\" " + str(operator) + " and "
            filter_string = filter_string[:-4] # remove extra  and
            file["queries"].append(template.replace("$filter_col",filter_string))
    
    
    # combo of nulls with other operators
    for template in templates:
        for cols in groups:
            filter_string = ""
            for idx, col in enumerate(cols):
                if random.randint(0,1) ==0 :
                    # randomly pick one operator
                    sample   = table_sample[table][col][random.randint(0,len(table_sample[table][col])-1)]

                    if col in table_hash["STRING"]:
                        operator = operators_t3[random.randint(0,len(operators_t3)-1)]
                        filter_string += "\""+col +"\" " + str(operator) + " '"+str(sample)+"' and "
                    elif col in table_hash["NUMBER"] + table_hash["DECIMAL"]:
                        operator = operators_t2[random.randint(0,len(operators_t2)-1)]
                        filter_string += "\""+col +"\" " + str(operator) + " "+ str(sample) + " and "
                    elif col in table_hash["DATE"]:
                        operator = operators_t3[random.randint(0,len(operators_t3)-1)]
                        filter_string += "\""+col +"\" " + str(operator) + " " + str(int(time.mktime(time.strptime(sample.strftime('%d-%b-%Y'),'%d-%b-%Y'))) - time.timezone) + " and "
                else:
                    operator = operators_t4[random.randint(0,len(operators_t4)-1)]
                    filter_string += "\""+col +"\" " + str(operator) + " and "   
 
            filter_string = filter_string[:-4] # remove extra  and
            file["queries"].append(template.replace("$filter_col",filter_string))
     # in clause
    for template in templates:
        for cols in groups:
            filter_string = ""
            for idx, col in enumerate(cols):
                # randomly pick one operator
                sample_size,sample = len(table_sample[table][col]),""
                random1 = random.randint(0,sample_size-1)
                random2 = max( sample_size , random.randint(random1,sample_size))
                
                if col in table_hash["STRING"]:
                    for x in table_sample[table][col][random1:random2]:
                        filter_string += "\""+col +"\" = '" + str(x) + "' or "  
                if col in table_hash["NUMBER"] + table_hash["DECIMAL"]:
                    for x in table_sample[table][col][random1:random2]:
                        filter_string += "\""+col +"\" = " + str(x) + " or "
                elif col in table_hash["DATE"]:
                    for x in table_sample[table][col][random1:random2]:
                        filter_string += "\""+col +"\" = " + str(int(time.mktime(time.strptime(x.strftime('%d-%b-%Y'),'%d-%b-%Y'))) - time.timezone) + " or "
            
            filter_string = filter_string[:-4] # remove extra  and
            file["queries"].append(template.replace("$filter_col",filter_string))

    return file["queries"]
    
#######################################################################################################
# replace functions
#######################################################################################################    
def build_functions(templates,functions,table_hash):

    file = {}
    file["queries"] = [] 
    # start of replacememnts:
    for template in templates:
        for nums in table_hash["NUMBER"] + table_hash["DECIMAL"]:
            for function in functions:
                temp_str = template.replace("$first_arg",nums).replace("$first_func_name",function)
                for function in functions:
                    file["queries"].append(temp_str.replace("$second_arg",nums).replace("$second_func_name",function))
        for data in table_hash["DATE"]:
            for function in ["MAX","MIN","COUNT"]:
                temp_str = template.replace("$first_arg",data).replace("$first_func_name",function)
                for function in ["MAX","MIN","COUNT"]:
                    file["queries"].append(temp_str.replace("$second_arg",data).replace("$second_func_name",function))
        for data in table_hash["STRING"]:
            for function in ["COUNT","MIN","MAX"]:
                temp_str = template.replace("$first_arg",data).replace("$first_func_name",function)
                for function in ["COUNT"]:
                    file["queries"].append(temp_str.replace("$second_arg",data).replace("$second_func_name",function))

    return file["queries"]

##############################################################################################################
# arg parser for the script
#########################################################################################################
def arg_parser():

    import argparse
    from datetime import timedelta
    parser = argparse.ArgumentParser(description="Send metric emails.")

    parser.add_argument('-e', action="append", dest='emails', default=[])
    parser.add_argument('-d', action="store", dest='startdate')
    parser.add_argument('-p', action="store", dest='product')

    args = parser.parse_args()
    if not args.emails:
        args.emails = ['babbar@amazon.com']
        print(args.emails)
    if args.startdate is None:
        startdate = datetime.datetime.now()
        if startdate.hour < 6:
            startdate += -timedelta(days=2)
        else:
            startdate += -timedelta(days=1)
        args.startdate = startdate.strftime("%Y-%m-%d")
    return args

###################################################################################################
# the metadata is stored in oracle 
#################################################################################################
def create_ora_connection():

    sid = 'SPICEACK'
    host = 'spiceacceptancetestgenerator.c4gq3x9r6akt.us-west-2.rds.amazonaws.com'
    port = 8192
    odin_name = 'com.amazon.aws.spaceneedle.dbcredentials.acceptancetest.oracle'
    user, password = helpers.get_odin_material(odin_name, 'Principal'), helpers.get_odin_material(odin_name, 'Credential')


    dsn_tns = cx_Oracle.makedsn(host, port, sid)
    conn = cx_Oracle.connect(user, password, dsn_tns)

    return conn
##############################################################################
# connect to pg to verufy your queries 
###############################################################################
def create_pg_connection():
    dbname = 'integ'
    host = 'postgresintegrationtestinstance.ci7hb3pawv1y.us-west-2.rds.amazonaws.com'
    port = 8192
    odin_name = 'com.amazon.aws.spaceneedle.dbcredentials.integtests.postgres'
    user, password = helpers.get_odin_material(odin_name, 'Principal'), helpers.get_odin_material(odin_name, 'Credential')  
    conn  = DB(dbname,host,port,user=user,passwd=password)
    #res = conn.query("""select * from "NYCRIMESDIVISION";""")
    return conn

if __name__ == "__main__":
    main()
