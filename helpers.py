import pgdb
import os
import paramiko
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
import pymysql
import base64
import logging
import json
from boto.s3.key import Key

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')


def get_odin_material(name, type, decode=True):

    import httplib, os, json
    from base64 import b64decode

    conn = httplib.HTTPConnection('localhost', 2009)
    conn.request("GET", "/query?Operation=retrieve&ContentType=JSON&material.materialName=" + name+ "&material.materialType="+type)
    res = conn.getresponse()
    data = res.read()
    json_data = json.loads(data)
    if decode:
        material = b64decode(json_data['material']['materialData'])
    else:
        material = json_data['material']['materialData']
    return material

def get_encrypted_odin_material(name, type, decode=True):

    from spdacm import spda_deobfuscate_password
    key = get_odin_material(name, type, decode)

    return spda_deobfuscate_password(key)

def create_pgdb_connection(dbname, host, user, password):

    import pgdb

    conn = pgdb.connect(database=dbname, host=host, user=user, password=password)
    return conn

def create_pg_connection(dbname, host, port, user, password):

    import pg
    import socket

    conn = pg.connect(dbname, host, port, user=user, passwd=password)
    sh = socket.fromfd(conn.fileno(), socket.AF_INET, socket.SOCK_STREAM)
    sh.setsockopt(socket.IPPROTO_TCP, socket.SO_KEEPALIVE, 1)
    return conn

def create_ora_connection(dsn):

    import cx_Oracle

    sid = dsn['url']['name']
    host = dsn['url']['host']
    port = dsn['url']['port']
    user, password = get_dsn_credential(dsn)

    dsn_tns = cx_Oracle.makedsn(host, port, sid)
    conn = cx_Oracle.connect(user, password, dsn_tns)

    return conn

def create_red_connection(dsn):

    dbname = dsn['url']['name']
    host = dsn['url']['host']
    port = dsn['url']['port']
    user, password = get_dsn_credential(dsn)

    conn = create_pg_connection(dbname, host, port, user, password)

    return conn

def create_red_db_connection(dsn):
    dbname = dsn['url']['name']
    host = dsn['url']['host']
    port = dsn['url']['port']
    user, password = get_dsn_credential(dsn)

    conn = create_pgdb_connection(dbname=dbname, host=host + ':' + str(port), user=user, password=password)

    return conn

###########################################################
# generic connection for Aurora ec2 machines
############################################################

def create_aurora_db_connection(dsn):
    
    #pkeyfilepath = '/tmp/glue_key.pem'
    #mypkey = paramiko.RSAKey.from_private_key_file(pkeyfilepath)
    
    #Private key pulled from odin
    material =""
    pkey = helpers.get_odin_material(material,'PrivateKey')
    pkey = base64.encodestring(pkey).strip()
    pkey_tmpl = '-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----'
    pkey = pkey_tmpl % (pkey,)
    mypkey = paramiko.RSAKey.from_private_key(mypkey)

    # db properties
    dbname = dsn['url']['name']
    db_host = dsn['url']['host']
    db_port = dsn['url']['port']

    # ssh properties
    ssh_user = 'ec2-user'
    ssh_port = 22
    ssh_host = ''

    #db user name and password
    db_user, db_password = get_dsn_credential(dsn)

    with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(db_host,db_port)) as tunnel:
        conn =  pymysql.connect(host='127.0.0.1',user=db_user,
                passwd=db_password,db=dbname,
                port=tunnel.local_bind_port)
    return conn

#############################################################################

def create_s3_connection(dsn):

    from boto.s3.connection import S3Connection

    access_key, secret_key = get_dsn_credential(dsn)

    return S3Connection(access_key,secret_key)

def get_s3_access_keys(material = None):

    import os

    if material is None:
        material = os.environ['S3_MATERIAL_SET']

    dsn = get_dsn(material)

    access_key, secret_key = get_dsn_credential(dsn)

    return access_key, secret_key

###############################################################################
# upload string or file to s3: 
# default uploads string
############################################################################

def uploadToS3(startdateS3Prefix,dsnName,uploadBucketName,uploadFilename,upload_content,type='string'):

    s3bucket = getS3Bucket(dsnName,uploadBucketName)
    k = Key(s3bucket)
    k.key = startdateS3Prefix + '/' + uploadFilename

    if type == 'file':
        k.set_contents_from_filename(upload_content) 
    else:
        k.set_contents_from_string(upload_content)

    print("uploaded the file to s3 bucket")
     
##########################################################################
def get_dsn_connection(dsn):

    conn_type = {
        'redshift': create_red_connection,
        'oracle': create_ora_connection,
        's3': create_s3_connection,
        'red_spl': create_red_db_connection,
        'aurora': create_aurora_db_connection
    }

    func = conn_type[dsn['type']]

    return func(dsn)

def get_dsn_credential(dsn):

    if dsn['type'] in ('redshift','oracle','s3','red_spl','aurora'):
        if dsn['credential_type'] == 'odin':
            user, password = get_odin_material(dsn['credentials']['material'], 'Principal'), get_odin_material(dsn['credentials']['material'], 'Credential')
        elif dsn['credential_type'] == 'encrypted_odin':
            user, password = get_odin_material(dsn['credentials']['material'], 'Principal'), get_encrypted_odin_material(dsn['credentials']['material'], 'Credential')
        elif dsn['credential_type'] == 'simple':
            user, password = dsn['credentials']['user'], dsn['credentials']['password']
        return user, password
    elif dsn['type'] == 'symmetrickey':
        if dsn['credential_type'] == 'odin':
            return get_odin_material(dsn['credentials']['material'], 'SymmetricKey', False)
        elif dsn['credential_type'] == 'simple':
            return dsn['credentials']['symmetrickey']

def get_connection(name):

    dsn = get_dsn(name)
    conn = get_dsn_connection(dsn)

    return conn


def get_credentials(name):

    dsn = get_dsn(name)
    credentials = get_dsn_credential(dsn)
    return credentials

def get_dsn(name, config = None):

    import yaml
    import inspect

    parent_caller = len(inspect.stack())
    caller = inspect.stack()[parent_caller-1][1]

    if config is None:
        if caller != 'lib/python2.7/site-packages/db_services_metrics/ora_unload.py':
            config = 'data/connections.yaml'
        else:
            config = 'data/connections_regional.yaml'

    stream = file(config,'r')
    doc = yaml.load(stream)
    dsn = [v for i, v in enumerate(doc) if v['dsn'] == name ][0]

    return dsn

## below is a bunch of crap that should be depricated and deleted from that module

def create_cookie_connection():

    dsn = get_dsn('datamart')
    dbname = dsn['url']['name']
    host = dsn['url']['host']
    port = dsn['url']['port']
    user, password = get_dsn_credential(dsn)
    conn = create_pgdb_connection(dbname=dbname, host=host + ':' + str(port), user=user, password=password)
    return conn

def create_datamart_connection(material = None):

    conn = get_connection('datamart')
    return conn

def create_awsdw_connection(material = None):

    dsn = get_dsn('awsdw')
    dbname = dsn['url']['name']
    host = dsn['url']['host']
    port = dsn['url']['port']
    user, password = get_dsn_credential(dsn)
    conn = create_pgdb_connection(dbname=dbname, host=host + ':' + str(port), user=user, password=password)
    return conn


def send_html_email(emails, html_content, subject):
    import smtplib
    from django.utils.html import strip_tags
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    import time

    text_content = strip_tags(html_content)
    msg = MIMEMultipart('alternative')
    part1 = MIMEText(text_content.encode('utf-8'), 'plain', 'utf-8')
    part2 = MIMEText(html_content.encode('utf-8'), 'html', 'utf-8')
    msg.attach(part1)
    msg.attach(part2)
    msg['Subject'] = subject
    msg['From'] = 'dbsmetricsgenerator@gmail.com'
    smtp = smtplib.SMTP('smtp.gmail.com')
    #smtp.starttls()
    print('to emails', emails)
    msg['To'] =  ', '.join(emails)
    try:
        smtp.sendmail('dbsmetricsgenerator@gmail.com', emails, msg.as_string())
        smtp.quit()
    except:
        time.sleep(60)
        smtp.sendmail('dbsmetricsgenerator@gmail.com', emails, msg.as_string())
        smtp.quit()
    print('Sent Out HTML Email')

def send_email(emails, payload, subject, file_input = True):

    print('sending email 1', emails)
    import os
    from smtplib import SMTP
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email.MIMEMultipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    from email import Encoders

    if file_input:

        filename = os.path.basename(payload.name)
        msg = MIMEMultipart()
        part = MIMEText(filename)
        msg.attach(part)
        part = MIMEBase('application','octet-stream')
        print('file name', filename)
        part.set_payload(open(payload.name,'rb').read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)
    else:
        msg = MIMEText(payload)

    msg['Subject'] = subject
    msg['From'] = 'dbsmetricsgenerator@gmail.com'

    smtp = SMTP('smtp.amazon.com')
    #smtp.starttls()
    msg['To'] =  ', '.join(emails)

    try:
        smtp.sendmail('dbsmetricsgenerator@gmail.com', emails, msg.as_string())
        smtp.quit()
    except:
        sleep(60)
        smtp.sendmail('dbsmetricsgenerator@gmail.com', emails, msg.as_string())
        smtp.quit()
    print('Sent Out CSV Email')


def get_cookie_data(sql, parms):
    conn = create_cookie_connection()
    cursor = conn.cursor()
    cursor.execute(sql, parms)
    desc = [d[0] for d in cursor.description]
    results = [dict(zip(desc,line)) for line in cursor]
    cursor.close()
    conn.close()
    return results

#####################################################################
# Generic function to connect to any database and retrive the data.
# input: dsn (name of database in connections.yaml)
#        sql
#        params
# output: result set
#####################################################################
def get_sql_header(dsn,sql,params):
    conn = get_connection(dsn)
    cursor = conn.cursor()
    cursor.execute(sql, params)
    desc = [d[0] for d in cursor.description]
    cursor.close()
    conn.close()
    return desc 
   
def get_data(dsn,sql, parms):
    conn = get_connection(dsn)
    cursor = conn.cursor()
    cursor.execute(sql, parms)
    desc = [d[0] for d in cursor.description]
    results = [dict(zip(desc,line)) for line in cursor]
    cursor.close()
    conn.close()
    return results

def run_dml(dsn,sql,params):
    import sys
    try:
        conn = get_connection(dsn)
        cursor = conn.cursor()
        cursor.execute(sql, params)
        cursor.close()
        conn.commit()
        conn.close()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

def get_awsdw_data(sql, parms):
    from string import Template
    conn = create_awsdw_connection()
    sql = Template(sql).safe_substitute(dict( parms ))
    results = conn.query(sql)
    conn.close()
    return results

def find_start_end_dates(date):
    from datetime import timedelta
    if date.weekday() < 6:
        days_to_go = date.weekday() + 2
        enddate = date - timedelta(days_to_go)
    else:
        enddate = date - timedelta(1)
    startdate = enddate - timedelta(6)
    return startdate, enddate

#####################################################################
# get s3 bucket
#####################################################################

def getS3Bucket(dsnName,bucketName):
    s3_conn = get_connection(dsnName)
    s3bucket = s3_conn.get_bucket(bucketName)
    return s3bucket

##################################################################################
# get content of s3 file as string
#################################################################################

def getS3FileString(file_name, s3_dsn, bucketName):
    
    from boto.s3.key import Key
    
    logging.info('Getting content of [{file}] from [{bucket}] using [{credentials}]'.format(
        file = file_name, bucket = bucketName, credentials = s3_dsn))
    
    bucketPrefix = 's3://{bucket_name}/'.format(bucket_name = bucketName)
    s3bucket = getS3Bucket(s3_dsn, bucketName)
    k = Key(s3bucket)
    # access file by relative name: remove bucket prefix
    k.key =  file_name[len(bucketPrefix):]
    json_str= k.get_contents_as_string()
    
    logging.info(json_str)
    return json_str

####################################################################################
# parse configuration Jsom 
####################################################################################

def parseConfigJsonString(json_str):
    s3_json_dict = json.loads(json_str)
    return s3_json_dict
    

def getS3JsonData(config_file_name, config_s3_dns, config_bucket):

    config_data = getS3FileString(config_file_name, config_s3_dns, config_bucket)
    config = parseConfigJsonString(config_data)
    return config

def getGlobalOverrideConfig():
    return getS3JsonData('s3://dbs-data-excavator-reporter-configuration/global/reporter-configuration.json', 'DBS.METRICS.ETL', 'dbs-data-excavator-reporter-configuration')
