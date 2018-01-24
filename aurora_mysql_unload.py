import paramiko
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder

from string import Template
import sys, getopt, logging, os, ast, csv, tempfile, gzip

from db_services_metrics import helpers


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def main():

    params = parse_parameters(sys.argv)

    f = open(params['file'],'r')
    s = " ".join( line.rstrip('\n') for line in f.readlines())
    params['sql'] = Template(s).safe_substitute(params)

    logging.info("Connecting to database...")
    
    db_conn =helpers.get_connection(params['dsn'])
    logging.info("Connected.")

    logging.info("Connecting to S3...")
    s3_conn = helpers.get_connection(params['s3account'])
    logging.info("Connected.")

    s3bucket = s3_conn.get_bucket(params['bucket'])
    s3key = s3bucket.new_key(params['s3path'])

    curs = db_conn.cursor()
    #print params['sql']
    curs.execute(params['sql'])

    tempfile.tempdir = params['tmpdir']
    tmp = tempfile.NamedTemporaryFile(delete=False)
    gzip_tmp = gzip.GzipFile(fileobj=tmp)
    logging.info("Create temporary file: " + tmp.name)
    wr = csv.writer(gzip_tmp, **params['csv_parameters'])
    logging.info("Writing columns...")
    logging.info([col[0] for col in curs.description])
    wr.writerow([col[0] for col in curs.description])

    logging.info("Writing data...")
    curs.arraysize = 1000000
    for row_data in curs:
        wr.writerow(row_data)
    gzip_tmp.close()
    tmp.close()
    logging.info("Writing data complete.")

    logging.info("UNLOAD data to S3: " + params['s3path'])
    s3key.set_contents_from_filename(tmp.name)
    logging.info("UNLOAD complete.")

    os.remove(tmp.name)
    db_conn.close();
    logging.info("Disconnected.")


def parse_parameters(arguments):

    usage = arguments[0] + '-c <Connection DSN> -f <SQL script file> -a <S3 Account Name> -p <S3 Path> -m <Map of script parameters> -d <Delimiter> -t <Temp Dir> -e <CSV dump parameters>'

    try:
        opts, args = getopt.getopt(arguments[1:],"hc:f:a:p:m:d:t:e:")
    except getopt.GetoptError:
        logging.error('Invalid parameters\n    Usage:{}'.format(usage))
        sys.exit(2)

    params = dict()
    csv_extract_params = dict()
    #default options
    csv_extract_params['delimiter'] = "|"

    for opt, arg in opts:
        if opt == '-h':
            logging.info(usage)
            sys.exit()
        elif opt in ("-c"):
            params['dsn'] = arg
        elif opt in ("-f"):
            params['file'] = arg
        elif opt in ("-a"):
            params['s3account'] = arg
        elif opt in ("-p"):
            _, path = arg.split(":", 1)
            params['bucket'], params['s3path'] = path.lstrip('/').split('/', 1)
        elif opt in ("-m"):
            params.update(ast.literal_eval(arg))
        elif opt in ("-e"):
            csv_extract_params.update(ast.literal_eval(arg))
        elif opt in ("-d"):
            csv_extract_params['delimiter'] = arg
        elif opt in ("-t"):
            params['tmpdir'] = arg
        else:
            assert False, "unhandled option"
        params["csv_parameters"] = csv_extract_params
    logging.info("Parsed Paramaters:")
    logging.info(params)
    return params

if __name__ == "__main__":
    main()

