import boto3
import gzip
import shutil
import subprocess
import os
import pandas as pd
from io import BytesIO, StringIO
from time import sleep
from qds_sdk.commands import *
from qds_sdk.qubole import Qubole
from qds_sdk.exception import ServerError
from mapytools import credentials
from mapytools.monitoring_utils import hipchat, grafana, stopwatch


sw = stopwatch()

args = sys.argv[0] #
if '/' in args: #
    script_name = args.split('/')[-1].replace('.','_') #
else: #
    script_name = args.replace('.','_') 

def check_bucket(s3_bucket, profile=None):
    """
    Helper function used to determine whether s3_bucket exists or not by attempting to list its contents
    Used by qubole_api.build_call
    """
    try:
        if profile:
            profile_clause = f'--profile={profile}'
        else:
            profile_clause = ''
        assert subprocess.check_output('aws s3 ls {} {}'.format(s3_bucket.split('s3://')[1].split('/')[0],profile_clause),shell=True) is not None
    except Exception:
        raise AssertionError('Is {} a valid bucket?  Bucket must already exist and credentials must allow access.'.format(s3_bucket))

def wait_all(jobs_to_watch=None,wait_interval=5):
    """Employs a time buffer that waits for multiple Hive commands to return a "error", "done", or "cancelled" status.

    :param jobs_to_watch: default None, a list of Hive command IDs to wait for.
    :type query: list of integers
    :param wait_interval: default 5, length of time in seconds to wait before retrieving statuses for all jobs to watch.

    >>> wait_all(jobs_to_watch=[12345,23456,34567],wait_interval=10)
    """
    done = False
    while done == False:
        done_count = 0
        for i in jobs_to_watch:
            s = HiveCommand.find(i).status
            if s in ['error','done','cancelled']:
                done_count += 1
        if done_count == len(jobs_to_watch):
            break
        else:
            sleep(wait_interval)

class qubole_api(object):
    """
    Initializes an object used to interact with the Qubole API.

    Examples
    --------
    >>> from get_dates import month_to_date
    >>> start_date, end_date, report_date = month_to_date()

    >>> query = '''
    >>> select * from mm_impressions_ct
    >>> where organization_id = __organization_id__
    >>> and impression_date between __start_date__ and __end_date__
    >>> limit 100
    >>> '''

    >>> replacements = {'__organization_id__':100667,
    >>> '__start_date__':start_date,
    >>> '__end_date__':end_date}
    >>>
    >>> q = qubole_api()
    >>> q.build_call(query=query,replacements,compression='gzip',s3='s3://mm-analytics-e1/mapytools-testing/qubole_api/')
    >>> q.make_call()
    >>> q.get_results(df=True)
    >>> df = q.df
    """
    
    def __init__(self,account='analytics'):
        """ 
        Only one api_token can be used during a session.
        Usage such as 
        >>> a = qubole_api(api_token=1234)
        >>> b = qubole_api(api_token=5678)
        is not yet supported by this library. 
        """
        self.codecs = {'gzip':'''SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;''',
'bzip2':'''SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;''',
'lzo':'''SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;''',
'snappy':'''SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;'''}
        self.Qubole = Qubole
        c = credentials.get(account=account,protocol='api',name='qubole')
        self.api_token = c['key']
        self.s3_default_bucket = c['default_bucket']
        try: # Since analytics uses a nested bucket/folder/key, but other accounts might not
            self.s3_default_key = c['default_key']
        except:
            self.s3_default_key = ''
        self.Qubole.configure(api_token=self.api_token)
        self.hc = HiveCommand       
 
    def build_call(self,query=None,query_template=None,replacements=None, delimiter='\t', s3=None, compression=None,headers=False, profile=None):
        """Creates a command to be executed by the Qubole API.

        :param query: default None, Command to be executed by Qubole.
        :type query: str
        :param query_template: default None, File containing command(s) to be executed by Qubole.
        :type query_template: str
        :param replacements: default None, a dict object containing keys to search for in the query or query template\
        and values with which to replace them with.
        :type replacements: dict
        :param delimiter: default '\t', specifies character to use in row delimination clause
        :type delimiter: str
        :param s3: default None, s3 bucket url to send results of Qubole command to.\
        Depending on the size of the query results, sending to s3 will yield partitioned files.
        :type s3: str
        :param compression: default None, specifies which codec to use in the compression clause of the Qubole command.
        :type param: str
        - gzip
        - bzip2
        - lzo
        - snappy

        >>> q = qubole_api()
        >>> q.build_call(query='use data pizzas in your dashboards;', replacements={'use data pizzas in your dashboards':'SHOW TABLES;'})
        """
        self.delimiter = delimiter
        if query_template:
            with open(query_template, 'r') as f:
                self.template = f.read()
        elif not query_template and query:
            self.template = query
        else:
            raise Exception('Query string or template must be provided when initializing qubole_api')
            
        if replacements:
            for k in replacements:
                self.template = self.template.replace(k,replacements[k])
        if compression:
            self.compression_clause = '''SET hive.exec.compress.output=true;
{}
SET mapreduce.output.fileoutputformat.compress=true;

'''.format(self.codecs[compression])

        else:
            self.compression_clause = ''
            
        if s3:
            prefix = 's3://'
            if s3[:5] != prefix:
                s3 = prefix + s3
            #check_bucket(s3,profile)
            self.s3_bucket = s3
            self.s3_clause = """INSERT OVERWRITE DIRECTORY '{}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '{}'

"""
        else:
            self.s3_clause = ''
  
        if headers:
            self.header_clause = """SET hive.cli.print.header=true;
"""
        else:
            self.header_clause = ''

        self.query = self.header_clause + self.compression_clause + self.s3_clause.format(s3,delimiter) + self.template

    def make_call(self,label=None):
        """Sends command generated by qubole_api.build_call() to Qubole API."""
        """
        Label parameter allow users to direct a query to run on Hadoop 1 (default cluster) by default, but optionally can run by             cluster label 
        """
        self.hc = HiveCommand.create(query=self.query,label=label)

    def wait(self,display=False,wait_interval=5):
        """Checks the status of the Hive Command in intervals specified by the wait_interval parameter (in seconds).  The method finishes once the status returned is either "error", "done", or "cancelled".

        :param display: default False, when True, response content will be printed.
        :type display: boolean
        :param wait_interval: default 5, length of time in seconds to wait before retrieving a Hive command status.
        :type wait_interval: int

        >>> q = qubole_api()
        >>> q.build_call(query='use data pizzas in your dashboards;', replacements={'use data pizzas in your dashboards':'SHOW TABLES;'})
        >>> q.make_call()
        >>> q.wait(display=True)

        """
        command_id = self.hc.id
        print('Waiting for Qubole command: {}'.format(command_id))
        log_rows_printed = 0
        while True:
            try:
                status = HiveCommand.find(command_id).status
            except ServerError:
                print('Qubole ServerError while fetching status. Retrying.')
                time.sleep(5)
                continue
            if status not in ['error','done','cancelled']:
                time.sleep(5)
                try:
                    log = self.hc.get_log()
                except ServerError:
                    print('Qubole ServerError while fetching log. Retrying.')
                    continue
                # log returns variable number of empty lines. Get rid of them to make counting work
                log_rows = list(filter(None, [line for line in log.split('\n') if not line.isspace()]))
                new_log_rows = log_rows[log_rows_printed:]
                # avoid blank lines if no new log has come in
                if (len(new_log_rows) > 0) & (display == True):
                    #print('\n'.join(new_log_rows))
                    # logs now contain non-ascii characters making everything fail! klynch 2015-10-30
                    print('\n'.join([row for row in new_log_rows]))
                log_rows_printed = len(log_rows)

            else:
                break

        if status != 'done':
            log = HiveCommand.get_log_id(command_id)
            log_rows = list(filter(None, [line for line in log.split('\n') if not line.isspace()]))
            try:
                error_log_rows = log_rows[log_rows_printed-1:]
            except NameError:
                error_log_rows = ''
            error_string = '\n'.join([row for row in error_log_rows])
            raise ValueError('qubole query with id {} failed with status "{}": {}'.format(command_id, status, error_string))
            
    def get_results(self, command_id=None, save_as=None, delimiter=None, df=None, quotechar=None, compression=None):
        """Delimiter is assumed, inherited from self.build_call() unless specified"""

        if delimiter == None:
            try:
                delimiter = self.delimiter # Assuming build_call() was used
            except:
                delimiter = None # If build_call() wasn't used
        if not command_id:
            command_id = self.hc.id
            self.wait()
        q = self.hc.find(command_id)
        results_path = q.attributes['path']
        if self.s3_default_key == '':
            results_path = results_path[1:] # If there isn't a folder in the bucket, just use the results path, but cleaned up
        s = boto3.client('s3')
        try:
            results_objs = [obj['Key'] for obj in s.list_objects(Bucket=self.s3_default_bucket, Prefix=self.s3_default_key + results_path)['Contents']]
        except KeyError:
            print(q.hc.get_log())
        # We always want this header data...
        header = s.get_object(Bucket=self.s3_default_bucket, Key=self.s3_default_key + results_path)['Body'].read().decode()
        headers = header.rstrip('\n').split('\t')
        # ...but what's under this object determines how we proceed
        if any('.dir/' in obj for obj in results_objs): # If this is true, we have to get results differently
            data = []
            results_data_objs = [obj for obj in results_objs if ('.dir' in obj) and ('$folder$' not in obj)]
            for obj in results_data_objs:
                data.append(s.get_object(Bucket=self.s3_default_bucket, Key=obj)['Body'].read().decode())
            data = ''.join(data) # Reassigning variable to dump list from memory
            if df:
                self.df = pd.read_csv(StringIO(data),delimiter=chr(1),names=headers)
                print('SAMPLE RESULTS:')
                print(self.df.head())
            if delimiter:
                if quotechar:
                    header = quotechar + header.replace('\t',quotechar+'\t'+quotechar).replace('\n',quotechar+'\n')
                    data = quotechar + data.replace('\x01',quotechar+'\x01'+quotechar).replace('\n',quotechar+'\n'+quotechar).rstrip('"')
                results_data = header.replace('\t', delimiter) + data.replace('\x01', delimiter).rstrip('\n')
            else:
                results_data = header + data.replace('\x01',delimiter).rstrip('\n')
        else: # We just care about the header file in this case
            if df:
                self.df = pd.DataFrame(data=[row.split('\t') for row in header.split('\n')[1:-1]],
                                       columns=header.split('\n')[0].split('\t'))
                print('SAMPLE RESULTS')
                print(self.df.head())
            if delimiter:
                if quotechar:
                    header = quotechar + header.replace('\t',quotechar+'\t'+quotechar).replace('\n',quotechar+'\n'+quotechar)
                    data = quotechar + data.replace('\x01',quotechar+'\x01'+quotechar).replace('\n',quotechar+'\n'+quotechar).rstrip(quotechar)
                results_data = header.replace('\t', delimiter).rstrip('\n')
            else:
                results_data = header
        if save_as:
            with open(save_as, 'w') as results:
                results.write(results_data)
            if compression and compression.lower() == 'gzip':
                with open(save_as, 'rb') as f_in:
                    with gzip.open(save_as+'.gz', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(save_as)
            elif compression and compression.lower() != 'gzip':
                raise(Exception('Only gzip compression is currently supported by this method.'))