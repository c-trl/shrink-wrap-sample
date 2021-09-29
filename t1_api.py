import boto
import json
import requests
import pandas as pd
import sys
from io import BytesIO
from mapytools import credentials
from mapytools.monitoring_utils import grafana, stopwatch

sw = stopwatch()

args = sys.argv[0] #
if '/' in args: #
    script_name = args.split('/')[-1].replace('.','_') #
else: #
    script_name = args.replace('.','_') 

def api_login():
    """Logs in to the MM API using partnerapi@mediamath.com credentials"""

    api_creds = credentials.get(account='analytics',protocol='api',name='t1')
    user = api_creds['user']
    password = api_creds['password']
    api_key = api_creds['key']
    payload = {'user' : user, 'password' : password, 'api_key' : api_key}
    s = requests.session()
    s.post('https://api.mediamath.com/api/v2.0/login',data=payload)
    return s

class t1_api:
    """Initializes an object used to interact with the T1 Reports API.

    Examples
    --------
    >>> params = {
    'filter': 'organization_id=100667&advertiser_id=157428',
    'dimensions': 'campaign_name,campaign_id',
    'metrics': 'impressions,clicks,total_spend,total_ad_cost,platform_access_fee,managed_service_fee,optimization_fee,pmp_no_opto_fee,pmp_opto_fee,mm_margin_share',
    'time_rollup': 'all',
    'start_date': '2017-09-01',
    'end_date': '2017-09-01'}
    >>>
    >>> t1 = t1_api(endpoint='performance')
    >>> t1.build_call(params)
    >>> t1.make_call(df=True,save_as='test.csv',s3='ctirol/t1_api_test')"""

    def __init__(self,endpoint='performance'):
        self.endpoint = endpoint
        s = self.api_login()
        picard = 'https://api.mediamath.com/reporting/v1/std/'
        r = s.get(picard + 'meta')
        self.available_endpoints = list(json.loads(r.content)['reports'].keys())
        self.available_endpoints.extend(['performance_usd','performance_viewability','site_transparency_viewability','deals'])
        try:
            assert endpoint in self.available_endpoints
            self.endpoint = endpoint
            if self.endpoint == 'deals': # This is while 'deals' is in beta
                self.info = json.loads(s.get('https://api.mediamath.com/reporting-beta/v1/std/deals/meta').content)
            else:
                self.info = json.loads(s.get(picard + endpoint + '/meta').content)
        except AssertionError:
            raise AssertionError('{} is not a supported endpoint.  Endpoint must be one of the following: {}'.format(endpoint,self.available_endpoints))

    def build_call(self,*args,**kwargs): 
        """Builds an HTTP call to make to the T1 API given the endpoint specified when initializing the T1 object.
        
        :param args: Keyword variables accepted by the T1 API set to single-string values
        :type args: str
        :param kwargs: Dict of parameter-keys accepted by the T1 API with single-string values  
        :type kwargs: str

        >>> t1.build_call(filter='organization_id=100667&advertiser_id=157428', dimensions='campaign_name,campaign_id')  
        >>> t1.build_call({'filter':'organization_id=100667&advertiser_id=157428','dimensions': 'campaign_name,campaign_id'})"""
        
        self.params = {}
        for k in args:
            for v in k:
                if type(k[v]) is list:
                    self.params[v] = ','.join(k[v])
                else:
                    self.params[v] = k[v]
        for k in kwargs:
            if type(k) is list:
                self.params[k] = ','.join(kwargs[k])
            else:
                self.params[k] = kwargs[k]      
    
    def make_call(self,display=False,df=False,s3=False,save_as=None):
        """Send an API request to the endpoint specified when initializing the T1 object.
        
        :param display: default False, returns None but prints an HTTP response as it is received to console. 
        :type display: boolean
        :param df: default False, returns a pandas.DataFrame() object of the API response.  Can be invoked without saving the response to a local file.
        :type df: boolean
        :param s3: default None, s3 bucket url to save API response to.
        :type s3: str
        :param save_as: default None, filename tosave API response to locally.
        :type save_as: str
       
        >>> t1.make_call(df=True,save_as='test.csv',s3='s3://mm-analytics-e1/t1-api-test')
        >>> print(t1.df.head())"""
        
        picard = 'https://api.mediamath.com/reporting/v1/std/'
        s = self.api_login()
        all_reports_metadata = s.get(picard + 'meta')

        if self.endpoint == 'transparency': 
            self.endpoint = 'site_transparency'

        # Account for the fact that some endpoints aren't in Picard metadata
        if self.endpoint == 'performance_usd':
            data_url = picard + 'performance_usd'
        elif self.endpoint == 'performance_viewability':
            data_url = picard + 'performance_viewability'
        elif self.endpoint == 'site_transparency_viewability':
            data_url = picard + 'site_transparency_viewability'
        elif self.endpoint == 'deals':
            data_url = 'https://api.mediamath.com/reporting-beta/v1/std/deals' # This is while 'deals' is in beta
        else:
            try:
                report_metadata = all_reports_metadata.json()['reports'][self.endpoint]
                try:
                    data_url = report_metadata['URI_Data']
                except KeyError:
                    raise ValueError('No data URI found for API endpoint: {}'.format(self.endpoint))
            except KeyError:
                raise ValueError('No metadata found for API endpoint: {}'.format(self.endpoint))

        try:
            sw.start()
            self.response = s.get(data_url, params=self.params, headers={'Accept-Encoding':'identity','Connection':'close'})
            sw.stop()
            grafana.send_stats(f'e1.mapytools.runtimes.t1api.performance.{script_name}',sw.seconds)
        except Exception:
            raise(Exception)        

        if self.response.status_code == 200:
            self.data = self.response.content
            if display:
                print(self.data)
            if df:
                self.df = pd.read_csv(BytesIO(self.data))
            if save_as:
                with open(save_as,'wb') as f:
                    f.write(self.data)
            if s3:
                try:
                    assert save_as
                except AssertionError:
                    raise AssertionError('In order to send to the specified S3 bucket, the file must be saved with a filename.  Set save_as to the filename you want to use, e.g. save_as="t1response.csv"')
                if s3[:5] == 's3://':
                    s3 = s3.lstrip('s3://')
                if '/' in s3:
                    folder = s3.split('/',1)[0]
                    subfolders = s3.split('/',1)[1]
                    if subfolders[-1] != '/':
                        subfolders += '/'
                else:
                    folder = s3
                    subfolders = ''
                print((folder,subfolders,save_as))
                session = boto3.resource('s3')
                bucket = session.Bucket(folder)
                bucket.put_object(Key=subfolders+save_as,Body=self.data,ACL='bucket-owner-full-control') #Key=where to put it, Body=file to put
        else:
            #raise ValueError('Params {0} for {1} report returned {2} status code.\n{3}'.format(params, self.endpoint, self.response.status_code, self.response.text))
            raise ValueError(self.response.text)
