"""
CREDENTIALS.PY
Get credentials for analytics-related things.
"""

import boto3
from datetime import datetime
from getpass import getuser

def load_credentials(client='',protocol='all',name='all'):
    dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    table = dynamodb.Table('') 
    if client != 'all':
        try:
            response = table.get_item(
                Key={
                    'client': client
                }
            )
            if protocol != 'all' and name == 'all':
                cred = response['Item'][protocol]
            elif protocol != 'all' and name != 'all':
                cred = response['Item'][protocol][name]
            else:
                cred = response['Item']
        
            if 'port' in cred.keys():
                cred['port'] = int(cred['port'])

            return cred

        except Exception:
            raise Exception("Could not load credentials for the specified client.  Verify the client you are trying to load.")
    else:
        return table.scan()['Items']

class credentials:
    '''
    Updated tool for managing credentials using AWS Secrets Manager.  Anything done with this module directly affects the credential storage.
    '''
    
    def __init__(self):
        self.client = boto3.client(
        service_name='secretsmanager',
        region_name='us-east-1',
        endpoint_url='https://secretsmanager.us-east-1.amazonaws.com'
        )
        self.user = getuser()
        
    def add(self,account=None,protocol=None,name=None,payload=None):
        """
        Will add credentials in payload to Secrets Manager storage.
        An error will be returned if credentials for account, protocol, and name already exist.
        In this case, the update() method will need to be used instead.
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        :param payload: Dictionary of credentials to be stored, e.g. {account:'me',password:'thisisagreatpasswword',key:'picklerick'}
        :type payload: dict
        """
        
        self.key = f'{account.lower()}/{protocol.lower()}/{name.lower()}'
        self.prepare(payload=payload)
        payload['last_modified_by'] = self.user
        payload['last_modified_time'] = str(datetime.now())
        self.client.create_secret(Name=self.key,SecretString=str(payload))
        print('Added:',self.key)

    def clear(self,account=None,protocol=None,name=None):
        """
        Does not actually DELETE credentials, but instead 
        sets the payload to a blank string while retaining the account, protocol, and name.
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        """
        
        self.key = f'{account}/{protocol}/{name}'
        self.client.update_secret(SecretId=self.key,SecretString=' ')
        print('Cleared:',self.key)

    def get(self,account=None,protocol=None,name=None):
        """
        Requires an entry for the specified account, protocol, and name to already be stored.
        A botocore error will otherwise be returned.
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        """        

        self.key = f'{account}/{protocol}/{name}'
        secret = self.client.get_secret_value(SecretId=self.key)['SecretString']
        if secret == ' ':
            raise(KeyError(f'Namespace exists, but the payload is empty.  Please update {self.key} with current credentials.'))
        else:
            return eval(secret)

    def list_all(self):
        secrets = []
        
        first_page = self.client.list_secrets()
        for i in first_page['SecretList']:
            secrets.append(i['Name'])

        next_page = self.client.list_secrets(NextToken=first_page['NextToken'])

        while 'NextToken' in next_page:
            for i in next_page['SecretList']:
                secrets.append(i['Name'])
            next_page = self.client.list_secrets(NextToken=next_page['NextToken'])

        for i in next_page['SecretList']:
            secrets.append(i['Name'])
        
        return sorted(secrets)
    
    def prepare(self,account=None,protocol=None,name=None,payload=None):
        """
        Helper function used to 
        - assemble SecretId using account, protocol, and name
        - check payload structure before adding/updating
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        :param payload: Dictionary of credentials to be stored, e.g. {account:'me',password:'thisisagreatpasswword',key:'picklerick'}
        :type payload: dict
        """
        
        if account and protocol and name:
            print(f'StringId: {account}/{protocol}/{name}')
            
        try:
            assert type(payload) == dict
        except AssertionError:
            raise(TypeError('Please pass payload as a dictionary'))
                
    def rename(self,old_account=None,old_protocol=None,old_name=None,new_account=None,new_protocol=None,new_name=None):
        """
        Allows users to move credentials from one account, protocol, and name combination key to another.
        """      
        
        old_key = f'{old_account}/{old_protocol}/{old_name}'
        new_key = f'{new_account}/{new_protocol}/{new_name}'
        
        try:
            assert any([(old_account != new_account), (old_protocol != new_protocol), (old_name != new_name)])
        except AssertionError:
            raise(KeyError('Nothing has been renamed!'))
        print(old_key)
        old_secret = self.client.get_secret_value(SecretId=old_key)['SecretString']
        if old_secret == ' ':
            raise(KeyError('No credentials exist for the specified account, protocol, and name you are using to replace!'))
        payload = eval(old_secret)
        payload['last_modified_by'] = self.user
        payload['last_modified_time'] = str(datetime.now())
        self.prepare(payload=payload)
        try:
            self.add(account=new_account,protocol=new_protocol,name=new_name,payload=payload)
        except Exception:
            self.update(account=new_account,protocol=new_protocol,name=new_name,payload=payload)
            print(f'Warning: {new_key} may have been overwritten by {old_key}')
            
        self.clear(account=old_account,protocol=old_protocol,name=old_name)

    def search(self,account=None,protocol=None,name=None):
        """
        Filters through all stored credential keys to find any matching the specified account, protocol, and/or name.
        Prints any matches, along with a pre-populated method and parameters 
        which can be copied and pasted, and used with a credentials() instance.
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        """      
        
        #written at 1AM... this could be way leaner...

        secrets = self.list_all()
        
        for secret in secrets:
            s_account, s_protocol, s_name = secret.split('/')

            if account == None:
                s_account = None
            if protocol == None:
                s_protocol = None
            if name == None:
                s_name = None
            
            if s_account == account and s_protocol == protocol and s_name == name:
                s_account, s_protocol, s_name = secret.split('/')
                #print('account:',s_account)
                #print('Protocol:',s_protocol)
                #print('Name:',s_name)
                print(f"- get(account='{s_account}',protocol='{s_protocol}',name='{s_name}')")
                #print()
        
    def update(self,account=None,protocol=None,name=None,payload=None):
        """
        Updates the payload for the specified account, protocol, and name.
        
        :param account: Name of person, client, or owner associated with the specified credentials
        :type account: str
        :param protocol: Type of connection the credentials are used for, e.g. AWS Secrets, FTP credentials, email credentials, API keys
        :type protocol: str
        :param name: Name of service, platform, or usage keyword to describe credentials, e.g. Qubole, internal, external
        :type name: str
        :param payload: Dictionary of credentials to be stored, e.g. {account:'me',password:'thisisagreatpasswword',key:'picklerick'}
        :type payload: dict
        """        

        self.key = f'{account}/{protocol}/{name}'
        payload['last_modified_by'] = self.user
        payload['last_modified_time'] = str(datetime.now())
        self.prepare(payload=payload)
        self.client.update_secret(SecretId=self.key,SecretString=str(payload))
        print('Updated:',self.key)
    
credentials = credentials()