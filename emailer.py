import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from os.path import basename, expanduser
from email import encoders as Encoders
from email.utils import formatdate
from email.mime.text import MIMEText

def send_email(msg_from='', msg_to=None, msg_cc=None, msg_bcc=None, subject=None, body='', attachments=None,connection_settings=None,noreply=False,footer=True):
    
    def make_list(text_or_list):
        if isinstance(text_or_list, list):
            return text_or_list
        else:
            return [text_or_list]
    
    msg_from = ""
    
    # Build an email
    msg = MIMEMultipart('alternative')
    
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body + CONF_FOOTER))
    
    if msg_from:
        msg['From'] = ','.join(make_list(msg_from))
    if msg_to:
        msg['To'] = ','.join(make_list(msg_to))
    if msg_cc:
        msg['Cc'] = ','.join(make_list(msg_cc))
    if msg_bcc:
        msg['Bcc'] = ','.join(make_list(msg_bcc))
    
    # What a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'
    
    if noreply:
        msg.add_header('reply-to', 'noreply@mediamath.com')
    
    msg['Date'] = formatdate(localtime=True)
    
    if attachments:
        attachments = make_list(attachments)
        for attachment in attachments:
            with open(expanduser(attachment), 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename='
                            '"{}"'.format(basename(attachment)))
            msg.attach(part)
    
    # Connect to Amazon SES
    ses = boto3.client(
        'ses',
        region_name='us-east-1',
    )
    # And finally, send the email
    ses.send_raw_email(
        Source=msg_from,
        RawMessage={
            'Data': msg.as_string(),
        }
    )