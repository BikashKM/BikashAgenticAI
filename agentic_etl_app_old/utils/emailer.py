from __future__ import annotations
from typing import List, Optional
import smtplib, ssl, os, boto3
from email.message import EmailMessage

def send_email_smtp(host: str, port: int, username: Optional[str], password: Optional[str], sender: str, recipients: List[str], subject: str, html_body: str, attachments: Optional[List[str]] = None, use_tls: bool=True):
    msg = EmailMessage()
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.set_content('This is an HTML report.')
    msg.add_alternative(html_body, subtype='html')
    for path in attachments or []:
        if not os.path.exists(path): continue
        with open(path, 'rb') as f:
            data = f.read()
        fname = os.path.basename(path)
        maintype, subtype = ('application','octet-stream')
        if fname.lower().endswith('.pdf'): maintype, subtype = ('application','pdf')
        if fname.lower().endswith('.html'): maintype, subtype = ('text','html')
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)
    ctx = ssl.create_default_context()
    with smtplib.SMTP(host, port) as s:
        if use_tls: s.starttls(context=ctx)
        if username and password:
            s.login(username, password)
        s.send_message(msg)

def send_email_ses(region: str, sender: str, recipients: List[str], subject: str, html_body: str, attachments: Optional[List[str]] = None):
    ses = boto3.client('ses', region_name=region)
    if attachments:
        em = EmailMessage()
        em['From'] = sender; em['To'] = ', '.join(recipients); em['Subject'] = subject
        em.set_content('This is an HTML report.')
        em.add_alternative(html_body, subtype='html')
        for path in attachments:
            if not os.path.exists(path): continue
            with open(path, 'rb') as f:
                data = f.read()
            fname = os.path.basename(path)
            maintype, subtype = ('application','octet-stream')
            if fname.lower().endswith('.pdf'): maintype, subtype = ('application','pdf')
            if fname.lower().endswith('.html'): maintype, subtype = ('text','html')
            em.add_attachment(data, maintype=maintype, subtype=subtype, filename=fname)
        ses.send_raw_email(RawMessage={'Data': em.as_bytes()})
    else:
        ses.send_email(Source=sender, Destination={'ToAddresses': recipients},
                       Message={'Subject': {'Data': subject}, 'Body': {'Html': {'Data': html_body}}})
