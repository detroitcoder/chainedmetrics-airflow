import logging
import sys
import smtplib
import os

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from jinja2 import Template


def configure_logging():
    '''Configures logging to go to standard out'''

    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def email_table_template(title, description, headers, rows):

    template_path = os.path.join(os.path.dirname(__file__), 'email_templates', 'notification-summary.html')
    with open(template_path) as fil:
        template_str = fil.read()
        template = Template(template_str)
    
    return template.render(title=title, description=description, headers=headers, rows=rows)

def send_email(email, from_email, html, subject, text, email_pass):

    if not isinstance(email, list):
        email = [email]
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ", ".join(email)

    # Create the body of the message (a plain-text and an HTML version).
    text = "Hi!\nHow are you?\nHere is the link you wanted:\nhttp://www.python.org"
 
    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    image_location = os.path.join(os.path.dirname(__file__), 'email_templates', 'chained-metrics-light.png')
    with open(image_location, 'rb') as fil:
        image_data = fil.read()
    
    img = MIMEImage(image_data, 'chained-metrics-light.png')
    img.add_header('Content-ID', '<logo-light>')
    msg.attach(img)

    # Send the message via local SMTP server.
    mail = smtplib.SMTP_SSL('smtp.gmail.com', 465)

    mail.ehlo()

    mail.login(from_email, email_pass)
    mail.sendmail(from_email, email, msg.as_string())
    mail.quit()