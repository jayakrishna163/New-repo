# Databricks notebook source
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(subject, body, to_email, from_email, smtp_server, smtp_port, login, password):
    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    
    # Attach the email body
    msg.attach(MIMEText(body, 'plain'))
    
    # Connect to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(login, password)
    
    # Send the email
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()

# Example usage
subject = "Anomaly Detected in ETL Job"
body = "An anomaly was detected in the ETL job run. Please check the logs for more details."
to_email = "vutukurijayakrishna66@gmail.com"
from_email = "jayakrishna.vutukuri@alephys.com"
#smtp_server = "smtp.example.com"
#smtp_port = 587
login = "jayakrishna.vutukuri@alephys.com"
password = "Alephys@123"

send_email(subject, body, to_email, from_email, smtp_server, smtp_port, login, password)


# COMMAND ----------


