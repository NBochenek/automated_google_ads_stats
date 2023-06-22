import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from keys import email_password


# Function to send an email
def send_email(subject, body):
    # Email configuration
    sender_email = "nick.bochenek@nonprofitmegaphone.com"  # Set your own email address here
    receiver_email = "nick.bochenek@nonprofitmegaphone.com"  # Set your own email address here
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_username = "nick.bochenek@nonprofitmegaphone.com"  # Set your own email address here
    smtp_password = email_password  # Set your own email password here

    # Construct the email message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    # Create a secure connection to the SMTP server
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, message.as_string())

if __name__ == "__main__":
    # Send an email notification to yourself
    subject = "Test Message"
    body = "Test"
    send_email(subject, body)
