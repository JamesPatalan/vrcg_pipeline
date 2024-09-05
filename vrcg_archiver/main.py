import json
import email
import config as cfg
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email import encoders
import logging
# import google.cloud.logging
from google.cloud import storage


# Set up logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')
# logging_client = google.cloud.logging.Client()
# logging_client.setup_logging()

def archive_emails(un, pw, im):
    # Connect to the IMAP server
    mail = imaplib.IMAP4_SSL(im)
    mail.login(un, pw)
    mail.select("inbox")  # Select the inbox folder

    # Search for all emails in the inbox
    result, data = mail.search(None, "ALL")

    if result == 'OK':
        for num in data[0].split():
            # Move the email to the 'All Mail' folder and remove from 'Inbox'
            result = mail.store(num, '+X-GM-LABELS', '\\All')
            if result[0] == 'OK':
                result = mail.store(num, '+FLAGS', '\\Deleted')
                if result[0] == 'OK':
                    mail.expunge()
                    print(f"Email {num} archived successfully.")
                else:
                    print(f"Failed to delete email {num} from inbox.")
            else:
                print(f"Failed to archive email {num}.")

    mail.close()
    mail.logout()

def main():
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(cfg.bucket)
    blob = bucket.blob(cfg.blob)

    keys = json.loads(blob.download_as_string())
    un = keys['UN']
    pw = keys['PW']
    im = keys['IM']

    archive_emails(un = un, pw = pw, im = im)

if __name__ == '__main__':
    main()
