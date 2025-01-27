import pandas as pd
from fuzzywuzzy import process
import imaplib
import email
from email.header import decode_header
from io import BytesIO
import json
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText # New
from email.mime.image import MIMEImage # New
from email import encoders
from datetime import datetime
import logging
# import google.cloud.logging
from google.cloud import bigquery
from google.cloud import storage
import numpy as np
from functools import lru_cache
import config as cfg

# Set up logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')
# logging_client = google.cloud.logging.Client()
# logging_client.setup_logging()

# Define a mapping of common column names to variations
column_mapping = {
    'Supplier': ['Supplier'],
    'State': ['state', 'st'],
    'Location': ['location', 'location name', 'city'],
    'Year': ['year', 'yr'],
    'Make': ['make', 'manufacturer', 'mfg'],
    'Model': ['model', 'car model'],
    'Series': ['series', 'model series', 'trim'],
    'Mileage': ['mileage', 'miles'],
    'Color': ['color', 'exterior'],
    'VIN': ['vin'],
    'Price': ['saleprice', 'price', 'cost'],
    'VDA': ['VDA'],
    'Grade': ['grade', 'cr']
}

make_mapping = {
    'Audi': ['AUDI', 'AUD'],
    'Bmw': ['BMW'],
    'Cadillac': ['CADI', 'Cadillac'],
    'Chevrolet': ['CHV', 'Chevrolet'],
    'Jeep': ['JEE', 'Jeep'],
    'Toyota': ['TOY', 'Toyota'],
    'Ford': ['FOR', 'Ford'],
    'Mercedes-Benz': ['MB', 'Mercedes-Benz'],
    'Chrysler': ['CHR', 'car model'],
    'Dodge': ['DOD', 'Dodge'],
    'Honda': ['HOND', 'Honda'],
    'Hyundai': ['HYUN', 'HYUNDAI'],
    'Kia': ['KIA'],
    'Gmc': ['GMC'],
    'Nissan': ['NIS', 'Nissan'],
    'Lincoln': ['LIN', 'Lincoln'],
    'Maserati': ['MAZ', 'Maserati'],
    'Mini': ['MIN', 'Mini', 'MINI Cooper'],
    'Mitsubishi': ['mitsubishi', 'MITS'],
    'RAM': ['RAM'],
    'Volvo': ['VOL', 'Volvo'],
    'Volkswagen': ['VW', 'Volkswagen']
}

city_to_state_mapping = {
    'AL': ['BIRMINGHAM'],
    'AR': ['LITTLE ROCK'],
    'GA': ['ATLANTA'],
    'CA': ['SOUTHERN CA', 'RIVERSIDE', 'GOLDEN GATE', 'VAN NUYS', 'SAN FRANCISCO', 'LOS ANGELES'],
    'TX': ['DALLAS', 'HOUSTON', 'SAN ANTONIO', 'TEXAS HOBBYS', 'AUSTIN'],
    'FL': ['MIAMI', 'PALM BEACH', 'ORLANDO', 'TAMPA', 'KEY WEST', 'JACKSONVILLE', 'FT LAUDERDALE', 'CLERMONT', 'WEST PA'],
    'NV': ['NEVADA', 'LAS VEGAS'],
    'PA': ['PENNSYLVANIA', 'PHILADELPHIA', 'PITTSBUR'],
    'WA': ['SEATTLE'],
    'UT': ['UTAH'],
    'AZ': ['PHOENIX', 'SKY HARBOR'],
    'HI': ['WAIKIKI', 'HONOLULU', 'LIHUE', 'KALAKAUA'],
    'NJ': ['NEW JERSEY', 'GLASSBORO', 'NEWARK'],
    'NY': ['BUFFALO', 'SYRACUSE'],
    'MA': ['NEW ENGLAND', 'BOSTON'],
    'OR': ['PORTLAND'],
    'IL': ['CHICAGO'],
    'CO': ['DENVER'],
    'MO': ['KANSAS CITY'],
    'MI': ['DETROIT'],
    'MS': ['GULFPORT'],
    'NC': ['STATESVILLE'],
    'TN': ['NASHVILLE'],
    'VA': ['REAGAN', 'DC', 'DULLES'],
    'LA': ['NEW ORLEANS'],
    'NM': ['NEW MEXICO']
}

region_mapping = {
    'North East': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE', 'MD', 'WV', 'VA'],
    'South East': ['NC', 'SC', 'TN', 'MS', 'AL', 'GA', 'FL'],
    'Southern': ['TX', 'LA', 'AR', 'OK'],
    'Midwest': ['OH', 'MI', 'KY', 'IN', 'IL', 'MO', 'IA', 'WI', 'MN'],
    'Mountain': ['ND', 'SD', 'NE', 'KS', 'CO', 'WY', 'MT', 'UT', 'ID'],
    'Southwest': ['NV', 'CA', 'AZ', 'NM'],
    'Northwest': ['WA', 'OR'],
    'AlaskaHawaii': ['AK', 'HI']
}

supplier_name_mapping = {
    'em.com' : 'Enterprise',
    'foxrentacar.com' : 'Fox',
    'hertz.com' : 'Hertz',
    'sixt.com' : 'Sixt',
    'tulsacoxmail.com' : 'Avis (On Rent)'
}


def get_best_match(col_name, choices):
    match, score = process.extractOne(col_name, choices)
    return match if score >= 80 else None  # Adjust the threshold as needed


def map_data(df):
    # Drop enterprise age column
    if 'age' in df.columns.str.lower():
        df = df.drop(columns=[col for col in df.columns if col.lower() == 'age'])
    
    # Initialize a dictionary to store the extracted data
    car_data_dict = {common_col: [] for common_col in column_mapping.keys()}

    for common_col, variants in column_mapping.items():
        best_match = None
        for variant in variants:
            match = get_best_match(variant.lower(), df.columns)
            if match:
                best_match = match
                break
        if best_match:
            car_data_dict[common_col] = df[best_match]
        else:
            car_data_dict[common_col] = pd.Series([None] * len(df))  # Append None if no match is found

    car_data_df = pd.DataFrame(car_data_dict)

    # Ensure columns are dtype float64
    if 'Price' in car_data_df.columns:
        car_data_df['Price'] = pd.to_numeric(car_data_df['Price'], errors='coerce')
    if 'VDA' in car_data_df.columns:
        car_data_df['VDA'] = pd.to_numeric(car_data_df['VDA'], errors='coerce')
    if 'Mileage' in car_data_df.columns:
        car_data_df['Mileage'] = pd.to_numeric(car_data_df['Mileage'], errors='coerce')
    if 'Year' in car_data_df.columns:
        car_data_df['Year'] = pd.to_numeric(car_data_df['Year'], errors='coerce')
    if 'Grade' in car_data_df.columns:
        car_data_df['Grade'] = pd.to_numeric(car_data_df['Grade'], errors='coerce').astype('str')

    return car_data_df


def fetch_data(email_ids, mail):
    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()

    for email_id in email_ids:
        # Fetch the email by ID
        status, msg_data = mail.fetch(email_id, "(RFC822)")

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])

                # Decode the email subject
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else "utf-8")

                # Get the sender's email address
                from_email = email.utils.parseaddr(msg["From"])[1]
                domain = from_email.split('@')[-1]  # Extract domain from email address

                # Check if the email has attachments
                if msg.is_multipart():
                    for part in msg.walk():
                        content_disposition = part.get("Content-Disposition", None)

                        # If the part is an attachment
                        if content_disposition:
                            dispositions = content_disposition.strip().split(";")

                            if "attachment" in dispositions:
                                # Get the filename
                                filename = part.get_filename()
                                if filename:
                                    filename, encoding = decode_header(filename)[0]
                                    if isinstance(filename, bytes):
                                        filename = filename.decode(encoding if encoding else "utf-8")

                                    # Extract the attachment
                                    file_data = part.get_payload(decode=True)

                                    # Load the attachment into a pandas DataFrame
                                    if filename.endswith(".xlsx") or filename.endswith(".xls"):
                                        file_stream = BytesIO(file_data)
                                        df = pd.read_excel(file_stream)

                                        # Add a 'Supplier' column with the cleaned domain of the sender
                                        supplier_name = supplier_name_mapping.get(domain, domain) # Will default to domain if not found in the map
                                        df['Supplier'] = supplier_name
                                        
                                        # Drop error causing columns
                                        if 'Due Location Date' in df.columns:
                                            df = df.drop('Due Location Date', axis='columns')

                                        # Map data
                                        mapped_df = map_data(df)

                                        # Append the data to the all_data DataFrame
                                        all_data = pd.concat([all_data, mapped_df], ignore_index=True)

                                    # Account for XLSB
                                    elif filename.endswith(".xlsb"):
                                        file_stream = BytesIO(file_data)
                                        df = pd.read_excel(file_stream, engine='pyxlsb')
                                        supplier_name = supplier_name_mapping.get(domain, domain)
                                        df['Supplier'] = supplier_name
                                        mapped_df = map_data(df)
                                        all_data = pd.concat([all_data, mapped_df], ignore_index=True)

    return all_data


def map_makes(df): # Vectorized version
    df['Make'] = df['Make'].astype(str)
    
    # Create lookup dictionary once
    lookup = {variant.upper(): make 
             for make, variants in make_mapping.items() 
             for variant in variants}
    
    # Vectorized operation using map with cached function
    @lru_cache(maxsize=1000)
    def find_make(make):
        make_upper = make.upper()
        best_match = get_best_match(make_upper, lookup.keys())
        return lookup.get(best_match, make) if best_match else make
    
    df['Make'] = df['Make'].map(find_make)
    return df


def map_state_from_location(row):
    if pd.isna(row['Location']):  # Handle NaN values
        return ''

    if pd.isna(row['State']) or row['State'] == None or row['State'] == '':
        location_upper = row['Location'].upper()
        for state, cities in city_to_state_mapping.items():
            for city in cities:
                if city in location_upper:
                    return state
        return ''
    else:
        return row['State']


def map_state_to_region(state):
    for region, states in region_mapping.items():
        if state in states:
            return region
    return 'Unknown'  # Return 'Unknown' if the state is not found in the mapping


def check_unread_emails(un, pw, im):
    un = un
    pw = pw
    im = im

    mail = imaplib.IMAP4_SSL(im)
    mail.login(un, pw)
    mail.select("inbox")

    # Search for all unread emails
    status, messages = mail.search(None, 'UNSEEN')

    # Convert messages to a list of email IDs
    unread_email_ids = messages[0].split()

    if unread_email_ids:
        # There are new emails
        status, all_messages = mail.search(None, 'ALL')
        email_ids = all_messages[0].split()
        return email_ids, mail

    else:
        # No new emails
        return None, None


def send_df_as_email(df, un, pw, logo_data, to_email, smtp_server, smtp_port):
    from_email = un
    password = pw

    today_date = datetime.now().strftime('%Y-%m-%d')
    # Convert DataFrame to Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name=today_date)
    output.seek(0)

    # Create the email
    msg = MIMEMultipart('related')
    msg['Subject'] = f'VRCG Master List {today_date}'
    msg['From'] = from_email
    msg['To'] = to_email

    # Create HTML body with logo
    html_body = f"""
    <html>
        <body>
            <p>Please see the attached Excel file.</p>
            <p><em>This is an automated email. Please do not respond to this message.</em></p>
            <br>
            <img src="cid:logo" width="200">
        </body>
    </html>
    """

    msg_alternative = MIMEMultipart('alternative')
    msg.attach(msg_alternative)
    
    # Attach HTML body
    msg_html = MIMEText(html_body, 'html')
    msg_alternative.attach(msg_html)
    
    # Attach logo as inline image
    img = MIMEImage(logo_data)
    img.add_header('Content-ID', '<logo>')
    img.add_header('Content-Disposition', 'inline')
    msg.attach(img)

    # Convert the DataFrame to an Excel file and attach it to the email
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(output.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment; filename="VRCG_Inventory.xlsx"')
    msg.attach(part)

    # Send the email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)


def load_df_to_bq(df, project_id):
    
    # Create a BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the table name using today's date
    table_id = 'inventory.vrcg_master'

    for column in df.select_dtypes(include=['object']).columns:
        df[column] = df[column].astype('string')

    # Load DataFrame into BigQuery table
    job = client.load_table_from_dataframe(df, table_id)

    # Wait for the load job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}.")


def vrcg_pipeline(event, context):
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(cfg.bucket)
    blob = bucket.blob(cfg.blob)
    logo_blob = bucket.blob('logo.jpg')
    logo_data = logo_blob.download_as_bytes()

    keys = json.loads(blob.download_as_string())
    un = keys['UN']
    pw = keys['PW']
    im = keys['IM']
    smtp = keys['SMTP']

    email_ids, mail = check_unread_emails(un, pw, im)

    # FIX TO LOG INSTEAD OF PRINT #
    if email_ids:
        # Pipeline
        # Change print to logging 
        print('pipeline running...')
        all_data = fetch_data(email_ids, mail)
        all_data = map_makes(all_data)
        all_data['State'] = all_data.apply(map_state_from_location, axis=1)
        all_data['Region'] = all_data['State'].apply(map_state_to_region)
        all_data['Inventory_Date'] = datetime.now().strftime("%Y_%m_%d")
        # Remove Duplicates
        all_data = all_data.sort_values(by='VIN')
        all_data = all_data.drop_duplicates(subset='VIN', keep='first')
        print('pipeline complete')

        # Email
        print('emailing...')
        send_df_as_email(
            df = all_data,
            un = un, pw = pw, logo_data=logo_data,
            to_email = 'cgoodman@vrcg.com',
            smtp_server = smtp,
            smtp_port=587
        )
        
        send_df_as_email(
            df = all_data,
            un = un, pw = pw, logo_data=logo_data,
            to_email = 'bhutto@vrcg.com',
            smtp_server = smtp,
            smtp_port=587
        )
        
        send_df_as_email(
            df=all_data,
            un=un, pw=pw, logo_data=logo_data,
            to_email='jamespatalan@gmail.com',
            smtp_server=smtp,
            smtp_port=587
        )
        print('sent')

        # Push to BQ
        print('pushing to bq')
        load_df_to_bq(
            all_data,
            project_id=cfg.project
        )
        print('done')

    else:
        print("No unread emails.")

if __name__ == '__main__':
    logging.info(f'beginning run of vrcg pipeline for {datetime.now()}')
    vrcg_pipeline("", "")
