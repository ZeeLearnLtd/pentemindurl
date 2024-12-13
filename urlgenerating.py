import os
import boto3
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

print("File uploading in process. Please wait...")

# AWS credentials
aws_access_key_id = ''
aws_secret_access_key = ''

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# AWS S3 bucket details
bucket_name = 'content.pentemind.com'

# Get the region of the S3 bucket
bucket_region = s3.get_bucket_location(Bucket=bucket_name).get('LocationConstraint', 'us-east-1')

def upload_to_s3(file_or_folder_paths, bucket_name, custom_folder_name, success_path):
    uploaded_count = 0
    failed_count = 0

    for path in file_or_folder_paths:
        if os.path.isfile(path):  # Single file
            uploaded_count = upload_file(path, s3, bucket_name, custom_folder_name, success_path, uploaded_count)
        elif os.path.isdir(path):  # Folder
            for root, _, files in os.walk(path):
                for file in files:
                    file_path = os.path.join(root, file)
                    uploaded_count = upload_file(file_path, s3, bucket_name, custom_folder_name, success_path, uploaded_count)
        else:
            print(f"Invalid path: {path}")
            failed_count += 1

    return uploaded_count, failed_count

def upload_file(file_path, s3, bucket_name, custom_folder_name, success_path, uploaded_count):
    try:
        # Generate the S3 key
        s3_key = f"{custom_folder_name}/{os.path.basename(file_path)}"

        # Determine the MIME type and Content-Disposition
        content_type = 'application/octet-stream'  # Default MIME type
        content_disposition = 'inline'
        if file_path.endswith('.pdf'):
            content_type = 'application/pdf'
        elif file_path.endswith('.jpg') or file_path.endswith('.jpeg'):
            content_type = 'image/jpeg'
        elif file_path.endswith('.png'):
            content_type = 'image/png'
        elif file_path.endswith('.xlsx'):
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            content_disposition = 'attachment'

        # Upload the file to S3
        s3.upload_file(file_path, bucket_name, s3_key, ExtraArgs={
            'ContentType': content_type,
            'ContentDisposition': content_disposition
        })

        # Ensure unique filename in success folder
        success_file_path = os.path.join(success_path, os.path.basename(file_path))
        if os.path.exists(success_file_path):
            base, ext = os.path.splitext(success_file_path)
            counter = 1
            while os.path.exists(success_file_path):
                success_file_path = f"{base}_{counter}{ext}"
                counter += 1

        # Move to success folder
        os.rename(file_path, success_file_path)

        print(f"File {file_path} uploaded to S3 successfully.")
        update_status(file_path, success_file_path, s3_key)
        uploaded_count += 1
    except Exception as e:
        print(f"Error uploading file {file_path}: {e}")
    return uploaded_count


def update_status(file_path, new_path, s3_key):
    # Update the dataframe with file information
    new_row = {
        "File Path": new_path,
        "Remote URL": f"https://s3.{bucket_region}.amazonaws.com/{bucket_name}/{s3_key}"
    }
    upload_status_df.loc[len(upload_status_df)] = new_row

def send_email(attachment_path, statistics):
    # Email configuration
    sender_email = "karthik.chauhan@kidzee.com"
    sender_password = "Karthik#7890"
    receiver_emails = ["dolly.jagani@zeelearn.com"]
    cc_emails = ["vrushabh.marathe@zeelearn.com"]

    subject = "File Upload Summary"
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_emails)
    msg['Cc'] = ", ".join(cc_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(f"Hi,\n\nWe have sent the attachment. Please check the statistics below:\n\n{statistics}", 'plain'))

    # Attach Excel file
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(attachment_path)}")
        msg.attach(part)

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_emails + cc_emails, msg.as_string())

# Paths
file_or_folder_paths = [r"D:\Kidzee Content\Data"]
success_path = r"D:\Kidzee Content\Success"

# Create success folder if not exists
os.makedirs(success_path, exist_ok=True)

custom_folder_name = datetime.now().strftime("%d-%m-%y")
excel_file_path = os.path.join(r"D:\Kidzee Content\Server Excel Files", f"server_{datetime.now().strftime('%d-%m-%y_%H-%M-%S')}.xlsx")
upload_status_df = pd.DataFrame(columns=["File Path", "Remote URL"])

# Upload files
uploaded_count, failed_count = upload_to_s3(file_or_folder_paths, bucket_name, custom_folder_name, success_path)

# Save upload status to Excel
upload_status_df.to_excel(excel_file_path, index=False)

# Summary
statistics_message = f"Statistics - Uploaded: {uploaded_count}, Failed: {failed_count}"
print("Process completed.")
print(statistics_message)

# Send email
send_email(excel_file_path, statistics_message)
print("Email sent successfully!")
