import logging
import azure.functions as func
import mailerlite as MailerLite
import requests
import os
from jinja2 import Template, select_autoescape
from datetime import datetime, timezone
from dateutil import tz
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings 

class TitleChecker:
    def __init__(self, contents):
        self.titles = set()
        self.load_titles(contents)

    def load_titles(self, contents):
        lines = contents.split('\n')
        for line in lines:
            self.titles.add(line.strip())

    def is_title_present(self, title_to_check):
        for title in self.titles:
            if title in title_to_check:
                return True
        return False
    
class IDChecker:
    def __init__(self, contents):
        self.ids = set()
        self.load_ids(contents)

    def load_ids(self, contents):
        lines = contents.split('\n')
        for line in lines:
            self.ids.add(line.strip())

    def is_id_present(self, id_to_check):
        return id_to_check in self.ids

def upload_image_to_blob(url: str, blob_service_client: BlobServiceClient, container_name: str, blob_name: str):
    # Check if blob already exists
    blob = blob_service_client.get_container_client(container_name).get_blob_client(blob_name)
    exists = blob.exists()
    if exists:
        return

    # Download the image
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        logging.error(f"Failed to download image from {url}. Status code: {response.status_code}")
        return

    # Upload the image to blob storage
    blob.upload_blob(response.raw, overwrite=True)

    logging.info(f"Uploaded {url} to {blob_name} in container {container_name}")


def get_blob_client():

    # Acquire a credential object
    token_credential = DefaultAzureCredential()
    account_url = os.getenv('BLOB_ACCOUNT_URL')
    return BlobServiceClient(
            account_url=account_url,
            credential=token_credential)

def upload_blob_data(blob_service_client: BlobServiceClient, container_name: str, blob: str, data: bytes, content_type: str = None):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
    content_settings = ContentSettings(content_type='text/html')

    blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True, content_settings=content_settings)

def download_blob_text(blob_service_client: BlobServiceClient, container_name: str, blob: str):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob)
    return blob_client.download_blob().readall().decode("utf-8")

def query_mailerlite(id_checker: IDChecker, title_checker: TitleChecker, blob_service_client: BlobServiceClient):
    last_page = 1
    current_page = 1
    camplist = []
    while current_page <= last_page:
        logging.info(f'Page {current_page} of {last_page}')
        
        response = requests.get(api_url, params={'page':current_page, 'limit': 10, 'filter[status]': 'sent'}, headers=headers)
        content = response.json()
        logging.info(content['meta'])
        last_page = content['meta']['last_page']
        campaigns = content['data']
        count = 0
        
        for camp in campaigns:
            count += 1
            campitem = {}
            logging.info(f"Campaign {camp['id']} titled {camp['emails'][0]['subject']}")
            if id_checker.is_id_present(camp['id']):
                continue
            if title_checker.is_title_present(camp['emails'][0]['subject']):
                continue
            campitem['id'] = camp['id']
            pst_datetime = datetime.strptime(camp['finished_at'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc_zone).astimezone(pst_zone)
            campitem['finished_at'] = pst_datetime.strftime('%d %b %Y %I:%M %p')  
            campitem['status'] = camp['status']
            campitem['subject'] = camp['emails'][0]['subject']
            campitem['screenshot_url'] = camp['emails'][0]['screenshot_url']
            campitem['preview_url'] = camp['emails'][0]['preview_url']
            id = camp['id']
            upload_image_to_blob(campitem['screenshot_url'], blob_service_client, '$web', f'img/{id}.png')
            camplist.append(campitem)

        current_page += 1
    return camplist

def process_mailerlite():
    blob_service_client = get_blob_client()
    remove_ids = download_blob_text(blob_service_client, "data", "remove_ids.txt")
    remove_titles = download_blob_text(blob_service_client, "data", "remove_titles.txt")
    id_checker = IDChecker(remove_ids)
    title_checker = TitleChecker(remove_titles)
    camplist = query_mailerlite(id_checker, title_checker, blob_service_client)
    template_contents = download_blob_text(blob_service_client, "data", "newsletters-template.html")
    template = Template(template_contents)
    output = template.render(campaigns=camplist, date=datetime.now().strftime('%d %b %Y %I:%M %p'))
    upload_blob_data(blob_service_client, "$web", "index.html", output.encode('utf-8'))

utc_zone = tz.tzutc()
pst_zone = tz.tzoffset('PST', -28800)  # 8 hours behind UTC
# Replace with your API endpoint and Bearer token
api_url = "https://connect.mailerlite.com/api/campaigns"
bearer_token = os.getenv('MAILERLITE_BEARER_TOKEN')
headers = {
    'Authorization': f'Bearer {bearer_token}'
}

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    process_mailerlite()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
