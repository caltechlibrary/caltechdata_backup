import math, os, json, time
import s3fs, boto3, glob
from tqdm import tqdm
import requests
from caltechdata_api import decustomize_schema


def upload_json(json_struct, bucket, location, s3_boto):
    s3_boto.put_object(Bucket=bucket, Body=json.dumps(json_struct), Key=location)


bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()

# Now use boto version of backup location to ensure copying works
s3_boto = boto3.client("s3")

url = "https://data.caltech.edu/api/records"

response = requests.get(f"{url}?q=NOT(subjects:%27HTE%27)")
# We don't include the HTE records due to Elasticsearch limitations
total = response.json()["hits"]["total"]
pages = math.ceil(int(total) / 1000)
hits = []
for c in tqdm(range(1, pages + 1)):
    # We don't include the HTE records due to Elasticsearch limitations
    chunkurl = f"{url}?q=NOT(subjects:%27HTE%27)&sort=-mostrecent&size=1000&page={c}"
    response = requests.get(chunkurl).json()
    hits += response["hits"]["hits"]

for h in tqdm(hits):
    rid = str(h["id"])
    print(rid)

    metadata = decustomize_schema(h["metadata"], True, True, True, "43")
    # Write both the raw API data and DataCite metadata as json files
    location = f"{path}/{rid}/datacite.json"
    upload_json(metadata, bucket, location, s3_boto)
    location = f"{path}/{rid}/raw.json"
    upload_json(h, bucket, location, s3_boto)
