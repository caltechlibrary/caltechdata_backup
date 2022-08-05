import math, os, json, time
import s3fs, boto3, glob
from tqdm import tqdm
import urllib.request
import requests
from caltechdata_api import decustomize_schema
from progressbar import ProgressBar, FileTransferSpeed, Bar, Percentage, ETA, Timer
import threading


class Progress(object):
    def __init__(self, bar):
        self._bar = bar
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            self._bar.update(self._seen_so_far)


def upload_file(f, file, size, bucket, s3_boto):
    print(size)
    print(file)
    bar = ProgressBar(
        max_value=size,
        widgets=[FileTransferSpeed(), Bar(), Percentage(), Timer(), ETA()],
    )
    s3_boto.upload_fileobj(f, bucket, f"{file}", Callback=Progress(bar))


bucket = "caltechdata-backup"
path = "caltechdata"
s3 = s3fs.S3FileSystem()
current = s3.ls(f"{bucket}/{path}")
existing = []
for ex in current:
    existing.append(ex.split(f"{bucket}/")[1])
    second_level = s3.ls(ex)
    for sec in second_level:
        existing.append(sec.split(f"{bucket}/")[1])

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

for h in hits:
    metadata = h["metadata"]
    rid = str(h["id"])
    if "electronic_location_and_access" in metadata:
        for erecord in metadata["electronic_location_and_access"]:
            size = float(erecord["file_size"])
            name = erecord["electronic_name"][0]
            filen = f"{path}/{rid}/{name}"
            if filen not in existing:
                if erecord["embargo_status"] != "closed":
                    print(filen)
                    with urllib.request.urlopen(
                        erecord["uniform_resource_identifier"]
                    ) as f:
                        upload_file(f, filen, size, bucket, s3_boto)
                else:
                    print(erecord)
