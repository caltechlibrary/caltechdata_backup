import argparse, os, json
import s3fs, boto3, glob
from progressbar import ProgressBar
from datacite import schema43
from caltechdata_api import caltechdata_write
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


parser = argparse.ArgumentParser(
    description="Adds S3-stored pilot files to backup on AWS Glacier Deep Archive"
)
parser.add_argument("folder", nargs=1, help="Folder")
parser.add_argument(
    "file_location",
    nargs=1,
    help="Location where the files are, either 'local' or 'OSN'",
)

args = parser.parse_args()
folder = args.folder[0]

bucket = "caltechdata-backup"
path = "ini210004tommorrell/"
s3 = s3fs.S3FileSystem()
current = s3.ls(f"{bucket}/{path}{folder}")
existing = []
for ex in current:
    existing.append(ex.split(path)[1])

if args.file_location == "local":
    backup_source = glob.glob(f"{folder}/*")
elif args.file_location == "OSN":
    endpoint = "https://renc.osn.xsede.org/"
    osn_s3 = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint})
    # Find the files to backup
    path = "ini210004tommorrell/" + args.folder[0] + "/"
    backup_source = osn_s3.glob(path + "/*")

s3_boto = boto3.client("s3")

for file in backup_source:
    if file not in existing:
        size = os.path.getsize(file)
        bar = ProgressBar(max_value=size)
        with open(file, "rb") as f:
            print(file)
            s3_boto.upload_fileobj(f, bucket, f"{path}{file}", Callback=Progress(bar))
            assert size == s3.du(f"{bucket}/{path}{file}")
