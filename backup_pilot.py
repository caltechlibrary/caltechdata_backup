import argparse, os, json, time
import s3fs, boto3, glob
from progressbar import ProgressBar
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


def upload_file(f, file, size, bucket, path, s3_boto):
    print(size)
    print(path)
    print(file)
    bar = ProgressBar(max_value=size)
    s3_boto.upload_fileobj(f, bucket, f"{file}", Callback=Progress(bar))
    time.sleep(10)
    uploaded_size = s3.du(f"{bucket}/{file}")
    print(uploaded_size)
    assert size == uploaded_size


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
location = args.file_location[0]

bucket = "caltechdata-backup"
path = "ini210004tommorrell/"
s3 = s3fs.S3FileSystem()
current = s3.ls(f"{bucket}/{path}{folder}")
existing = []
for ex in current:
    existing.append(ex.split(f"{bucket}/")[1])
    #We support looking at one level of folders
    second_level = s3.ls(ex)
    for sec in second_level:
        existing.append(sec.split(f"{bucket}/")[1])

# Now use boto version of backup location to ensure copying works
s3_boto = boto3.client("s3")

if location == "local":
    backup_source = glob.glob(f"{folder}/*")
    for file in backup_source:
        if os.path.isfile(file):
            if file not in existing:
                size = os.path.getsize(file)
                print(file)
                with open(file, "rb") as f:
                    upload_file(f, f"{path}{file}", size, bucket, path, s3_boto)
        else:
            for fil in glob.glob(f"{file}/*"):
                if fil not in existing:
                    print(fil)
                    size = os.path.getsize(fil)
                    with open(fil, "rb") as f:
                        upload_file(f, f"{path}{fil}", size, bucket, path, s3_boto)

elif location == "OSN":
    endpoint = "https://renc.osn.xsede.org/"
    osn_s3 = s3fs.S3FileSystem(anon=True, client_kwargs={"endpoint_url": endpoint})
    # Find the files to backup
    backup_source = osn_s3.glob(f"{path}{folder}/*")
    for file in backup_source:
        size = osn_s3.info(file)["Size"]
        if size > 0:
            #we have a fille
            if file not in existing:
                print(file)
                with osn_s3.open(file, "rb") as f:
                    upload_file(f, file, size, bucket, path, s3_boto)
        else:
            # We have a directory, get all the files under it
            for fil in osn_s3.glob(f"{file}/*"):
                if fil not in existing:    
                    print(fil)
                    size = osn_s3.info(fil)["Size"]
                    with osn_s3.open(fil, "rb") as f:
                        upload_file(f, fil, size, bucket, path, s3_boto)
else:
    print(f"{args.file_location} is not a supported file location")
