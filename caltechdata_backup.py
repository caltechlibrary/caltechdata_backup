import argparse
import json
import os
import requests
from py_dataset import dataset
from progressbar import progressbar


def download_file(erecord, rid):
    r = requests.get(erecord["uniform_resource_identifier"], stream=True)
    fname = erecord["electronic_name"][0]
    size = erecord["file_size"]
    if os.path.isfile(fname):
        if os.stat(fname) == size:
            print("Using already downloaded file")
            return fname
    elif r.status_code == 403:
        print(
            "It looks like this file is embargoed.  We can't access until after the embargo is lifted"
        )
        return None
    else:
        with open(fname, "wb") as f:
            total_length = int(r.headers.get("content-length"))
            for chunk in progressbar(
                r.iter_content(chunk_size=1024), max_value=(total_length / 1024) + 1
            ):
                if chunk:
                    f.write(chunk)
                    # f.flush()
        return fname


def read_records(data, current, collection):
    # read records in 'hits' structure
    for record in data:
        rid = str(record["id"])
        metadata = record["metadata"]
        download = False  # Flag for downloading files
        # Do we need to download?
        if "electronic_location_and_access" in metadata:
            # Get information about already backed up files:
            existing_size = []
            existing_names = []
            if rid in current:
                # Get existing files
                attachments = dataset.attachments(collection, rid)
                for a in attachments:
                    split = a.split(" ")
                    name = split[0]
                    size = split[1]
                    existing_names.append(name)
                    existing_size.append(size)
            # Look at all files
            count = len(metadata["electronic_location_and_access"])
            dl = 0
            for erecord in metadata["electronic_location_and_access"]:
                # Check if file has been downloaded
                size = erecord["file_size"]
                name = erecord["electronic_name"][0]
                if size in existing_size and name in existing_names:
                    dl = dl + 1
            if dl == count:
                print(
                    "files already downloaded ", existing_size, existing_names,
                )
                download = False
            else:
                print("file mismatch ", existing_size, existing_names, dl, count)
                download = True

        # Save results in dataset
        print("Saving record " + str(rid))

        if rid in current:
            update = dataset.update(collection, str(record["id"]), record)
            if update == False:
                print(f"Failed, could not create record: {dataset.error_message()}")
                exit()
        else:
            update = dataset.create(collection, str(record["id"]), record)
            if update == False:
                print(f"Failed, could not create record: {dataset.error_message()}")
                exit()

        if download == True:
            files = []

            print("Downloading files for ", rid)

            for erecord in metadata["electronic_location_and_access"]:
                f = download_file(erecord, rid)
                if f != None:
                    files.append(f)

            print(files)
            print("Attaching files")

            if len(files) != 0:
                err = dataset.attach(collection, rid, files)
                if err == False:
                    print(f"Failed on attach {dataset.error_message()}")
                    exit()

            for f in files:
                if f != None:
                    os.remove(f)

            ### Need to handle old files


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="caltechdata_backup queries the caltechDATA (Invenio 3) API\
    returns data and adds to dataset structure on disk"
    )

    collection = "caltechdata.ds"
    if os.path.isdir(collection) == False:
        err = dataset.init(collection)
        if err != "":
            print(f"Failed on create {err}")
            exit()

    args = parser.parse_args()

    api_url = "https://data.caltech.edu/api/records/"

    # Get the existing records
    current = dataset.keys(collection)
    req = requests.get(api_url)
    data = req.json()

    read_records(data["hits"]["hits"], current, collection)
    # if we have more pages of data
    while "next" in data["links"]:
        req = requests.get(data["links"]["next"])
        data = req.json()

        read_records(data["hits"]["hits"], current, collection)
