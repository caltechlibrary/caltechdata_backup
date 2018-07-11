import argparse
import urllib.request
import ssl
import json
import os
import subprocess
import requests
import dataset
from clint.textui import progress

def download_file(erecord,rid):
    r = requests.get(erecord["uniform_resource_identifier"],stream=True)
    fname = erecord['electronic_name'][0]
    if r.status_code == 403:
        print("It looks like this file is embargoed.  We can't access until after the embargo is lifted")
        return None
    else:
        with open(fname, 'wb') as f:
            total_length = int(r.headers.get('content-length'))
            for chunk in \
progress.bar(r.iter_content(chunk_size=1024),expected_size=(total_length/1024) + 1):
                if chunk:
                    f.write(chunk)
                    #f.flush()
        return fname

def read_records(data,current,collection):
    #read records in 'hits' structure
    for record in data:
        rid = str(record['id'])
        metadata = record['metadata']
        download = False #Flag for downloading files
        #Do we need to download?
        if 'electronic_location_and_access' in metadata:
            #Look at all files
            for erecord in  metadata['electronic_location_and_access']:
                #Check existing record for file
                if rid in current:
                    existing_metadata,err = dataset.read(collection,rid)
                    if err != "":
                        print(f"Unexpected error for {key} in {collection_name}, {err}")
                        exit()
                    existing_metadata=existing_metadata["metadata"]
                    #Check if file was there previously
                    if 'electronic_location_and_access' in existing_metadata:
                        new_size = erecord['file_size']
                        existing_size = []
                        for ex_rec in existing_metadata['electronic_location_and_access']:
                            existing_size.append(ex_rec['file_size'])
                        if new_size not in existing_size:
                            #New file
                            download = True
                        
                        #This code block is a double check that we have the
                        #files.  It works but is slow because of the metadata
                        #limitations in dataset.  This code will be used
                        #pending changes to dataset
                        #else:
                        #    #File sizes match - no change indicated from metadata
                        #    try: existing_files=subprocess.check_output(["dataset","attachments",rid],universal_newlines=True)
                        #    except subprocess.CalledProcessError:
                        #        #Missing file-embargo or previous error
                        #        print("No existing attachments")
                        #        download = True
                        #    else:
                        #        #Confirm that we actually have the attachment
                        #        if erecord['electronic_name'][0] not in existing_files:
                        #            print(erecord)
                        #            print(erecord['electronic_name'][0],existing_files)
                        #            download = True
                    else:
                        #New file
                        download = True
                else:
                    #New record
                    download = True

        #Save results in dataset
        print("Saving record " + str(rid))

        if rid in current:
            err = dataset.update(collection,str(record['id']),record)
            if err != '':
                print(f"Failed, could not create record: {err}")
                exit()
        else:
            err = dataset.create(collection,str(record['id']),record)
            if err != '':
                print(f"Failed, could not create record: {err}")
                exit()

        if download == True:
            files = []

            print("Downloading files for ",rid)

            for erecord in metadata['electronic_location_and_access']:
                f = download_file(erecord,rid)
                if f != None:
                    files.append(f)

            print(files)
            print("Attaching files")

            if len(files) != 0:
                err = dataset.attach(collection,rid,files)
                if err != '':
                    print(f"Failed on attach {err}")
                    exit()   
        
            for f in files:
                if f != None:
                    os.remove(f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=\
    "caltechdata_read queries the caltechDATA (Invenio 3) API\
    returns data and adds to dataset structure on disk")

    collection = "caltechdata.ds"
    if os.path.isdir(collection)==False:
        err = dataset.init(collection)
        if err != '':
            print(f"Failed on creatr {err}")
            exit()

    args = parser.parse_args()

    api_url = "https://caltechdata.tind.io/api/records/"
 
    #Get the existing records
    current = dataset.keys(collection)
    req = urllib.request.Request(api_url)
    s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    response = urllib.request.urlopen(req,context=s)
    data = json.JSONDecoder().decode(response.read().decode('UTF-8'))

    read_records(data['hits']['hits'],current,collection)
    #if we have more pages of data
    while 'next' in data['links']:
        req = urllib.request.Request(data['links']['next'])        
        response = urllib.request.urlopen(req,context=s)
        data = json.JSONDecoder().decode(response.read().decode('UTF-8'))
        
        read_records(data['hits']['hits'],current,collection)
