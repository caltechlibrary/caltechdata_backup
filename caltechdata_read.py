import argparse
import urllib.request
import urllib.parse
import urllib.error
import ssl
import json
import os
import subprocess
import shutil


def download_file(erecord,rid):
    url = erecord["uniform_resource_identifier"]
    req = urllib.request.Request(url)
    s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    fname = erecord['electronic_name'][0]
    print("Downloading: "+fname)
    try:
        response = urllib.request.urlopen(req,context=s)
        outfile = open(fname,'wb')
        outfile.write(response.read())
    except urllib.error.HTTPError:
        print("It looks like this file is embargoed.  We can't access until after the embargo is lifted")
    else:
        return fname

def read_records(data,current):
    #read records in 'hits' structure
    for record in data:
        rid = str(record['id'])
        metadata = record['metadata']
        files = []
        #Download all files
        if 'electronic_location_and_access' in metadata:
            #Look at all files
            for erecord in  metadata['electronic_location_and_access']:
                #Check existing record for file
                if rid in current:
                    existing_metadata =\
                    subprocess.check_output(["dataset","read",rid],universal_newlines=True)
                    #existing_metadata = existing_metadata.decode('utf-8')
                    #print(existing_metadata)
                    existing_metadata = json.loads(existing_metadata)
                    existing_metadata=existing_metadata["metadata"]
                    #Check if file was there previously
                    if 'electronic_location_and_access' in existing_metadata:
                        #If the file has changes
                        new_size = erecord['file_size']
                        existing_size = []
                        #print(existing_metadata)
                        #print(existing_metadata['metadata'])
                        #print(existing_metadata['electronic_location_and_access'])
                        for ex_rec in existing_metadata['electronic_location_and_access']:
                            existing_size.append(ex_rec['file_size'])
                        if new_size not in existing_size:
                            print(new_size, existing_size)
                            files.append(download_file(erecord,rid))
                        else:
                            #print("We have the file-or not")
                            try: existing_files=subprocess.check_output(["dataset","attachments",rid],universal_newlines=True)
                            except subprocess.CalledProcessError:
                                #Handle case where there is no files
                                #SHould check specific error
                                print("No existing attachments")
                                files.append(download_file(erecord,rid))
                            else:
                                if erecord['electronic_name'][0] not in existing_files:
                                    print(erecord)
                                    print(erecord['electronic_name'][0],existing_files)
                                    files.append(download_file(erecord,rid))
                    else:
                        #Fille wasn't listed before
                        print("Not listed")
                        files.append(download_file(erecord,rid))

        #Save results in dataset
        outstr = json.dumps(record)

        #Replace single quotes with complicated escape
        outstr = outstr.replace("'","'\\''")
        print("Saving record " + str(rid))

        os.system("dataset create "+str(record['id'])+'.json'+" '"+outstr+"'")

        print("Attaching ",files)
        for f in files:
            if f != None:
                os.system("dataset attach "+str(rid)+" "+f)
                os.remove(f)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=\
    "caltechdata_read queries the caltechDATA (Invenio 3) API\
    returns data and adds to dataset structure on disk")

    os.environ["DATASET"] = "caltechdata"

    #parser.add_argument('-t', dest='topic_id',help="topic of questions to\
    #        return (default all)")

    #TODO: - Support paging through records from api
    #http://caltechdata.tind.io/api/records/?q=&sort=-mostrecent&size=10&page=1
    # - Check dates to limit file downloads
    # - Check file size for files

    args = parser.parse_args()

    api_url = "https://caltechdata.tind.io/api/records/"
 
    #Get the existing records
    current = subprocess.check_output(["dataset","keys"],universal_newlines=True).splitlines()

    req = urllib.request.Request(api_url)
    s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    response = urllib.request.urlopen(req,context=s)
    data = json.JSONDecoder().decode(response.read().decode('UTF-8'))

    read_records(data['hits']['hits'],current)
    #if we have more pages of data
    while 'next' in data['links']:
        req = urllib.request.Request(data['links']['next'])        
        response = urllib.request.urlopen(req,context=s)
        data = json.JSONDecoder().decode(response.read().decode('UTF-8'))
        
        read_records(data['hits']['hits'],current)
