import argparse
import urllib.request
import urllib.parse
import urllib.error
import ssl
import json
import os
import shutil

def read_records(data):
    #read records in 'hits' structure
    for record in data:
        outstr = json.dumps(record)

        #Replace single quotes with complicated escape
        outstr = outstr.replace("'","'\\''")
        print(str(record['id']))
        os.system("echo '" + outstr +"' | dataset create "+str(record['id'])+'.json')
        
        #Download all file
        metadata = record['metadata']
        if 'electronic_location_and_access' in metadata:
            for erecord in  metadata['electronic_location_and_access']:
                url = erecord["uniform_resource_identifier"]
                #req = urllib.request.Request(url)
                #s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                #response = urllib.request.urlopen(req,context=s)

                #outfile = open(erecord['electronic_name'][0],'wb')
                #outfile.write(response.read())
                print(erecord['electronic_name'][0])    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=\
    "caltechdata_read queries the caltechDATA (Invenio 3) API\
    returns data and adds to dataset structure on disk")

    os.environ["DATASET"] = "caltechdata"

    #parser.add_argument('-t', dest='topic_id',help="topic of questions to\
    #        return (default all)")

    #TODO: - Support paging through records from api
    #http://caltechdata.tind.io/api/records/?q=&sort=-mostrecent&size=10&page=1
    # - Add files to dataset
    # - Check dates to limit file downloads
    # - Check file size for files

    args = parser.parse_args()

    api_url = "https://caltechdata.tind.io/api/records/"
    
    #if args.topic_id:
    #    api_url = api_url + '&topic_id=' + args.topic_id

    req = urllib.request.Request(api_url)
    s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    response = urllib.request.urlopen(req,context=s)
    data = json.JSONDecoder().decode(response.read().decode('UTF-8'))

    read_records(data['hits']['hits'])
    #if we have more pages of data
    while 'next' in data['links']:
        req = urllib.request.Request(data['links']['next'])        
        response = urllib.request.urlopen(req,context=s)
        data = json.JSONDecoder().decode(response.read().decode('UTF-8'))
        
        read_records(data['hits']['hits'])
