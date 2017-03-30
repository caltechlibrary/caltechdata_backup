import argparse
import urllib.request
import urllib.parse
import urllib.error
import ssl
import json
import os
import shutil

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description=\
    "caltechdata_read queries the caltechDATA (Invenio 3) API\
    returns data and adds to dataset structure on disk")
    #parser.add_argument('-t', dest='topic_id',help="topic of questions to\
    #        return (default all)")

    #TODO: - Support paging through records from api
    #http://caltechdata.tind.io/api/records/?q=&sort=-mostrecent&size=10&page=1
    # - Add files to dataset
    # - Check dates to limit file downloads
    # - Check file size for files

    args = parser.parse_args()

    api_url = "https://caltechdata.tind.io/api/records/"
    
    req = urllib.request.Request(api_url)
    s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    response = urllib.request.urlopen(req,context=s)
    data = json.JSONDecoder().decode(response.read().decode('UTF-8'))

    #Get number of documents
    count = 0
    for dtype in data['aggregations']['cal_resource_type']['buckets']:
        count = count + dtype['doc_count']

    new_url = api_url + '?query=&size='+str(count)
    req = urllib.request.Request(new_url)
    response = urllib.request.urlopen(req,context=s)
    data = json.JSONDecoder().decode(response.read().decode('UTF-8'))

    for f in data['hits']['hits']:
        #o = open(str(f['id'])+'.json','w')
        #if f['files']:
        #    print(f['files'])
        outstr = json.dumps(f)
        #print(outstr)
        
        #Replace single quotes with complicated escape
        outstr = outstr.replace("'","'\\''")
        print(str(f['id']))
        os.system("echo '" + outstr +"' | dataset create "+str(f['id'])+'1.json')  

        #Download all file
        record = f['metadata']
        if 'electronic_location_and_access' in record:
            for erecord in  record['electronic_location_and_access']:
                #url = erecord["uniform_resource_identifier"]
                #req = urllib.request.Request(url)
                #s = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                #response = urllib.request.urlopen(req,context=s)
            
                #outfile = open(erecord['electronic_name'][0],'wb')
                #outfile.write(response.read())
                print(erecord['electronic_name'][0])
