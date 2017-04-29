import requests
import shutil
import csv
import json
import logging

def download(url, id):
  r = requests.get(url, stream=True)
  if r.status_code == 200:
    with open(id, 'wb') as f:
      r.raw.decode_content = True
      shutil.copyfileobj(r.raw, f)
    return True

def upload(filename, uploadurl):
    payload = open(filename, "rb").read()
    res = requests.put(url=uploadurl,data=payload,headers={'Content-Type': 'application/octect-stream'})
    if res.status_code == 200:
        logging.info('Uploaded' + filename)

def write_index(count):
    index_file = 'discogs_{count}.dat'.format(count=count)
    payload = {'type' : 'WRITE', 'index_path' : index_file}
    r = requests.post("http://127.0.0.1:4212/index/io", data=json.dumps(payload))
    if r.status_code == 200:
        logging.info('Writing index file ' + index_file + ' ' + str(r.status_code))

# initialize logging
logging.basicConfig(filename='import-pastec.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Starting import')

# open and read the file
try:
    f = open('most-popular-release-images.csv')
    csv_f = csv.reader(f)
    count = 0
except IOError:
    logging.info("can't open file")
    exit()

# do the things
for row in csv_f:
  releaseid = row[0]
  imageurl = row[2]
  imagename = imageurl.split("/")[-1]
  if download(imageurl, imagename):
    count += 1
    logging.info('Writing ' + releaseid + ' ' + imagename + ' ' + str(count))
    upload(imagename, 'http://localhost:4212/index/images/' + releaseid)
  if count % 20000 == 0:
      write_index(count)

