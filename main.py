#import epub
#from lxml import etree
from google.cloud import storage
import os
import zipfile
from zipfile import ZipFile
from zipfile import is_zipfile
import io
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import dask.dataframe as dd

def epub2text(data, context, verbose=True):
    def vprint(s):
        if verbose:
            print(s)

    input_bucket_name = data['bucket']
    source_file = data['name']
    isbn = source_file.split("/")[2].replace(".epub", "")
    uri = 'gs://{}/{}'.format(input_bucket_name, source_file)
    
    if str(source_file).lower().endswith('.epub'):
        vprint('Event ID: {}'.format(context.event_id))
        vprint('Event type: {}'.format(context.event_type))
        vprint('Importing required modules.')
        vprint('This is the data: {}'.format(data))
        vprint('Getting the data from bucket "{}"'.format(uri))

        client = storage.Client()
        bucket = client.get_bucket(input_bucket_name)
        blob = bucket.blob(source_file)
        file = io.BytesIO(blob.download_as_string())

        output = "test/unzipped_epubs/{}".format(isbn) 
        
        with ZipFile(file, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(output + "/" + contentfilename)
                blob.upload_from_string(contentfile)
                vprint('Uploading file "{}"'.format(contentfile))

        # Read .opf file
        blobs = bucket.list_blobs(prefix=output)
        files = []
        for blob in blobs:
            files.append(blob.name)
            if ".opf" in blob.name:
                opf_path = blob.name 

        vprint('This is the opf file path: "{}"'.format(opf_path))

        blob = bucket.get_blob(opf_path)
        file = blob.download_as_string()
        soup = BeautifulSoup(file, 'html.parser')
        # Get spine
        spine = soup.find(name = "spine")
        itemref = spine.find_all("itemref")
        itemref_list = []
        for i in itemref:
            idref = i['idref']
            itemref_list.append(idref)
        # Get manifest
        manifest = soup.find(name = "manifest")
        items = manifest.find_all("item")
        href_list = []
        id_list = []
        for i in items:
            href = i["href"]
            id = i["id"]
            href_list.append(href)
            id_list.append(id)

        # Get href in the right order
        href_ordered = []
        for i in itemref_list:
            href_i = href_list[id_list.index(i)]
            href_ordered.append(href_i)

        # Loop through files in href_ordered and extract text
        text_df = pd.DataFrame(data = [])
        for h in href_ordered:
            file_path = "test/unzipped_epubs/" + isbn + "/" + h
            blob = bucket.get_blob(file_path)
            file = blob.download_as_string()
            soup = BeautifulSoup(file, 'html.parser')
            title = soup.title.text
            if len(title) == 0:
                title = "NA"
            h1_list = soup.find_all("h1")
            if len(h1_list) > 0:
                h1 = h1_list[0].text # For now, just get first header and ignore h2, h3, bullet lists etc. 
            else:
                h1 = "NA" 
            href_list = []
            title_list = []
            header_list = []
            paragraphs = soup.find_all("p")
            text_list = []
            for p in paragraphs:
                text_p = p.text
                if len(text_p) == 0:
                    text_p = "NA"
                text_list.append(text_p)
                title_list.append(title)
                href_list.append(h)
                header_list.append(h1)
            paragraph_ind = list(range(1,len(paragraphs)+1))
            text_df_h = pd.DataFrame({'HREF': href_list, 'TITLE': title_list, 'PARAGRAPH': paragraph_ind, 'TEXT': text_list})
            text_df = pd.concat([text_df, text_df_h])

        vprint('This is the title: "{}"'.format(title))

        # Export as CSV
        ddf = dd.from_pandas(text_df, npartitions=1, sort=False)
        output = 'gs://storytel-intelligence-experiment/test/extracted_text/' + isbn + "-*.csv"
        dd.to_csv(df = ddf, filename = output, sep = ",")