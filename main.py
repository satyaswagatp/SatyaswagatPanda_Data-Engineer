import requests
import zipfile
import io
import xml.etree.ElementTree as ET

xml_url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2022-01-01T00:00:00Z+TO+2022-01-01T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
r = requests.get(xml_url)
root = ET.fromstring(r.content)
zip_url = None

for result in root.findall('./result'):
    file_type = result.find('./str[@name="file_type"]').text
    if file_type == 'DLTINS':
        zip_url = result.find('./str[@name="download_link"]').text
        break

if zip_url:
    r = requests.get(zip_url)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall()

import pandas as pd
import xml.etree.ElementTree as ET

# assuming the XML file has been extracted to a file named "data.xml"
xml_file = 'data.xml'

# create an empty DataFrame to store the data
data = pd.DataFrame(columns=['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 
                             'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                             'FinInstrmGnlAttrbts.NtnlCcy'])

# parse the XML file
tree = ET.parse(xml_file)
root = tree.getroot()

# iterate over the XML elements and extract the required data
for instrument in root.iter('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts'):
    id = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id').text
    full_name = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm').text
    classification_type = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp').text
    commodity_deriv_ind = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd').text
    national_currency = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy').text
    data = data.append({'FinInstrmGnlAttrbts.Id': id,
                        'FinInstrmGnlAttrbts.FullNm': full_name,
                        'FinInstrmGnlAttrbts.ClssfctnTp': classification_type,
                        'FinInstrmGnlAttrbts.CmmdtyDerivInd': commodity_deriv_ind,
                        'FinInstrmGnlAttrbts.NtnlCcy': national_currency}, ignore_index=True)

# write the data to a CSV file
data.to_csv('output.csv', index=False)

import pandas as pd
import xml.etree.ElementTree as ET
import boto3

def lambda_handler(event, context):
    # assuming the XML file has been extracted to a file named "data.xml"
    xml_file = 'data.xml'
    
    # create an empty DataFrame to store the data
    data = pd.DataFrame(columns=['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 
                                 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                                 'FinInstrmGnlAttrbts.NtnlCcy'])

    # parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # iterate over the XML elements and extract the required data
    for instrument in root.iter('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts'):
        id = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id').text
        full_name = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm').text
        classification_type = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp').text
        commodity_deriv_ind = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd').text
        national_currency = instrument.find('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy').text
        data = data.append({'FinInstrmGnlAttrbts.Id': id,
                            'FinInstrmGnlAttrbts.FullNm': full_name,
                            'FinInstrmGnlAttrbts.ClssfctnTp': classification_type,
                            'FinInstrmGnlAttrbts.CmmdtyDerivInd': commodity_deriv_ind,
                            'FinInstrmGnlAttrbts.NtnlCcy': national_currency}, ignore_index=True)

    # write the data to a CSV file
    csv_file = 'output.csv'
    data.to_csv(csv_file, index=False)
    
    # upload the CSV file to S3
    bucket_name = 'your-bucket-name'
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(csv_file, bucket_name, csv_file)
