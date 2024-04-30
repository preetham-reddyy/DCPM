import requests
import re
import sys
from bs4 import BeautifulSoup

resp = requests.get("http://172.16.0.111:{}/stages/".format(sys.argv[2]))
soup = BeautifulSoup(resp.text, 'html.parser')

table = soup.select_one(".aggregated-allCompletedStages table tbody")

def get_bytes(text):
    if text.strip() == '':
        return 0
    value = float(re.findall("\d+\.\d+", text)[0])
    if 'KB' in text:
        return value*1024
    if 'MB' in text:
        return value*1024*1024
    if 'GB' in text:
        return value*1024*1024*1024
    if ' B' in text:
        return value
    return 0

input_size = 0
output_size = 0
shuffle_read = 0
shuffle_write = 0

for row in table.select("tr"):
    cols = row.select("td")
    input_size += get_bytes(cols[5].getText())
    output_size += get_bytes(cols[6].getText())
    shuffle_read += get_bytes(cols[7].getText())
    shuffle_write += get_bytes(cols[8].getText())

# print(input_size,output_size,shuffle_read, shuffle_write)
metrics = "input size: {}\noutput size: {}\nshuffle read: {}\nshuffle write:{}".format(input_size,output_size,shuffle_read,shuffle_write)
print(metrics)
with open(sys.argv[1],'w') as f:
    f.write(metrics)