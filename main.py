import json

from sotorrent.xml_to_json import parse_xml
from sotorrent.upload import upload_xml

if __name__ == '__main__':
    #setup_logger('pipeline')

    for post in parse_xml('so-dump/Posts.xml'):
        print(json.dumps(post))

    #print(posts)

    #upload_xml('so-dump/Posts.xml')

