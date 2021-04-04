
from sotorrent.pipeline import Pipeline
from sotorrent.upload import upload_xml

if __name__ == '__main__':
    #pipeline = Pipeline()
    #pipeline.run()
    upload_xml('so-dump/Posts.xml')
