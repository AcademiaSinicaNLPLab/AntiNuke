# coding: utf-8

import sys
sys.path.append('/home/plum/kimo_emo')

from Tokenizer import Tokenizer

import pymongo
co = pymongo.Connection('localhost')['AntiNuke']['FBcollection']

tkzr = Tokenizer()

total = co.count()
logging = open('parsing.log', 'w')

# get all Post_IDs
pids = [x['Post_ID'] for x in co.find()]

for i, pid in enumerate(pids):
    mdoc = co.find_one({'Post_ID': pid})
    if 'Message_CKIP' in mdoc and 'Caption_CKIP' in mdoc:
        print i+1, '/', total, '\t', mdoc['_id'], '(PROCESSED)'
        continue
    try:
        mckip, cckip = map(lambda x: tkzr.tokenizeStr( ''.join(x.split('\x0b')).encode('utf-8') ).decode('utf-8'), [mdoc['Message'], mdoc['Caption']] )
        print i+1, '/', total, '\t', mdoc['_id'], '(OK)'
    except: 
        logging.write( mdoc['Post_ID'] + '\n' )
        print i+1, '/', total, '\t', mdoc['_id'], '(ERROR)'
        continue
    co.update({'_id': mdoc['_id']}, { '$set': {'Message_CKIP': mckip, 'Caption_CKIP': cckip} })
    

logging.close()
