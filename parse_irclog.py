#!/usr/bin/env python

import datetime
import gzip
import re
import sys

if len(sys.argv) < 2:
    print 'Usage: <full path>'
    sys.exit(1)

PARSERS = {
    'privmsg': re.compile(r'^(?P<time>\d{4}:\d{2}) <(?P<modes>[@+~^ ])(?P<nickname>\S+)> (?P<message>.*)$'),
}

from elasticsearch import Elasticsearch

es = Elasticsearch()

path = sys.argv[1]

print 'Parsing ::  %s' % path

if path[0:2] == './':
    path = path[2:]

split_path = path.split('/')

if len(split_path) <> 5:
    print 'this is wrong - must pass in the full path'
    sys.exit(1)

network = split_path[0]
channel = split_path[1]
filename = split_path[-1]

date = filename[-15:-7]

if filename[-2:] == 'gz':
    irclog = gzip.open(path)
else:
    irclog = file(filename, 'r')

for line in irclog.readlines():
    line = line.strip()

    m = None
    for event in PARSERS:
        m = PARSERS[event].match(line)

        if m:
            break

    if not m:
        continue

    data = m.groupdict()

    obj = {
        'event': event,
        'channel': channel,
        'network': network,
        'path': path,
        'datetime': datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:8]), int(data['time'][0:2]), int(data['time'][2:4]), int(data['time'][5:7])),
        'modes': data['modes'].strip(),
        'nickname': data['nickname'].split(':')[0],
        'message': data['message'].strip(),
        'path': path
    }

    result = es.index(index="irclogs", doc_type="message", body=obj)


es.indices.refresh('irclogs')

