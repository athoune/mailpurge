#!/usr/bin/env python3
import os
from pprint import pprint
from datetime import datetime, timedelta

from imapclient import IMAPClient
import yaml

HEADERS = set(["list-id", "list-archive"])


assert os.getenv("IMAP"), "You need to set some ENVs"

server = IMAPClient(os.getenv("IMAP"), use_uid=True, ssl=True)
print(server.login(os.getenv("LOGIN"), os.getenv("PASSWORD")))


rules = yaml.load(open("purge.yml", "r"), Loader=yaml.Loader)
pprint(rules)
delta = timedelta(days=rules["old"])

select_info = server.select_folder("INBOX")
print("%d messages in INBOX" % select_info[b"EXISTS"])

now = datetime.now()
purged = 0

for purge in rules["purge"]:
    if purge is None:
        continue
    k, v = list(purge.items())[0]
    criteria = [k.upper(), v]
    if k.lower() in HEADERS:
        criteria = ["HEADER", k, v]
    messages = server.search(criteria)
    print("search", criteria, len(messages))
    prunes = []
    for msgid, data in server.fetch(messages, ["ENVELOPE"]).items():
        envelope = data[b"ENVELOPE"]
        if (now - envelope.date) > delta:
            prunes.append(msgid)
    print("Purge", len(prunes), "messages")
    purged += len(prunes)
    server.delete_messages(prunes)

print(purged, "purged")
