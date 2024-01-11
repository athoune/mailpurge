#!/usr/bin/env python3
from pprint import pprint
from datetime import datetime, timedelta
from itertools import batched

from imapclient import IMAPClient
import yaml

HEADERS = ["list"]
BATCH_DELETE_SIZE = 500


def flamer(
    server: IMAPClient, rules: dict, debug: bool = False
) -> int:
    delta = timedelta(days=rules["old"])


    now = datetime.now()
    purged = 0

    for folder, rules in rules["purge"].items():
        select_info = server.select_folder(folder)
        if debug:
            print("%d messages in INBOX" % select_info[b"EXISTS"])
        for purge in rules:
            k, v = list(purge.items())[0]
            criteria = [k.upper(), v]
            for prefix in HEADERS:
                if k.lower().startswith(prefix):
                    criteria = ["HEADER", k, v]
                    break
            if k.find("-") != -1:
                criteria = ["HEADER", k, v]
            messages = server.search(criteria)
            if debug:
                print("search", criteria, len(messages))
            prunes = []
            for msgid, data in server.fetch(messages, ["ENVELOPE"]).items():
                if b"ENVELOPE" not in data:
                    continue
                envelope = data[b"ENVELOPE"]
                if (now - envelope.date) > delta:
                    prunes.append(msgid)
            if debug:
                print("Purge", len(prunes), "messages")
            purged += len(prunes)
            for batch in batched(prunes, BATCH_DELETE_SIZE):
                server.delete_messages(batch)
                exp = server.expunge()
                if debug:
                    print("expunge", exp)
    return purged


if __name__ == "__main__":
    import os
    assert os.getenv("IMAP"), "You need to set some ENVs"

    client = IMAPClient(os.getenv("IMAP"), use_uid=True, ssl=True)
    print(client.login(os.getenv("LOGIN"), os.getenv("PASSWORD")))
    print("Capabilities", client.capabilities())
    print("ID", client.id_())
    print("Quota", client.get_quota())
    print("Folders", client.list_folders())

    rules = yaml.load(open("purge.yml", "r"), Loader=yaml.Loader)
    pprint(rules)

    purged = flamer(client, rules, debug=True)
    print(purged, "purged")
    client.logout()
