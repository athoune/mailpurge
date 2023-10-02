#!/usr/bin/env python3
from pprint import pprint
from datetime import datetime, timedelta

from imapclient import IMAPClient
import yaml

HEADERS = ["list", "x-github"]


def flamer(
    server: IMAPClient, rules: dict, box: str = "INBOX", debug: bool = False
) -> int:
    delta = timedelta(days=rules["old"])

    select_info = server.select_folder(box)
    if debug:
        print("%d messages in INBOX" % select_info[b"EXISTS"])

    now = datetime.now()
    purged = 0

    for purge in rules["purge"]:
        if purge is None:
            continue
        k, v = list(purge.items())[0]
        criteria = [k.upper(), v]
        for prefix in HEADERS:
            if k.lower().startswith(prefix):
                criteria = ["HEADER", k, v]
                break
        messages = server.search(criteria)
        if debug:
            print("search", criteria, len(messages))
        prunes = []
        for msgid, data in server.fetch(messages, ["ENVELOPE"]).items():
            envelope = data[b"ENVELOPE"]
            if (now - envelope.date) > delta:
                prunes.append(msgid)
        if debug:
            print("Purge", len(prunes), "messages")
        purged += len(prunes)
        server.delete_messages(prunes)
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
