#!/usr/bin/env python3

from email.parser import BytesParser
from email.header import Header
from email.message import Message
from datetime import date

from imapclient import IMAPClient
import plyvel
import orjson


def iterate_per_year(client: IMAPClient, start: int = 2000, stop: int = 2024):
    for year in range(start, stop):
        messages = client.search(["BEFORE", date(year, 1, 1),
                                 "SINCE", date(year - 1, 1, 1)])
        if len(messages) == 0:
            continue
        print("# messages", len(messages))
        if len(messages) == 1:
            print("debug", type(messages[0]))
        yield year, messages


def default(obj):
    if isinstance(obj, Header):
        return str(obj)
    elif isinstance(obj, Message):
        return list((k.lower(), default(v)) for k, v in obj.items())
    return obj


class HeadersCache:
    def __init__(self, client: IMAPClient, path: str = "./headers.cache"):
        self.db = plyvel.DB(path, create_if_missing=True)
        self.client = client

    def get(self, *ids):
        print("get", ids)
        todo = []
        parser = BytesParser()

        for id_ in ids:
            r = self.db.get(str(id_).encode())
            if r is None:
                todo.append(id_)
            else:
                yield id_, orjson.loads(r)

        while len(todo):
            size = min(len(todo), 100)
            ids = todo[:size]
            wb = self.db.write_batch()
            print("messages", ids, size, todo)
            for msgid, data in self.client.fetch(ids, ["RFC822"]).items():
                if msgid is None:
                    continue
                email = parser.parsebytes(text=data[b"RFC822"],
                                          headersonly=True)
                headers = orjson.dumps(email, default=default)
                wb.put(str(msgid).encode(), headers)
                yield msgid, orjson.loads(headers)
            todo = todo[size:]
            wb.write()


if __name__ == "__main__":
    import os
    assert os.getenv("IMAP"), "You need to set some ENVs"

    client = IMAPClient(os.getenv("IMAP"), use_uid=True, ssl=True)
    print(client.login(os.getenv("LOGIN"), os.getenv("PASSWORD")))
    client.select_folder("INBOX")
    cache = HeadersCache(client)
    for year, messages in iterate_per_year(client, start=2005):
        print(year)
        for id_, headers in cache.get(*messages):
            print("\t", headers)
