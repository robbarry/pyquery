import sys
import db
import csv
import json
import argparse
from datetime import datetime

def how_it_works():
    print("Syntax:")
    print()
    print("To run a new query:")
    print("[server IP address] [project] [title] \"[query]\" (\"description\")")
    print()
    print("To run a saved query:")
    print("[title]")
    print()
    print("To list queries:")
    print("-list")
    print()
    sys.exit()

def load_query(title):
    return json.load(open("meta/{}.json".format(title)))

def save_query(query):
    title = query["title"]
    with open("meta/{}.json".format(title), "w") as f:
        json.dump(query, f)

def print_params(data):
    from collections import OrderedDict
    import os
    params = OrderedDict()
    title = data["title"]
    print_title = title.upper()
    try:
        data["size"] = os.path.getsize("data/{}.csv".format(title))
    except:
        pass

    print("*** {} ***".format(print_title))
    params["project"] = "Project"
    params["created"] = "Created at"
    params["last_run"] = "Last run"
    params["description"] = "Description"
    params["size"] = "Result set file size"
    params["rows_count"] = "Result set rows count"
    params["query"] = "SQL query"
    for k, v in params.items():
        if k in data:
            print("{}: {}".format(v, data[k]))

def show_argument(arg):
    if arg == "-list":
        import glob
        for filename in glob.glob("meta/*.json"):
            data = json.load(open(filename, "rb"))
            print_params(data)
            print()

    sys.exit()


parser = argparse.ArgumentParser()
parser.add_argument("--server", "-s", help = "Server IP address", required=True)
parser.add_argument("--project", "-p", help = "Project", required=True)
parser.add_argument("--title", "-t", help="Query title", required=True)
parser.add_argument("--query", "-q", help="Query text", required=True)
parser.add_argument("--description", "-d", help="Description", default = "")
args = parser.parse_args()

query = dict()

query["server"] = args.server
query["project"] = args.project
query["title"] = args.title
query["query"] = args.query
if query["server"] == "data1":
    query["server"] = "10.2.173.9"
query["created"] = str(datetime.now())    
query["description"] = args.description

dbc = db.mssql(query["server"], "master")
rows_count = 0
with open("data/{}.csv".format(query["title"]), "w") as f:
    header = None
    for row in dbc.query_range(query["query"]):
        # for k, v in row.items():
        #     try:
        #         row[k] = v.encode("utf-8")
        #     except:
        #         pass
            # try:
            #     row[k] = unidecode(v)
            # except:
            #     pass
            # if isinstance(v, basestring):
            #     row[k] = unidecode(v)
        if header is None:
            header = row.keys()
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
        writer.writerow(row)
        rows_count += 1

# message = {"recipient": "+13054953678", "message": "Query {} complete".format(query["title"])}
# dbc.insert("alerts.dbo.signal_queue", message)

dbc = None

query["last_run"] = str(datetime.now())
query["rows_count"] = rows_count
save_query(query)
