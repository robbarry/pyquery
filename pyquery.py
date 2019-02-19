import sys
import db
import csv
import json
from unidecode import unidecode
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

arg_flag = False

try:
    arg = sys.argv[1].lower()
    if arg[0] == "-":
        arg_flag = True
except:
    pass

if arg_flag:
    show_argument(arg)

query = dict()
try:
    query["server"] = sys.argv[1]
    query["project"] = sys.argv[2]
    query["title"] = sys.argv[3]
    query["query"] = sys.argv[4]
    if query["server"] == "data1":
        query["server"] = "10.2.173.9"
    query["created"] = str(datetime.now())
    try:
        query["description"] = sys.argv[5]
    except:
        pass

except:
    try:
        query = load_query(sys.argv[1])
    except:
        how_it_works()

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
