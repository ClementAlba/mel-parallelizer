import pdal
import json


def load_pipeline(pipeline):
    with open(pipeline, 'r') as ppln:
        p = json.load(ppln)
    return p


def get_readers(pipeline):
    return list(filter(lambda x: x['type'].startswith('readers'), pipeline))


def get_writers(pipeline):
    return list(filter(lambda x: x['type'].startswith('writers'), pipeline))


def set_readers_filename(pipeline, filename):
    readers = get_readers(pipeline)
    readers[0]["filename"] = filename


def set_writers_filename():
    pass


def get_stages(pipeline):
    pass
