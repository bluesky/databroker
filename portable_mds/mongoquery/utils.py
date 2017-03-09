import json
from mongoquery import Query


def find_one(collection, query):
    """
    Parameters
    ----------
    collection : list of dicts
    query: dict
    
    Returns
    -------
    result : dict
    """
    match = Query(query).match
    for doc in collection:
        if match(doc):
            return doc
        return None


def find(collection, query, sort=[]):
    """
    Parameters
    ----------
    collection : list of dicts
    query: dict
    sort : list
        example: [('time', ASCENDING)]
        More than one element is not supported.

    Returns
    -------
    results : list
    """
    match = Query(query).match
    result = filter(match, collection)
    if sort is None:
        return result
    elif len(sort) > 2:
        raise NotImplementedError("Only one sort key is supported.")
    else:
        key, reverse = sort
        sorted_result = sorted(result, key=lambda x: x[key], reverse=reverse)
        # Make it a generator so it is the same as the unsorted code path.
        return (elem for elem in sorted_result)


def insert_one(filepath, doc):
    with open(filepath, 'r') as f:
        data = json.load(f)
    data.append(doc)
    with open(filepath, 'w') as f:
        json.dump(f, data)


def insert(filepath, docs):
    with open(filepath, 'r') as f:
        data = json.load(f)
    data.extend(docs)
    with open(filepath, 'w') as f:
        json.dump(f, data)
