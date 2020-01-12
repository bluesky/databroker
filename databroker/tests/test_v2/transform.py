def transform(doc):
    doc = dict(doc)
    assert 'test_key' not in doc.keys()
    doc['test_key'] = 'test_value'
    return doc
