def transform(doc):
    assert 'test_key' not in doc.keys()
    doc['test_key'] = 'test_value'
    return doc
