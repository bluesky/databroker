The scripts in this directory generate example data in temporary files or in
databases named `test-<UUID4>` and then construct a `databroker.v2.Broker`
instance aimed at that data.

```sh
$ ipython -i generate_msgpack_data.py
```

This starts an IPython session where the variable ``catalog`` is defined and can
be used such as

```py
In [1]: catalog                                                                                                                                                                               
Out[1]: <Intake catalog: None>

In [2]: list(catalog)                                                                                                                                                                         
Out[2]: 
['3d3a6422-1fce-4375-a4e4-41edf63fa7b0',
 '7eed4b9d-9bc9-471a-846e-bdd9a7c7536e']

In [3]: catalog[-1]                                                                                                                                                                           
Out[3]: 
Run Catalog
  uid='7eed4b9d-9bc9-471a-846e-bdd9a7c7536e'
  exit_status='success'
  2020-01-16 08:45:37.628 -- 2020-01-16 08:45:37.631
  Streams:
    * primary
```
