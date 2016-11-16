dev
---

* Removed core function ``get_datumkw_by_resuid_gen`` which returned a
  generator of datum kwargs. Replaced it with ``get_datum_by_res_gen`` which
  returns a generator of complete datum documents. The core function was
  exposed through the FileStore method ``datum_gen_given_resource`` which,
  correspondingly, now returns full datum documents, not just datum kwargs.

* trying to copy or move files who's resource does not have a 'root' will
  intentionally raise.  Previously, this would result in an very obscure error.
