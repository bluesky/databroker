import operator

from .queries import PartialUID, ScanID


class BlueskyEventStreamMixin:
    "Convenience methods used by the server- and client-side"

    def __repr__(self):
        return f"<{type(self).__name__} {set(self)!r} stream_name={self.metadata['stream_name']!r}>"

    @property
    def descriptors(self):
        return self.metadata["descriptors"]

    def read(self):
        """
        Shortcut for reading the 'data' (as opposed to timestamps or config).

        That is:

        >>> stream.read()

        is equivalent to

        >>> stream["data"].read()
        """
        return self["data"].read()


class BlueskyRunMixin:
    "Convenience methods used by the server- and client-side"

    def __repr__(self):
        metadata = self.metadata
        return (
            f"<{type(self).__name__} "
            f"{set(self)!r} "
            f"scan_id={metadata['start'].get('scan_id', 'UNSET')!s} "  # (scan_id is optional in the schema)
            f"uid={metadata['start']['uid'][:8]!r}"  # truncated uid
            ">"
        )


class CatalogOfBlueskyRunsMixin:
    "Convenience methods used by the server- and client-side"

    def __repr__(self):
        # This is a copy/paste of the general-purpose implementation
        # tiled.catalog.utils.catalog_repr
        # with some modifications to extract scan_id from the metadata.
        sample = self.items_indexer[:10]
        # Use scan_id (int) if defined; otherwise fall back to uid.
        sample_reprs = [
            repr(value.metadata["start"].get("scan_id", key)) for key, value in sample
        ]
        out = "<Catalog {{"
        # Always show at least one.
        if sample_reprs:
            out += sample_reprs[0]
        # And then show as many more as we can fit on one line.
        counter = 1
        for sample_repr in sample_reprs[1:]:
            if len(out) + len(sample_repr) > 60:  # character count
                break
            out += ", " + sample_repr
            counter += 1
        approx_len = operator.length_hint(self)  # cheaper to compute than len(catalog)
        # Are there more in the catalog that what we displayed above?
        if approx_len > counter:
            out += f", ...}} ~{approx_len} entries>"
        else:
            out += "}>"
        return out

    def __getitem__(self, key):
        # For convenience and backward-compatiblity reasons, we support
        # some "magic" here that is helpful in an interactive setting.
        if isinstance(key, str):
            # CASE 1: Interpret key as a uid or partial uid.
            results = self.search(PartialUID(key))
            if not results:
                raise KeyError(f"No Run matches the (partial) uid {key}")
            else:
                # By construction there must be only one result. Return it.
                return results.values_indexer[0]
        elif isinstance(key, int):
            if key > 0:
                # CASE 2: Interpret key as a scan_id.
                results = self.search(ScanID(key, duplicates="latest"))
                if not results:
                    raise KeyError(f"No match for scan_id={key}")
                else:
                    # By construction there must be only one result. Return it.
                    return results.values_indexer[0]
            else:
                # CASE 3: Interpret key as a recently lookup, as in
                # `catalog[-1]` is the latest entry.
                return self.values_indexer[-1]
