# There are methods that IPython will try to call.
# We special-case them because we want to avoid the getattr
# resulting in an unnecessary network hit just to raise
# AttributeError.

IPYTHON_METHODS = {"_ipython_canary_method_should_not_exist_", "_repr_mimebundle__ipython_display_"}
