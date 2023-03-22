from bluesky import RunEngine
from bluesky.plans import count
from tiled.client import Context, from_context
from tiled.server.app import build_app_from_config

from ..mongo_normalized import MongoAdapter, SimpleAccessPolicy


def test_access_policy_pass_through():
    # Ensure access_policy is propagated to __init__.

    access_policy = SimpleAccessPolicy({}, key="...", provider="...")

    class InstrumentedMongoAdapter(MongoAdapter):
        def __init__(self, *args, **kwargs):
            assert kwargs["access_policy"] is not None

    InstrumentedMongoAdapter.from_uri(
        "mongodb://localhost:27017/dummy",  # never actually connects
        access_policy=access_policy,
    )
    InstrumentedMongoAdapter.from_mongomock(access_policy=access_policy)


def test_access_policy_example(tmpdir, enter_password):

    config = {
        "authentication": {
            "providers": [
                {
                    "provider": "toy",
                    "authenticator": "tiled.authenticators:DictionaryAuthenticator",
                    "args": {"users_to_passwords": {"alice": "secret"}},
                }
            ],
        },
        "trees": [
            {
                "path": "/",
                "tree": "databroker.mongo_normalized:MongoAdapter.from_mongomock",
                "access_control": {
                    "access_policy": "databroker.mongo_normalized:SimpleAccessPolicy",
                    "args": {
                        "provider": "toy",
                        "key": "color",
                        "access_lists": {
                            "alice": ["blue", "red"],
                        },
                    },
                },
            }
        ],
    }
    with Context.from_app(build_app_from_config(config), token_cache=tmpdir) as context:
        with enter_password("secret"):
            client = from_context(context, username="alice", prompt_for_reauthentication=True)

        def post_document(name, doc):
            client.post_document(name, doc)

        RE = RunEngine()
        RE.subscribe(post_document)
        (red_uid,) = RE(count([], md={"color": "red"}))
        (blue_uid,) = RE(count([], md={"color": "green"}))
        (blue_uid,) = RE(count([], md={"color": "blue"}))

        # alice can see red and blue but not green
        assert set(client.keys()) == {red_uid, blue_uid}
