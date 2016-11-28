"""
This module implements a simple in-memory data store. It supports schema validation and watchers.
Schema validation helps catch errors with data. Watchers helps decouple components. Components
watch the store for changes, without needing to know which other components are mutating the data
in the store.

For more examples, check the tests is test_store.py, or look at the individual method docstrings.
"""

from copy import deepcopy

WATCH_ACTION_PUT = 'put'
WATCH_ACTION_DELETE = 'delete'

class ValidationError(Exception):
    # TODO: Add more useful information for validation errors
    pass

def merge(v0, v1):
    """
    Recursively merges two values. For simple types and lists, v1 replaces v0. For dicts, the keys
    of both are merged. In the event of duplicate keys, the keys in v1 win.

    Returns the merged value.
    """
    if isinstance(v1, dict):
        if isinstance(v0, dict):
            d = v0.copy()

            for k, v in v1.items():
                d[k] = merge(d.get(k), v)

            return d
        else:
            return dict(v1)
    else:
        return v1

def validate(v, s):
    """
    Recursively validates a value against a schema. Returns False if v is invalid, else True-y.
    """
    if not s:
        return False
    elif isinstance(s, dict):
        if isinstance(v, dict):
            sk, sv = dict(s).popitem()

            if isinstance(sk, type):
                return not [v for k, v in v.items()
                            if not (isinstance(k, sk) and validate(v, sv))]
            else:
                return not [v for k, v in v.items()
                            if not (s.get(k) and validate(v, s[k]))]
        else:
            False
    elif isinstance(s, list):
        if isinstance(v, list):
            return not [v for v in v if not validate(v, s[0])]
        else:
            return False
    elif isinstance(s, type):
        return isinstance(v, s)
    elif callable(s):
        return s(v)
    else:
        return False

class Store():
    """
    The Store constructor takes a single argument, a schema. A schema is a dict containing
    information about entities and their attributes.

    To create a store with a schema:

        store = Store({
          'entity-a': {
            'attr-a': str,
            'attr-b': [int],
            'attr-c': {'id': str},
            'attr-d': {int: {str: str}}
          },
          'entity-b': {
            'attr-e': str
          }
        })

    In the above snippet, the keys 'entity-a' and 'entity-b' are top-level entity types. Their
    values describe the attributes for an entity type. Attributes can be simple types (like str
    or int), lists, or dicts. Schemas are processed recursively, so lists and dicts may contain
    other lists and dicts. dict keys may be either a simple type, or a string. A string key is
    matched exactly for the purposes of validation.
    """
    def __init__(self, schema):
        self.schema = schema
        self.entities = {k: {} for k in schema.keys()}
        self.watchers = {}

    def merge(self, type, id, e):
        """
        Merges partial entity data with data for entity alread in the store. Merge behavior depends on
        the type of data being merged. Simple types (like str and int) and lists are replaced, while
        dicts are merged. In the event of duplicate keys, the data passed to merge wins.

        # assuming entity-a with an id of 1 looks like {'attr-a': 'a'}
        store.merge('entity-a', 1, {'attr-b': [1]}) # => {'attr-a': 'a', 'attr-b': [1]}

        Returns the updated entity.
        """
        if self.is_entity_type(type):
            return self.put(type, id, merge(self.entities[type].get(id, {}), e))

    def delete(self, type, id):
        """
        Delete an entity from the store. Requires an entity type, and an id:

        store.delete('entity-a', 1) # => {...}

        Returns the deleted entity.
        """
        if self.is_entity_type(type) and id in self.entities[type]:
            e = self.entities[type][id]

            try:
                del self.entities[type][id]
            except KeyError:
                pass

            for (f, args) in self.watchers.values():
                f(WATCH_ACTION_DELETE, type, id, e, *args)

            return e
        else:
            return None

    def put(self, type, id, e):
        """
        Add an entity to the store. Requires an entity type, an id, and a value:

            store.put('entity-a', 1, {'attr-a': 'hello'}) # => {'attr-a': ...}

        Returns the added entity.

        Entities need not have all possible attributes. There is no notion of "required".

        A ValidationError is thrown only when an attribute of the wrong type is provided:

            store.put('entity-a', 1, {'attr-a': 1}) # would throw ValidationError
        """
        if self.is_entity_type(type):
            if validate(e, self.schema[type]):
                self.entities[type][id] = deepcopy(e)

                for (f, args) in self.watchers.values():
                    f(WATCH_ACTION_PUT, type, id, e, *args)

                return e
            else:
                raise ValidationError()

    def get(self, type, id):
        """
        Get an entity from the store. Requires an entity type, and an id:

            store.get('entity-a', 1) # => {...}

        Returns a copy of the entity in the store, to avoid accidental mutation.
        """
        if self.is_entity_type(type) and id in self.entities[type]:
            return deepcopy(self.entities[type][id])
        else:
            return None

    def all(self, type):
        """
        Get all entities of type:

            store.all('entity-a') # => {1: {...}}

        Returns a dict of entities by id.

        Only copies of entites are returned, to avoid accidental mutation.
        """
        if self.is_entity_type(type):
            return deepcopy(self.entities[type])

    def is_entity_type(self, type):
        """
        Determine if entity type is valid. An entity type is valid if it has a schema.

        Returns True or False.
        """
        return type in self.schema

    def add_watch(self, id, w, *args):
        """
        Add a watch function with an id. Watches are called when the store is mutated, either
        by a put (either directly, or by a call to merge) or a delete. Any additional arguments
        are passed along to the watch function when it is called, in addition to the type of
        action (either WATCH_ACTION_PUT or WATCH_ACTION_DELETE), the entity type, id, and data.
        Be careful about mutating the store from a watch function, as nothing is done to prevent
        infinite loops.

            store.add_watch('a-watch', lambda action, type, id, data: print('okay'))

        """
        self.watchers[id] = (w, args)

    def remove_watch(self, id):
        """
        Remove a watch function with the id provided to add_watch.
        """
        del self.watchers[id]
