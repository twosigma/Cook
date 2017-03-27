
import pytest

from cook.store import Store, WATCH_ACTION_PUT, WATCH_ACTION_DELETE, ValidationError

schema = {
    'Entity': {
        'attr-a': str,
        'attr-b': [
            {
                'sub-attr-a': str,
                'sub-attr-b': str
            }
        ],
        'attr-c': {str: float},
        'attr-d': lambda v: isinstance(v, str) and len(v) < 4
    }
}

entity  = {
    'attr-a': 'some-value',
    'attr-b': [
        {
            'sub-attr-a': 'a-str'
        }
    ],
    'attr-c': {'hello': 2.0}
}

def test_store_access():
    s = Store(schema)

    assert s.put('Entity', 1, entity) is entity
    assert s.get('Entity', 1) == entity

def test_store_delete():
    s = Store(schema)

    assert not s.delete('Entity', 1)
    assert s.put('Entity', 1, entity) is entity
    assert s.delete('Entity', 1) == entity
    assert not s.get('Entity', 1)

def test_store_all():
    s = Store(schema)

    assert not s.all('Entity')
    assert s.put('Entity', 1, entity) is entity
    assert s.all('Entity') == {1: entity}

def test_store_merge():
    s = Store(schema)
    e = entity.copy()
    u = {'attr-a': 'some-other-value'}

    e.update(u)

    assert s.merge('Entity', 1, entity) == entity
    assert s.merge('Entity', 1, u) == e
    assert s.get('Entity', 1) == e

def test_store_validation():
    s = Store(schema)

    with pytest.raises(ValidationError):
        s.put('Entity', 1, {'not-attr': 'some-other-value'})

def test_store_function_validation():
    s = Store(schema)

    assert s.put('Entity', 1, {'attr-d': 'aaa'})

    with pytest.raises(ValidationError):
        s.put('Entity', 1, {'attr-d': 'some-long-value'})

def test_store_watchers():
    s = Store(schema)
    a = []

    def on_mutate(action, type, id, e, a):
        a.append(action)

    s.add_watch('my_watch', on_mutate, a)

    assert s.put('Entity', 1, entity) is entity
    assert a == [WATCH_ACTION_PUT]
    assert s.delete('Entity', 1) == entity
    assert a == [WATCH_ACTION_PUT, WATCH_ACTION_DELETE]

    s.remove_watch('my_watch')

    assert s.put('Entity', 1, entity) is entity
    assert a == [WATCH_ACTION_PUT, WATCH_ACTION_DELETE]
