from boto import connect_sdb
from uuid import uuid4
import time

class SDBObject(object):
    invalid_attributes = ['_id', '_version', '_attributes']
    # {'attribute': (validator_function, required)}
    schema = {}

    def __init__(self, connection, domain, _id=None, **kwargs):
        self.connection = connection
        self.domain = connection.get_domain(domain)
        object.__setattr__(self, '_id', _id)
        object.__setattr__(self, '_attributes', dict())
        # 0 is invalid for objects stored in SimpleDB
        object.__setattr__(self, '_version', 0)

    def __getattr__(self, name):
        if name in self.schema:
            self.refresh()
            try:
                return self._attributes[name]
            except KeyError:
                raise AttributeError(name)
        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name in self.invalid_attributes:
            raise AttributeError('%s is a private attribute and cannot be'
                    'set.' % (name,))
        if name in self.schema:
            if self._id:
                self.refresh()
            self._attributes[name] = self._validate_field(name, value)
        object.__setattr__(self, name, value)

    def _validate_field(self, name, value):
        validator = self.schema[name][0]
        return validator(value)

    def refresh(self, force=False):
        if not self._id:
            raise AttributeError('No object id specified.')
        if self._version and not force:
            return
        object.__setattr__(self, '_attributes',
                self.domain.get_attributes(self._id, consistent_read=True))
        object.__setattr__(self, '_version',
                int(self._attributes['_version']))

    def save(self):
        old_version = self._version
        new_version = old_version + 1
        if self._version:
            self._attributes['_version'] = new_version
            self.domain.put_attributes(self._id, self._attributes,
                    expected_value=['_version', old_version])
            object.__setattr__(self, '_version', new_version)
        else:
            if self._id:
                return
            else:
                _id = uuid4().hex
                object.__setattr__(self, '_id', _id)
                self._attributes['_version'] = new_version
                self.domain.put_attributes(_id, self._attributes)
                object.__setattr__(self, '_version', new_version)


class Note(SDBObject):
    schema = {
            'title': (str, True),
            'count': (int, False)
    }

    def __init__(self, connection, _id=None, **kwargs):
        domain = 'note'
        super(Note, self).__init__(connection, domain, _id, **kwargs)
