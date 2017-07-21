# (C) Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import six

from oslo_config import cfg
from oslo_log import log
from oslo_versionedobjects import exception
from oslo_versionedobjects import fields
from oslo_versionedobjects import base

from freezerclient.v1 import client

Enum = fields.Enum
BaseEnumField = fields.BaseEnumField
LOG = log.getLogger(__name__)
CONF = cfg.CONF


global api
api = None


def api_client():
    global api
    if api is None:
        api = client.Client(opts=CONF, insecure=False if CONF.insecure else True)
    return api


class BackupStatus(Enum):
    ERROR = 'error'
    ERROR_DELETING = 'error_deleting'
    CREATING = 'creating'
    AVAILABLE = 'available'
    DELETING = 'deleting'
    DELETED = 'deleted'
    RESTORING = 'restoring'

    ALL = (ERROR, ERROR_DELETING, CREATING, AVAILABLE, DELETING, DELETED,
           RESTORING)

    def __init__(self):
        super(BackupStatus, self).__init__(valid_values=BackupStatus.ALL)


class BackupStatusField(BaseEnumField):
    AUTO_TYPE = BackupStatus()


@base.VersionedObjectRegistry.register
class Backup(base.VersionedObject):
    fields = {
        'backend_id': fields.UUIDField(),
        'curr_backup_level': fields.IntegerField(nullable=True),
        'client_os': fields.StringField(nullable=True),
        'source_id': fields.UUIDField(),
        'project_id': fields.UUIDField(),
        'size': fields.IntegerField(nullable=True),
        'description': fields.StringField(nullable=True),
        'end_time_stamp': fields.IntegerField(nullable=True),
        'backup_chain_name': fields.StringField(nullable=True),
        'parent_id': fields.StringField(nullable=True),
        'client_version': fields.StringField(nullable=True),
        'time_stamp': fields.IntegerField(nullable=True),
        'action': fields.StringField(default='backup'),
        'always_level': fields.StringField(default=False),
        'backup_name': fields.StringField(nullable=True),
        'hostname_backup_name': fields.StringField(nullable=True),
        'container': fields.StringField(default='backups'),
        'hostname': fields.StringField(nullable=True),
        'storage': fields.StringField(default='ceph'),
        'mode': fields.StringField(default='cindernative'),
        'status': BackupStatusField(nullable=True),
        'compression': fields.StringField(nullable=True),
        'consistency_checksum': fields.StringField(nullable=True),
    }

    obj_extra_fields = ['backup_id', 'is_incremental', 'has_dependent_backups']

    def __init__(self, context=None, **kwargs):
        super(Backup, self).__init__(context, **kwargs)
        self.backup_id = None

    @property
    def is_incremental(self):
        return bool(self.parent_id)

    @property
    def obj_fields(self):
        return list(self.fields.keys()) + self.obj_extra_fields

    def __iter__(self):
        for name in self.obj_fields:
            if (self.obj_attr_is_set(name) or
                    name in self.obj_extra_fields):
                yield name

    iterkeys = __iter__

    def itervalues(self):
        for name in self:
            yield getattr(self, name)

    def iteritems(self):
        for name in self:
            yield name, getattr(self, name)

    if six.PY3:
        # Python 3 dictionaries don't have iterkeys(),
        # itervalues() or iteritems() methods. These methods are provided to
        # ease the transition from Python 2 to Python 3.
        keys = iterkeys
        values = itervalues
        items = iteritems
    else:
        def keys(self):
            return list(self.iterkeys())

        def values(self):
            return list(self.itervalues())

        def items(self):
            return list(self.iteritems())

    def __getitem__(self, name):
        return getattr(self, name)

    def __setitem__(self, name, value):
        setattr(self, name, value)

    def obj_load_attr(self):
        pass

    def create(self):
        if self.obj_attr_is_set('backup_id'):
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.obj_get_changes()
        backup_id = api_client().backups.create(updates)
        self.backup_id = backup_id

        self.obj_reset_changes()

    @classmethod
    def get_latest_backup(cls, source_id=None, from_timestamp=None):
        backups = api_client().backups.list(limit=100)
        backups = list(filter(lambda x: x['backup_metadata']['source_id'] ==
                       source_id and x['backup_metadata']['status'] == 'available', backups))
        backups.sort(key=lambda x: x['backup_metadata']['time_stamp'])

        if not backups:
            return None
        backup = backups[-1]
        if from_timestamp:
            backups = list(filter(lambda x: x['backup_metadata']['time_stamp']
                           < from_timestamp, backups))
            backup = backups[-1]
        return cls._from_db_backup(cls(), backup)

    def save(self):
        updates = self.obj_get_changes()
        if updates:
            api_client().backups.update(self.backup_id, updates)

        self.obj_reset_changes()

    def destroy(self):
        api_client().backups.delete(self.backup_id)

    def to_primitive(self):
        primitive = dict()
        for name, field in self.fields.items():
            primitive[name] = field.primitive(self, name,
                                              getattr(self, name))
        return primitive

    @staticmethod
    def _from_db_backup(backup, db_backup):
        backup_meta = db_backup.get('backup_metadata', {})
        for name, field in backup.fields.items():
            value = backup_meta.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            if value is None:
                continue
            backup[name] = value

        backup.backup_id = db_backup.get('backup_id')
        backup.obj_reset_changes()
        return backup

    @classmethod
    def get_by_id(cls, backup_id):
        db_backup = api_client().get(backup_id)
        return cls._from_db_backup(cls(), db_backup)



