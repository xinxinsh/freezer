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

import sys
import socket
import six

from oslo_config import cfg
from oslo_log import log
from oslo_utils import excutils
from oslo_versionedobjects._i18n import _, _LE
from oslo_versionedobjects import exception
from oslo_versionedobjects import fields

from freezer import __version__ as FREEZER_VERSIO
from freezerclient.v1 import client

Enum = fields.Enum
LOG = log.getLogger(__name__)
CONF = cfg.CONF

global api
api = None


def api_client():
    """dummy implementation"""
    global api
    if api is None:
        api = client.Client(opts=CONF, insecure=False if CONF.insecure else True)
        if CONF.client_id:
            api.client_id = CONF.client_id
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


def _make_class_properties(cls):
    for name, field in six.iteritems(cls.fields):
        if not isinstance(field, fields.Field):
            raise exception.ObjectFieldInvalid(
                field=name, objname=cls.__name__)

        def getter(self, name=name):
            return getattr(self, name)

        def setter(self, value, name=name, field=field):
            field_value = field.coerce(self, name, value)
            self._changed_fields.add(name)
            try:
                return setattr(self, name, field_value)
            except Exception:
                with excutils.save_and_reraise_exception():
                    attr = "%s.%s" % (self.__name__, name)
                    LOG.exception(_LE('Error setting %(attr)s'),
                                  {'attr': attr})

        def deleter(self, name=name):
            if not hasattr(self, name):
                raise AttributeError("No such attribute `%s'" % name)
            delattr(self, name)

        setattr(cls, name, property(getter, setter, deleter))


class Backup(object):
    fields = {
        'backend_id': fields.UUIDField(nullable=True),
        'curr_backup_level': fields.IntegerField(default=0),
        'client_os': sys.platform,
        'source_id': fields.UUIDField(nullable=True),
        'project_id': fields.UUIDField(nullable=True),
        'description': fields.StringField(nullable=True),
        'end_time_stamp': fields.StringField(nullable=True),
        'backup_chain_name': fields.StringField(nullable=True),
        'is_incremental': fields.BooleanField(default=False),
        'client_version': FREEZER_VERSIO,
        'time_stamp': fields.StringField(nullable=True),
        'action': fields.StringField(default='backup'),
        'always_level': fields.StringField(default=False),
        'backup_name': fields.StringField(),
        'hostname_backup_name': fields.StringField(),
        'container': fields.StringField(default='backups'),
        'hostname': socket.gethostname(),
        'max_level': fields.BooleanField(default=False),
        'storage': fields.StringField(default='ceph'),
        'mode': fields.StringField(default='cindernative'),
        'status': BackupStatus(nullable=True),
        'compression': fields.StringField(nullable=True),
        'consistency_checksum': fields.StringField(nullable=True),
    }

    def __init__(self, **kwargs):
        self._changed_fields = set()
        self.backup_id = None

        _make_class_properties(self)
        for key in kwargs.keys():
            setattr(self, key, kwargs[key])

    @property
    def is_incremental(self):
        return bool(self.fields.is_incremental)

    def get_changes(self):
        """Returns a set of changed fields and their new values"""
        changed_fields = set([field for field in self._change_fields
                       if field in self.fields])
        changes = {}
        for key in changed_fields:
            changes[key] = getattr(self, key)
        return changes

    def reset_changes(self, fields=None):
        """Reset the list of fields that have been changed

        :param fields: List of fields to reset, or "all" if None.
        """
        if fields:
            self._changed_fields -= set(fields)
        else:
            self._changed_fields.clear()

    def create(self):
        if self.backup_id is not None:
            raise exception.ObjectActionError(action='create',
                                              reason='already created')
        updates = self.get_changes()
        backup_id = api_client().backups.create(updates)
        self.backup_id = backup_id

        self.reset_changes()

    def save(self):
        updates = self.get_changes()
        if updates:
            api_client().backups.update(self.backup_id, updates)

        self.reset_changes()

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
        for name, field in backup.fields.items():
            value = db_backup.get(name)
            if isinstance(field, fields.IntegerField):
                value = value if value is not None else 0
            backup[name] = value

        backup.backup_id = db_backup.get('backup_id')
        backup.reset_changes()
        return backup

    @classmethod
    def get_by_id(cls, backup_id):
        db_backup = api_client().get(backup_id)
        return cls._from_db_backup(cls(), db_backup)



