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

from oslo_config import cfg
from oslo_log import log

from freezer.exceptions import utils
from freezerclient.v1 import client


CONF = cfg.CONF
LOG = log.getLogger(__name__)


class BackupQuota(object):

    def __init__(self):
        self._api_client = None

    @property
    def api_client(self):
        """Lazy load the driver so we give a chance for the config file to
            be read before grabbing the config for which api_client to use'
        """
        if self._api_client:
            return self._api_client

        self._api_client = client.Client(opts=CONF, insecure=False if CONF.insecure else True)
        return self._api_client

    def reserve(self, backups, backup_bytes, **kwargs):
        """reserve backup quota and update quota record in db"""
        quota_list = self.api_client.quotas.list(limit=1, offset=0, search=None)
        quota = quota_list[0] if quota_list else None

        if quota:
            if (quota['max_num'] < (quota['used_num'] + backups) or
                    quota['max_vol'] < (quota['used_vol'] + backup_bytes)):
                raise utils.ExceedQuotaException("Exceed backup quota limitation")
            quota['used_num'] += backups
            quota['used_bytes'] += backup_bytes
            self.api_client.quotas.update(quota['quota_id'], quota)
            return quota
        else:
            return {}

    def commit(self, **kwargs):
        pass

    def rollback(self, backups, backup_bytes, **kwargs):
        """rollback backup quota and update quota record in db"""
        quota_list = self.api_client.quotas.list(limit=1, offset=0, search=None)
        quota = quota_list[0] if quota_list else None
        if quota:
            quota['used_num'] -= backups
            quota['used_vol'] -= backup_bytes
            self.api_client.quotas.update(quota['quota_id'], quota)

QUOTA = BackupQuota()
