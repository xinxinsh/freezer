# (c) Copyright 2016 Hewlett-Packard Enterprise Development , L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from freezer.mode import mode


class NovaMode(mode.Mode):
    """
    Execute a nova backup/restore
    """
    def __init__(self, conf):
        self.conf = conf

    @property
    def name(self):
        return "nova"

    @property
    def version(self):
        return "1.0"

    def release(self):
        pass

    def prepare(self):
        pass
