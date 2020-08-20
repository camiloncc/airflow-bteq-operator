# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging

from bcitools.custom_hooks.ttu_hook import TtuHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class BteqOperator(BaseOperator):
    """
    This hook is a wrapper around the BTEQ binary to kick off a BTEQ file.
    It requires that the "BTEQ" binary is in the PATH
    :param bteq: bteq file with .bteq or .sql extension
    :type bteq: str
    """
    template_fields = ('sql',)
    template_ext = ('.sql', '.bteq',)
    ui_color = '#ff976d'

    @apply_defaults
    def __init__(self,
                 bteq='',
                 xcom_push=False,
                 conn_id='ttu_default',
                 *args,
                 **kwargs):
        super(BteqOperator, self).__init__(*args, **kwargs)
        self.sql = bteq
        self._hook = None
        self._conn_id = conn_id
        self.xcom_push = xcom_push

    def execute(self, context):
        """
        Call the BteqHook to run the provided BTEQ File
        """
        self._hook = TtuHook(ttu_conn_id=self._conn_id)
        self._hook.run_bteq(self.sql, self.xcom_push)

    def on_kill(self):
        self._hook.on_kill()
