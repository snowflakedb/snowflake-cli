# Copyright (c) 2024 Snowflake Inc.
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

from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from click import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.cortex.types import (
    Language,
    Model,
    Question,
    SourceDocument,
    Text,
)
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

log = logging.getLogger(__name__)


class CortexManager(SqlExecutionMixin):
    def complete_for_prompt(
        self,
        text: Text,
        model: Model,
    ) -> str:
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                '{self._escape_input(text)}'
            ) AS CORTEX_RESULT;"""
        return self._query_cortex_result_str(query)

    def complete_for_conversation(
        self,
        conversation_json_file: SecurePath,
        model: Model,
    ) -> str:
        json_content = conversation_json_file.read_text(
            file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB
        )
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                PARSE_JSON('{self._escape_input(json_content)}'),
                {{}}
            ) AS CORTEX_RESULT;"""
        raw_result = self._query_cortex_result_str(query)
        json_result = json.loads(raw_result)
        return self._extract_text_result_from_json_result(
            lambda: json_result["choices"][0]["messages"]
        )

    def extract_answer_from_source_document(
        self,
        source_document: SourceDocument,
        question: Question,
    ) -> str:
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                '{self._escape_input(source_document)}',
                '{self._escape_input(question)}'
            ) AS CORTEX_RESULT;"""
        raw_result = self._query_cortex_result_str(query)
        json_result = json.loads(raw_result)
        return self._extract_text_result_from_json_result(
            lambda: json_result[0]["answer"]
        )

    def extract_answer_from_source_document_file(
        self,
        source_document_input_file: SecurePath,
        question: Question,
    ) -> str:
        source_document_content = source_document_input_file.read_text(
            file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB
        )
        return self.extract_answer_from_source_document(
            source_document=SourceDocument(source_document_content), question=question
        )

    def calculate_sentiment_for_text(
        self,
        text: Text,
    ) -> str:
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.SENTIMENT(
                '{self._escape_input(text)}'
            ) AS CORTEX_RESULT;"""
        return self._query_cortex_result_str(query)

    def calculate_sentiment_for_text_file(
        self,
        text_file: SecurePath,
    ) -> str:
        file_content = text_file.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
        return self.calculate_sentiment_for_text(
            text=Text(file_content),
        )

    def summarize_text(
        self,
        text: Text,
    ) -> str:
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
                '{self._escape_input(text)}'
            ) AS CORTEX_RESULT;"""
        return self._query_cortex_result_str(query)

    def summarize_text_file(
        self,
        text_file: SecurePath,
    ) -> str:
        file_content = text_file.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
        return self.summarize_text(
            text=Text(file_content),
        )

    def translate_text(
        self,
        text: Text,
        source_language: Optional[Language],
        target_language: Language,
    ) -> str:
        query = f"""\
            SELECT SNOWFLAKE.CORTEX.TRANSLATE(
                '{self._escape_input(text)}',
                '{source_language or ""}',
                '{target_language}'
            ) AS CORTEX_RESULT;"""
        return self._query_cortex_result_str(query)

    def translate_text_file(
        self,
        text_file: SecurePath,
        source_language: Optional[Language],
        target_language: Language,
    ) -> str:
        file_content = text_file.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
        return self.translate_text(
            text=Text(file_content),
            source_language=source_language,
            target_language=target_language,
        )

    @staticmethod
    def _escape_input(plain_input: str):
        # escape backslashes to not escape too much, this replace has to be the first one
        # escape single quotes because they are wrapping the whole string in SQL
        return plain_input.replace("\\", "\\\\").replace("'", "\\'")

    @staticmethod
    def _extract_text_result_from_json_result(
        extract_function: Callable[[], str]
    ) -> str:
        try:
            return extract_function()
        except (KeyError, IndexError) as ex:
            log.debug("Cannot find Cortex result message in a response", exc_info=ex)
            raise ClickException("Unexpected format of response from Snowflake")

    def _query_cortex_result_str(self, query: str) -> str:
        try:
            cursor = self._execute_query(query, cursor_class=DictCursor)
            if cursor.rowcount is None:
                raise SnowflakeSQLExecutionError(query)
            return str(cursor.fetchone()["CORTEX_RESULT"])
        except ProgrammingError as ex:
            log.debug("ProgrammingError occurred during SQL execution", exc_info=ex)
            raise ClickException(str(ex))
