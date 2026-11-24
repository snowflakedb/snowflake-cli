# Copyright (c) 2026 Snowflake Inc.
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

"""Tests for ``snow app templates`` and ``snow app init``.

A local HTTP server stands in for GitHub: it serves the commit lookup, the
catalog, and the template files at the paths GitHub uses.
"""

import hashlib
import json
import socket
from unittest.mock import patch

import pytest
import requests
import yaml
from snowflake.cli._plugins.apps import templates
from snowflake.cli._plugins.apps.app_yml import AppYmlDefinition
from snowflake.cli._plugins.apps.feature_flags import SnowflakeAppFeatureFlag
from snowflake.cli._plugins.apps.templates import (
    Template,
    TemplateRepository,
    TemplateSourceError,
    _network_failure,
    readable_label,
    render_app_yml,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.metrics import CLIMetrics
from snowflake.cli.api.secure_path import SecurePath

from tests_common.feature_flag_utils import with_feature_flags

# The release tag points at COMMIT, where the templates are served. main is ahead.
COMMIT = "0123456789abcdef0123456789abcdef01234567"
MAIN_COMMIT = "89abcdef0123456789abcdef0123456789abcdef"
TAG_OBJECT = "fedcba9876543210fedcba9876543210fedcba98"
REPOSITORY_PATH = f"/snowflakedb/sar-sdk/{COMMIT}"
COMMANDS = "snowflake.cli._plugins.apps.commands"

NEXTJS_APP_YML = b"""\
# created-by: sar-sdk-templates; template=nextjs
version: 2
runtime: node

name: <%APP_NAME%>
database: <%DATABASE%>
schema: <%SCHEMA%>
query_warehouse: <%QUERY_WAREHOUSE%>

label: '<%LABEL%>'
description: A Next.js app.
"""

STREAMLIT_APP_YML = b"""\
# created-by: sar-sdk-templates; template=streamlit
version: 2
runtime: python
streamlit: true
name: <%APP_NAME%>
database: <%DATABASE%>
schema: <%SCHEMA%>
query_warehouse: <%QUERY_WAREHOUSE%>
compute_resource: SERVERLESS
label: '<%LABEL%>'
run:
  command: [".venv/bin/python", "launch.py"]
"""

FILES = {
    "nextjs": {
        "app.yml": NEXTJS_APP_YML,
        "package.json": b'{"name": "app"}\n',
        "app/[slug]/page.tsx": b"export default function Page() {}\n",
        "public/icon.svg": b"<svg/>\n",
    },
    "streamlit": {
        "app.yml": STREAMLIT_APP_YML,
        "streamlit_app.py": b"import streamlit as st\n",
    },
}


def _template_entry(template_id, files, **metadata):
    return {
        "id": template_id,
        "path": f"templates/{template_id}",
        "title": template_id.title(),
        "summary": f"The {template_id} template.",
        "when_to_use": "When it fits.",
        "when_not_to_use": "When it doesn't.",
        "runtime": "node",
        "framework": template_id,
        "requires": [],
        "surfaces": ["cli", "workspaces"],
        "default": False,
        "digest": "sha256:" + "0" * 64,
        "files": [
            {
                "path": path,
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for path, content in sorted(files.items())
        ],
        **metadata,
    }


def _catalog():
    return {
        "catalog_version": 1,
        "templates": [
            _template_entry(
                "streamlit",
                FILES["streamlit"],
                runtime="python",
                requires=["serverless", "python-serverless", "streamlit-serverless"],
            ),
            _template_entry("nextjs", FILES["nextjs"], default=True),
        ],
    }


@pytest.fixture(autouse=True)
def reset_metrics():
    get_cli_context_manager().metrics = CLIMetrics()


def _counters() -> dict:
    """The template telemetry counters this test's command recorded."""
    return {
        name: value
        for name, value in get_cli_context_manager().metrics.counters.items()
        if name.startswith(("snowflake_app.templates.", "snowflake_app.init."))
    }


@pytest.fixture
def github(httpserver):
    """Point the templates module at *httpserver*, serving GitHub's layout."""
    base = httpserver.url_for("").rstrip("/")
    with patch.object(templates, "GITHUB_URL", base), patch.object(
        templates, "GITHUB_RAW_URL", base
    ):
        yield httpserver


def _pkt_line(data: bytes) -> bytes:
    return f"{len(data) + 4:04x}".encode() + data


def _advertisement(*, tag: str = "templates-v1", annotated: bool = False) -> bytes:
    """A git smart-HTTP ref advertisement, as GitHub sends it.

    *tag* points at COMMIT, through a tag object when *annotated*.
    """
    tag_lines = (
        [f"{TAG_OBJECT} refs/tags/{tag}\n", f"{COMMIT} refs/tags/{tag}^{{}}\n"]
        if annotated
        else [f"{COMMIT} refs/tags/{tag}\n"]
    )
    return (
        _pkt_line(b"# service=git-upload-pack\n")
        + b"0000"
        + _pkt_line(
            f"{MAIN_COMMIT} HEAD\0multi_ack side-band-64k agent=git/2\n".encode()
        )
        + _pkt_line(f"{'f' * 40} refs/heads/feature\n".encode())
        + _pkt_line(f"{MAIN_COMMIT} refs/heads/main\n".encode())
        + b"".join(_pkt_line(line.encode()) for line in tag_lines)
        + b"0000"
    )


def _expect_refs(httpserver):
    return httpserver.expect_request(
        "/snowflakedb/sar-sdk.git/info/refs",
        query_string="service=git-upload-pack",
    )


def _serve(httpserver, catalog=None, files=None, advertisement=None):
    _expect_refs(httpserver).respond_with_data(advertisement or _advertisement())
    httpserver.expect_request(
        f"{REPOSITORY_PATH}/templates/catalog.json"
    ).respond_with_data(json.dumps(catalog or _catalog()))
    for template_id, template_files in (files or FILES).items():
        for path, content in template_files.items():
            httpserver.expect_request(
                f"{REPOSITORY_PATH}/templates/{template_id}/{path}"
            ).respond_with_data(content)


def _flat(output: str) -> str:
    """*output* without the error box's borders and line breaks."""
    return " ".join(output.replace("|", " ").split())


def _template(template_id="nextjs", **metadata) -> Template:
    return Template.model_validate(
        _template_entry(template_id, FILES[template_id], **metadata)
    )


class TestTemplateRepository:
    def test_load_reads_the_catalog_at_the_release_tag(self, github):
        _serve(github)
        with TemplateRepository() as repository:
            source = repository.load()
        assert source.ref == "templates-v1"
        assert source.commit == COMMIT
        assert [template.id for template in source.templates] == [
            "nextjs",
            "streamlit",
        ]
        assert _counters() == {"snowflake_app.templates.result.ok": 1}

    def test_annotated_release_tag_resolves_to_its_commit(self, github):
        _serve(github, advertisement=_advertisement(annotated=True))
        with TemplateRepository() as repository:
            assert repository.load().commit == COMMIT

    def test_ref_reads_a_branch(self, github):
        github.expect_request(
            f"/snowflakedb/sar-sdk/{MAIN_COMMIT}/templates/catalog.json"
        ).respond_with_data(json.dumps(_catalog()))
        _serve(github)
        with TemplateRepository("main") as repository:
            source = repository.load()
        assert (source.ref, source.commit) == ("main", MAIN_COMMIT)
        assert _counters() == {
            "snowflake_app.templates.result.ok": 1,
            "snowflake_app.templates.custom_ref": 1,
        }

    def test_ref_with_a_commit_skips_the_ref_lookup(self, github):
        _serve(github)
        with TemplateRepository(COMMIT) as repository:
            assert repository.load().commit == COMMIT
        assert all(not request.path.endswith("/info/refs") for request, _ in github.log)

    def test_unknown_ref_is_named(self, github):
        _serve(github)
        with TemplateRepository("v9") as repository:
            with pytest.raises(TemplateSourceError, match="no tag or branch named v9"):
                repository.load()
        assert _counters() == {
            "snowflake_app.templates.custom_ref": 1,
            "snowflake_app.templates.result.unknown_ref": 1,
            "snowflake_app.templates.failed_request.commit": 1,
        }

    def test_missing_release_tag_suggests_main(self, github):
        _serve(github, advertisement=_advertisement(tag="templates-v2"))
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError, match="pass --ref main"):
                repository.load()
        assert _counters()["snowflake_app.templates.result.unknown_ref"] == 1

    @pytest.mark.parametrize("ref", ["", "-main", "a..b", "a//b", "a b", "x.lock"])
    def test_invalid_ref_is_rejected_before_any_request(self, github, ref):
        with pytest.raises(CliError, match="isn't a tag, a branch, or a full"):
            TemplateRepository(ref)
        assert github.log == []

    def test_download_writes_every_file(self, github, tmp_path):
        _serve(github)
        with TemplateRepository() as repository:
            source = repository.load()
            repository.download(source, source.get("nextjs"), SecurePath(tmp_path))
        for path, content in FILES["nextjs"].items():
            assert (tmp_path / path).read_bytes() == content
        assert _counters() == {
            "snowflake_app.templates.result.ok": 1,
            "snowflake_app.init.template.nextjs": 1,
        }

    def test_failed_file_download_writes_nothing(self, github, tmp_path):
        files = {**FILES, "nextjs": dict(FILES["nextjs"])}
        del files["nextjs"]["package.json"]
        _serve(github, files=files)
        github.expect_request(
            f"{REPOSITORY_PATH}/templates/nextjs/package.json"
        ).respond_with_data("unavailable", status=503)
        with TemplateRepository() as repository:
            source = repository.load()
            with pytest.raises(TemplateSourceError, match="HTTP 503"):
                repository.download(source, source.get("nextjs"), SecurePath(tmp_path))
        assert list(tmp_path.iterdir()) == []
        assert _counters() == {
            "snowflake_app.templates.result.ok": 0,
            "snowflake_app.templates.result.http": 1,
            "snowflake_app.templates.failed_request.file": 1,
            "snowflake_app.templates.http_status": 503,
            "snowflake_app.init.template.nextjs": 1,
        }

    def test_download_rejects_a_file_that_does_not_match_the_catalog(
        self, github, tmp_path
    ):
        tampered = {**FILES, "nextjs": {**FILES["nextjs"], "package.json": b"{}\n"}}
        _serve(github, files=tampered)
        with TemplateRepository() as repository:
            source = repository.load()
            with pytest.raises(TemplateSourceError, match="package.json"):
                repository.download(source, source.get("nextjs"), SecurePath(tmp_path))
        assert list(tmp_path.iterdir()) == []
        assert _counters()["snowflake_app.templates.result.integrity"] == 1
        assert _counters()["snowflake_app.templates.result.ok"] == 0

    def test_missing_catalog_names_the_url_and_status(self, github):
        _expect_refs(github).respond_with_data(_advertisement())
        github.expect_request(
            f"{REPOSITORY_PATH}/templates/catalog.json"
        ).respond_with_data("Not Found", status=404)
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError) as error:
                repository.load()
        assert "templates/catalog.json returned HTTP 404" in error.value.message
        assert _counters() == {
            "snowflake_app.templates.result.http": 1,
            "snowflake_app.templates.failed_request.catalog": 1,
            "snowflake_app.templates.http_status": 404,
        }

    @pytest.mark.parametrize(
        "status, headers",
        [(429, {}), (403, {"X-RateLimit-Remaining": "0"})],
    )
    def test_exhausted_rate_limit_is_reported(self, github, status, headers):
        _expect_refs(github).respond_with_data(
            "rate limited", status=status, headers=headers
        )
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError, match="rate limit"):
                repository.load()
        assert _counters() == {
            "snowflake_app.templates.result.rate_limited": 1,
            "snowflake_app.templates.failed_request.commit": 1,
            "snowflake_app.templates.http_status": status,
        }

    @pytest.mark.parametrize("body", [b"<html>sign in</html>", b"00ff", b""])
    def test_malformed_ref_list_is_rejected(self, github, body):
        _expect_refs(github).respond_with_data(body)
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError, match="tags and branches"):
                repository.load()
        assert _counters() == {
            "snowflake_app.templates.result.invalid_response": 1,
            "snowflake_app.templates.failed_request.commit": 1,
        }

    def test_newer_catalog_version_asks_for_an_upgrade(self, github):
        _serve(github, catalog={**_catalog(), "catalog_version": 2})
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError, match="Upgrade Snowflake CLI"):
                repository.load()
        assert _counters()["snowflake_app.templates.result.unsupported_catalog"] == 1

    @pytest.mark.parametrize(
        "path", ["../outside", "/etc/passwd", "a//b", "a\\b", "C:/x", "./a"]
    )
    def test_catalog_with_an_unsafe_file_path_is_rejected(self, github, path):
        catalog = _catalog()
        catalog["templates"][1]["files"][0]["path"] = path
        _serve(github, catalog=catalog)
        with TemplateRepository() as repository:
            with pytest.raises(TemplateSourceError, match="valid template catalog"):
                repository.load()

    def test_catalog_larger_than_the_limit_is_rejected(self, github):
        _serve(github)
        with patch.object(templates, "MAX_CATALOG_BYTES", 10):
            with TemplateRepository() as repository:
                with pytest.raises(TemplateSourceError, match="more than 10 bytes"):
                    repository.load()
        assert _counters() == {
            "snowflake_app.templates.result.too_large": 1,
            "snowflake_app.templates.failed_request.catalog": 1,
        }

    def test_air_gapped_never_contacts_github(self, github):
        with patch.object(templates, "is_air_gapped", return_value=True):
            with TemplateRepository() as repository:
                with pytest.raises(TemplateSourceError, match="cli.air_gapped"):
                    repository.load()
        assert _counters() == {"snowflake_app.templates.result.air_gapped": 1}
        assert github.log == []

    def test_unreachable_host_is_a_connection_failure(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with patch.object(templates, "GITHUB_URL", f"http://127.0.0.1:{port}"):
            with TemplateRepository() as repository:
                with pytest.raises(TemplateSourceError, match="Could not reach"):
                    repository.load()
        assert _counters() == {
            "snowflake_app.templates.result.connection": 1,
            "snowflake_app.templates.failed_request.commit": 1,
        }


def _connection_error_caused_by(cause: BaseException) -> requests.ConnectionError:
    error = requests.ConnectionError("failed")
    error.__cause__ = cause
    return error


@pytest.mark.parametrize(
    "error, expected",
    [
        (requests.exceptions.ProxyError("proxy"), "proxy"),
        (requests.exceptions.SSLError("certificate verify failed"), "tls"),
        (requests.exceptions.ConnectTimeout("timed out"), "timeout"),
        (requests.exceptions.ReadTimeout("timed out"), "timeout"),
        (
            _connection_error_caused_by(socket.gaierror(8, "nodename nor servname")),
            "dns",
        ),
        (requests.ConnectionError("refused"), "connection"),
    ],
)
def test_network_failures_are_classified(error, expected):
    assert _network_failure(error) == expected


class TestTemplateSource:
    def _source(self):
        return templates.TemplateSource(
            ref="templates-v1",
            commit=COMMIT,
            catalog=templates.Catalog.model_validate(_catalog()),
        )

    def test_no_template_id_means_the_default(self):
        assert self._source().get(None).id == "nextjs"

    def test_unknown_template_lists_the_available_ones(self):
        with pytest.raises(CliError, match="Available templates: nextjs, streamlit"):
            self._source().get("vue")


class TestRenderAppYml:
    def _render(self, text=NEXTJS_APP_YML.decode(), **overrides):
        arguments = dict(
            template=_template(),
            ref="templates-v1",
            commit=COMMIT,
            name="MY_APP",
            label="My App",
            database="USER$",
            schema="PUBLIC",
            warehouse="COMPUTE_WH",
            build_eai=None,
        )
        arguments.update(overrides)
        return render_app_yml(text, **arguments)

    def test_placeholders_are_filled_in(self):
        rendered = yaml.safe_load(self._render())
        definition = AppYmlDefinition(**rendered)
        assert definition.name == "MY_APP"
        assert definition.database == "USER$"
        assert definition.schema_ == "PUBLIC"
        assert definition.query_warehouse == "COMPUTE_WH"
        assert definition.label == "My App"

    def test_first_line_records_the_template_and_commit(self):
        first_line = self._render().splitlines()[0]
        assert first_line == (
            f"# created-by: snow-app-init; template=nextjs; ref=templates-v1; "
            f"commit={COMMIT}"
        )

    def test_provenance_is_added_when_the_template_has_none(self):
        rendered = self._render(text="version: 2\nname: <%APP_NAME%>\n")
        assert rendered.splitlines()[0].startswith("# created-by: snow-app-init;")
        assert rendered.splitlines()[1] == "version: 2"

    def test_build_eai_follows_query_warehouse(self):
        lines = self._render(build_eai="MY_EAI").splitlines()
        index = lines.index("query_warehouse: COMPUTE_WH")
        assert lines[index + 1] == "build_eai: MY_EAI"

    def test_label_with_an_apostrophe_stays_one_quoted_string(self):
        rendered = yaml.safe_load(self._render(label="Guy's App"))
        assert rendered["label"] == "Guy's App"

    def test_database_with_double_quotes_is_kept_literally(self):
        rendered = yaml.safe_load(self._render(database='"lower_db"'))
        assert rendered["database"] == '"lower_db"'

    def test_unknown_placeholder_asks_for_an_upgrade(self):
        with pytest.raises(CliError, match="<%REGION%>"):
            self._render(text=NEXTJS_APP_YML.decode() + "region: <%REGION%>\n")


@pytest.mark.parametrize(
    "name, label",
    [
        ("sales-dashboard", "Sales Dashboard"),
        ("my_cool_app", "My Cool App"),
        ("myCoolApp", "My Cool App"),
        ("SALES_APP", "SALES APP"),
    ],
)
def test_readable_label(name, label):
    assert readable_label(name) == label


def test_commands_run_with_the_feature_flag_off(github, runner):
    _serve(github)
    with with_feature_flags({SnowflakeAppFeatureFlag.ENABLE_APP_TEMPLATES: False}):
        result = runner.invoke(["app", "templates"])
    assert result.exit_code == 0, result.output
    assert "nextjs" in result.output


class TestTemplatesCommand:
    def test_lists_the_default_template_first(self, github, runner):
        _serve(github)
        result = runner.invoke(["app", "templates"])
        assert result.exit_code == 0, result.output
        assert result.output.index("nextjs") < result.output.index("streamlit")

    def test_json_describes_each_template_without_its_files(self, github, runner):
        _serve(github)
        result = runner.invoke(["app", "templates", "--format", "json"])
        assert result.exit_code == 0, result.output
        listed = json.loads(result.output)
        assert [template["id"] for template in listed] == ["nextjs", "streamlit"]
        assert listed[1]["requires"] == [
            "serverless",
            "python-serverless",
            "streamlit-serverless",
        ]
        assert "files" not in listed[0]
        assert listed[0]["when_to_use"] == "When it fits."

    def test_unreachable_github_fails_with_the_url(self, github, runner):
        _expect_refs(github).respond_with_data("unavailable", status=503)
        result = runner.invoke(["app", "templates"])
        assert result.exit_code == 1
        assert "returned HTTP 503" in _flat(result.output)


@pytest.fixture
def manager():
    with patch(f"{COMMANDS}.SnowflakeAppManager") as manager_class, patch(
        f"{COMMANDS}._app_service_exists", return_value=False
    ):
        instance = manager_class.return_value
        instance.fetch_app_service_defaults.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }
        instance.fetch_account_capabilities.return_value = {
            "serverless": True,
            "python": True,
            "python-serverless": True,
            "streamlit-serverless": True,
        }
        yield instance


class TestInitCommand:
    def test_creates_the_project_from_the_default_template(
        self, github, runner, manager, tmp_path
    ):
        _serve(github)
        project = tmp_path / "sales-dashboard"
        result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 0, result.output
        assert "from the nextjs template" in result.output
        for path, content in FILES["nextjs"].items():
            if path != "app.yml":
                assert (project / path).read_bytes() == content
        app_yml = (project / "app.yml").read_text()
        assert app_yml.splitlines()[0].endswith(
            f"template=nextjs; ref=templates-v1; commit={COMMIT}"
        )
        definition = AppYmlDefinition(**yaml.safe_load(app_yml))
        assert definition.name == "SALES_DASHBOARD"
        assert definition.database == "PARAM_DB"
        assert definition.schema_ == "PARAM_SCHEMA"
        assert definition.query_warehouse == "PARAM_WH"
        assert definition.label == "Sales Dashboard"
        assert _counters() == {
            "snowflake_app.templates.result.ok": 1,
            "snowflake_app.init.template.nextjs": 1,
        }

    def test_json_reports_the_project(self, github, runner, manager, tmp_path):
        _serve(github)
        project = tmp_path / "my-app"
        result = runner.invoke(
            [
                "app",
                "init",
                str(project),
                "--template",
                "streamlit",
                "--app-name",
                "st_app",
                "--format",
                "json",
            ]
        )
        assert result.exit_code == 0, result.output
        report = json.loads(result.output)
        assert report["template"] == "streamlit"
        assert report["commit"] == COMMIT
        assert report["name"] == "ST_APP"
        assert report["database"] == "PARAM_DB"
        assert report["missing_capabilities"] == []
        assert (project / "streamlit_app.py").exists()

    def test_ref_is_recorded_in_the_project(self, github, runner, manager, tmp_path):
        _serve(github)
        project = tmp_path / "app"
        result = runner.invoke(
            ["app", "init", str(project), "--ref", COMMIT, "--format", "json"]
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["ref"] == COMMIT
        first_line = (project / "app.yml").read_text().splitlines()[0]
        assert first_line.endswith(f"ref={COMMIT}; commit={COMMIT}")

    def test_build_eai_from_the_account_is_written(
        self, github, runner, manager, tmp_path
    ):
        _serve(github)
        manager.fetch_app_service_defaults.return_value["build_eai"] = "BUILD_EAI"
        project = tmp_path / "app"
        result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 0, result.output
        assert yaml.safe_load((project / "app.yml").read_text())["build_eai"] == (
            "BUILD_EAI"
        )

    def test_connects_before_reading_the_templates(
        self, github, runner, manager, tmp_path
    ):
        _expect_refs(github).respond_with_data("unavailable", status=503)
        result = runner.invoke(["app", "init", str(tmp_path / "app")])
        assert result.exit_code == 1
        manager.fetch_app_service_defaults.assert_called_once()

    def test_invalid_ref_fails_before_connecting(
        self, github, runner, manager, tmp_path
    ):
        result = runner.invoke(["app", "init", str(tmp_path / "app"), "--ref", "a..b"])
        assert result.exit_code == 1
        manager.fetch_app_service_defaults.assert_not_called()
        assert github.log == []

    def test_empty_directory_is_used(self, github, runner, manager, tmp_path):
        _serve(github)
        project = tmp_path / "empty"
        project.mkdir()
        result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 0, result.output
        assert (project / "app.yml").exists()

    def test_non_empty_directory_is_left_alone(self, github, runner, manager, tmp_path):
        project = tmp_path / "taken"
        project.mkdir()
        (project / "notes.txt").write_text("keep me")
        result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 1
        assert "already exists and is not an empty directory" in _flat(result.output)
        assert [path.name for path in project.iterdir()] == ["notes.txt"]
        assert github.log == []

    def test_unknown_template_creates_nothing(self, github, runner, manager, tmp_path):
        _serve(github)
        project = tmp_path / "app"
        result = runner.invoke(["app", "init", str(project), "--template", "vue"])
        assert result.exit_code == 1
        assert "Unknown template 'vue'" in _flat(result.output)
        assert not project.exists()

    def test_tampered_file_creates_nothing(self, github, runner, manager, tmp_path):
        tampered = {**FILES, "nextjs": {**FILES["nextjs"], "app.yml": b"name: x\n"}}
        _serve(github, files=tampered)
        project = tmp_path / "app"
        result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 1
        assert "doesn't match the template catalog" in _flat(result.output)
        assert not project.exists()

    def test_missing_capabilities_are_a_warning(
        self, github, runner, manager, tmp_path
    ):
        _serve(github)
        manager.fetch_account_capabilities.return_value["streamlit-serverless"] = False
        project = tmp_path / "st"
        result = runner.invoke(["app", "init", str(project), "--template", "streamlit"])
        assert result.exit_code == 0, result.output
        assert "needs streamlit-serverless" in _flat(result.output)
        assert (project / "app.yml").exists()

    def test_existing_app_service_is_a_warning(self, github, runner, manager, tmp_path):
        _serve(github)
        project = tmp_path / "app"
        with patch(f"{COMMANDS}._app_service_exists", return_value=True):
            result = runner.invoke(["app", "init", str(project)])
        assert result.exit_code == 0, result.output
        assert "PARAM_DB.PARAM_SCHEMA.APP already exists" in _flat(result.output)
