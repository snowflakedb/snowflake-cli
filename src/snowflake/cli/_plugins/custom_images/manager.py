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

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from click import ClickException

_FAIL_SEVERITIES = {"high", "critical"}


def _grype_fail_table(stdout: str) -> str | None:
    """Extract High/Critical CVEs from Grype JSON as a readable table."""
    try:
        matches = json.loads(stdout).get("matches")
    except (json.JSONDecodeError, AttributeError, TypeError):
        return None
    if not isinstance(matches, list):
        return None

    rows = []
    for m in matches:
        a = m.get("artifact") or {}
        v = m.get("vulnerability") or {}
        severities = {
            str(s).strip().lower()
            for s in [
                v.get("severity"),
                *[
                    rv.get("severity")
                    for rv in (m.get("relatedVulnerabilities") or [])
                    if isinstance(rv, dict)
                ],
            ]
            if s
        }
        hit = severities & _FAIL_SEVERITIES
        if not hit:
            continue
        sev = "Critical" if "critical" in hit else "High"
        rows.append(
            (
                a.get("name", "?"),
                a.get("version", "?"),
                a.get("type", "?"),
                v.get("id", "?"),
                sev,
            )
        )

    if not rows:
        return None

    lines = [
        "High/Critical vulnerabilities (--fail-on high):",
        "",
        "package | version | type | cve | severity",
    ]
    for pkg, ver, typ, cve, sev in rows:
        lines.append(f"{pkg} | {ver} | {typ} | {cve} | {sev}")
    lines.append(
        f"\n{len(rows)} finding(s). Run `grype <image> -o json` for full details."
    )
    return "\n".join(lines)


@dataclass
class ValidationContext:
    """Context passed to all check handlers."""

    image: str
    image_info: dict


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    message: str


@dataclass
class ValidationReport:
    image_name: str
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    def add_result(self, result: ValidationResult) -> None:
        self.results.append(result)


class CustomImageManager:
    """Manager for custom image validation operations."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = self._load_config(self.config_path)
        # Config-driven checks (controlled by image_validation.yaml)
        self._check_handlers: dict[str, Any] = {
            "required_scripts": self._check_required_scripts,
            "environment_variables": self._check_environment_variables,
            "python_packages": self._check_python_packages,
            "dependency_health": self._check_dependency_health,
        }

    def _load_config(self, config_path: Path) -> dict:
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _run_docker_command(
        self, cmd: list[str], timeout: int = 120
    ) -> tuple[int, str, str]:
        """Run a docker command and return (returncode, stdout, stderr)."""
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except FileNotFoundError:
            raise ClickException("Docker is not installed.")
        except subprocess.TimeoutExpired:
            raise ClickException("Docker command timed out.")

    def _run_grype_command(self, image_name: str) -> tuple[int, str, str]:
        """Run grype vulnerability scan. --fail-on high: exit non-zero only for High/Critical CVEs."""
        cmd = ["grype", image_name, "-o", "json", "--fail-on", "high"]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except FileNotFoundError:
            raise ClickException(
                "Grype is required for vulnerability scanning. "
                "Please install it from https://github.com/anchore/grype"
            )
        except subprocess.TimeoutExpired:
            raise ClickException("Grype scan timed out.")

    def _get_image_info(self, image_name: str) -> Optional[dict]:
        """Get Docker image inspection info."""
        returncode, stdout, _stderr = self._run_docker_command(
            ["docker", "inspect", image_name]
        )
        if returncode != 0:
            return None

        try:
            info = json.loads(stdout)
            if info:
                return info[0]
        except json.JSONDecodeError:
            pass
        return None

    def validate(
        self, image: str, scan_vulnerabilities: bool = False
    ) -> tuple[ValidationReport, str]:
        """Validate a Docker image against the configured rules.

        Args:
            image: Docker image to validate. Accepts image name (e.g., 'myimage:latest')
                   or image ID/hash.
            scan_vulnerabilities: Whether to run vulnerability scan. Defaults to False.
        """
        report = ValidationReport(image_name=image)
        checks = self.config.get("checks", {})

        # Check if image exists
        image_info = self._get_image_info(image)
        if image_info is None:
            report.add_result(
                ValidationResult(
                    check_name="image_exists",
                    passed=False,
                    message=f"Image '{image}' not found. Please ensure the image exists locally.",
                )
            )
            return report, format_report(report)

        report.add_result(
            ValidationResult(
                check_name="image_exists",
                passed=True,
                message=f"Image '{image}' found",
            )
        )

        # Create context for check handlers
        context = ValidationContext(image=image, image_info=image_info)

        # Run entrypoint check FIRST (critical - stop early if fails)
        entrypoint_config = checks.get("entrypoint")
        if entrypoint_config:
            entrypoint_result = self._check_entrypoint(context, entrypoint_config)
            report.add_result(entrypoint_result)

            # Stop early if entrypoint check fails (file missing or mismatch)
            # Entrypoint is fundamental - other checks are irrelevant if it's wrong
            if not entrypoint_result.passed:
                return report, format_report(report)

        for check_name, handler in self._check_handlers.items():
            check_config = checks.get(check_name)
            if check_config is None or check_config is False:
                continue
            results = handler(context, check_config)
            if isinstance(results, list):
                for result in results:
                    report.add_result(result)
            else:
                report.add_result(results)

        # User-requested checks (controlled by CLI flags)
        if scan_vulnerabilities:
            result = self._check_vulnerabilities(context)
            report.add_result(result)

        return report, format_report(report)

    def _check_entrypoint(
        self, context: ValidationContext, expected: str
    ) -> ValidationResult:
        """Check if the image entrypoint matches the expected value and file exists."""
        # Get the actual configured entrypoint
        entrypoint = context.image_info.get("Config", {}).get("Entrypoint")
        if isinstance(entrypoint, list):
            actual = entrypoint[0] if entrypoint else None
        else:
            actual = entrypoint

        # Check if entrypoint matches expected
        if actual != expected:
            return ValidationResult(
                check_name="entrypoint",
                passed=False,
                message=f"Entrypoint mismatch. Expected: {expected}, Actual: {actual}",
            )

        # Check if the entrypoint file exists (use --entrypoint "" to bypass)
        returncode, _, _ = self._run_docker_command(
            [
                "docker",
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                "--entrypoint",
                "",
                context.image,
                "test",
                "-f",
                expected,
            ]
        )
        if returncode != 0:
            return ValidationResult(
                check_name="entrypoint",
                passed=False,
                message=f"Entrypoint file '{expected}' does not exist in the image",
            )

        return ValidationResult(
            check_name="entrypoint",
            passed=True,
            message=f"Entrypoint is correctly set to '{expected}'",
        )

    def _check_required_scripts(
        self, context: ValidationContext, scripts: list[str]
    ) -> list[ValidationResult]:
        """Check that scripts exist and are executable in the image working directory."""
        results = []
        for script in scripts:
            returncode, _, _ = self._run_docker_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--platform",
                    "linux/amd64",
                    "--entrypoint",
                    "",
                    context.image,
                    "test",
                    "-x",
                    script,
                ]
            )
            if returncode != 0:
                results.append(
                    ValidationResult(
                        check_name=f"script_{script}",
                        passed=False,
                        message=f"Script '{script}' is missing or not executable",
                    )
                )
            else:
                results.append(
                    ValidationResult(
                        check_name=f"script_{script}",
                        passed=True,
                        message=f"Script '{script}' exists and is executable",
                    )
                )
        return results

    def _check_environment_variables(
        self, context: ValidationContext, env_vars: list[dict]
    ) -> list[ValidationResult]:
        """Check if required environment variables are set."""
        results = []

        # Docker returns env vars in "KEY=VALUE" format
        env_list = context.image_info.get("Config", {}).get("Env") or []
        env_dict = {}
        for env in env_list:
            key, _, value = env.partition("=")
            env_dict[key] = value

        # Config format: [{name: "VAR_NAME", value: "VAR_VALUE"}, ...]
        for env_spec in env_vars:
            var_name = env_spec.get("name")
            expected_value = env_spec.get("value")

            if not var_name:
                continue

            if var_name not in env_dict:
                results.append(
                    ValidationResult(
                        check_name=f"env_{var_name}",
                        passed=False,
                        message=f"Environment variable '{var_name}' not found. Expected: {var_name}={expected_value}",
                    )
                )
            elif expected_value and env_dict[var_name] != expected_value:
                results.append(
                    ValidationResult(
                        check_name=f"env_{var_name}",
                        passed=False,
                        message=f"Environment variable '{var_name}' has wrong value. Expected: {expected_value}, Actual: {env_dict[var_name]}",
                    )
                )
            else:
                results.append(
                    ValidationResult(
                        check_name=f"env_{var_name}",
                        passed=True,
                        message=f"Environment variable '{var_name}' is correctly set",
                    )
                )

        return results

    def _check_python_packages(
        self, context: ValidationContext, packages: list[str]
    ) -> list[ValidationResult]:
        """Check if required Python packages are installed in the image."""
        results = []

        returncode, stdout, stderr = self._run_docker_command(
            [
                "docker",
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                context.image,
                "bash",
                "-c",
                "pip list --format json",
            ]
        )

        if returncode != 0:
            results.append(
                ValidationResult(
                    check_name="python_packages",
                    passed=False,
                    message=f"Failed to list Python packages. pip may not be installed or accessible: {stderr}",
                )
            )
            return results

        try:
            installed_packages = json.loads(stdout)
            # pip package names can use underscore or hyphen interchangeably
            # e.g., "snowflake_ml_python" and "snowflake-ml-python" are the same
            # Normalize to lowercase with hyphens for comparison
            installed_set = {
                pkg["name"].lower().replace("_", "-") for pkg in installed_packages
            }
        except json.JSONDecodeError:
            results.append(
                ValidationResult(
                    check_name="python_packages",
                    passed=False,
                    message=f"Failed to parse pip output: {stdout}",
                )
            )
            return results

        for pkg_name in packages:
            pkg_name_normalized = pkg_name.lower().replace("_", "-")

            if pkg_name_normalized in installed_set:
                results.append(
                    ValidationResult(
                        check_name=f"pkg_{pkg_name}",
                        passed=True,
                        message=f"Package '{pkg_name}' is installed",
                    )
                )
            else:
                results.append(
                    ValidationResult(
                        check_name=f"pkg_{pkg_name}",
                        passed=False,
                        message=f"Package '{pkg_name}' is not installed",
                    )
                )

        return results

    def _check_dependency_health(
        self, context: ValidationContext, config: bool | dict
    ) -> ValidationResult:
        """Run 'pip check' to verify no broken dependencies."""
        # Handle both old format (bool) and new format (dict with ignore_patterns)
        if isinstance(config, dict):
            if not config.get("enabled", True):
                return ValidationResult(
                    check_name="dependency_health",
                    passed=True,
                    message="Dependency health check skipped (disabled)",
                )
            ignore_patterns = config.get("ignore_patterns", [])
        else:
            ignore_patterns = []

        returncode, stdout, stderr = self._run_docker_command(
            [
                "docker",
                "run",
                "--rm",
                "--platform",
                "linux/amd64",
                context.image,
                "bash",
                "-c",
                "pip check",
            ]
        )

        if returncode == 0:
            return ValidationResult(
                check_name="dependency_health",
                passed=True,
                message="No broken dependencies found",
            )

        output = stdout or stderr

        # Filter out ignored patterns
        if ignore_patterns:
            lines = output.split("\n")
            filtered_lines = []
            for line in lines:
                should_ignore = any(
                    pattern.lower() in line.lower() for pattern in ignore_patterns
                )
                if not should_ignore:
                    filtered_lines.append(line)

            # If all issues are ignored, consider it passed
            remaining_issues = [l for l in filtered_lines if l.strip()]
            if not remaining_issues:
                return ValidationResult(
                    check_name="dependency_health",
                    passed=True,
                    message="No broken dependencies found",
                )
            output = "\n".join(filtered_lines)

        return ValidationResult(
            check_name="dependency_health",
            passed=False,
            message=f"Broken dependencies detected:\n{output}",
        )

    def _check_vulnerabilities(self, context: ValidationContext) -> ValidationResult:
        """Run Grype with --fail-on high; on failure, show a concise CVE table."""
        code, out, err = self._run_grype_command(context.image)
        if code == 0:
            return ValidationResult(
                check_name="vulnerability_scan",
                passed=True,
                message="No high/critical vulnerabilities found",
            )

        table = _grype_fail_table(out or "")
        msg = table or f"Grype scan failed.\n{(err or out or '')[:1500]}"
        return ValidationResult(
            check_name="vulnerability_scan",
            passed=False,
            message=msg,
        )


def format_report(report: ValidationReport) -> str:
    """Format the validation report for display."""
    lines = []
    lines.append(f"\n{'=' * 60}")
    lines.append(f"Image Validation Report: {report.image_name}")
    lines.append(f"{'=' * 60}\n")

    for result in report.results:
        status = "PASS" if result.passed else "FAIL"
        lines.append(f"[{status}] {result.check_name}: {result.message}")

    lines.append(f"\n{'-' * 60}")
    lines.append(f"Summary: {report.passed_count} passed, {report.failed_count} failed")

    if report.all_passed:
        lines.append("Status: ALL CHECKS PASSED")
    else:
        lines.append("Status: VALIDATION FAILED")

    lines.append(f"{'=' * 60}\n")

    return "\n".join(lines)
