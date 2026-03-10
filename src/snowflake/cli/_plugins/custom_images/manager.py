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
from typing import Optional

import yaml
from click import ClickException

from snowflake.cli._plugins.custom_images.constants import (
    CPU_BASE_IMAGE_PATH,
    GPU_BASE_IMAGE_PATH,
)


CONFIG_DIR = Path(__file__).parent / "config"
GRYPE_CPU_CONFIG_PATH = CONFIG_DIR / "grype_cpu.yaml"
GRYPE_GPU_CONFIG_PATH = CONFIG_DIR / "grype_gpu.yaml"


@dataclass
class ValidationContext:
    """Context passed to all check handlers."""
    image_hash: str
    image_info: dict
    is_gpu: bool


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
        self._check_handlers = {
            "entrypoint": self._check_entrypoint,
            "environment_variables": self._check_environment_variables,
            "python_packages": self._check_python_packages,
            "dependency_health": self._check_dependency_health,
            "vulnerability_scan": self._check_vulnerabilities,
        }

    def _load_config(self, config_path: Path) -> dict:
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _run_docker_command(self, cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
        """Run a docker command and return (returncode, stdout, stderr)."""
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except FileNotFoundError:
            raise ClickException("Docker is not installed.")
        except subprocess.TimeoutExpired:
            raise ClickException("Docker command timed out.")

    def _run_grype_command(self, image_name: str, is_gpu: bool) -> tuple[int, str, str]:
        """Run grype vulnerability scan with appropriate config."""
        grype_config = GRYPE_GPU_CONFIG_PATH if is_gpu else GRYPE_CPU_CONFIG_PATH
        cmd = ["grype", image_name]
        if grype_config.exists():
            cmd.extend(["--config", str(grype_config)])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except FileNotFoundError:
            raise ClickException("Grype is not installed.")
        except subprocess.TimeoutExpired:
            raise ClickException("Grype scan timed out.")

    def _get_image_info(self, image_name: str) -> Optional[dict]:
        """Get Docker image inspection info."""
        returncode, stdout, stderr = self._run_docker_command(
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

    def _get_base_image(self, image_name: str) -> Optional[str]:
        """Try to get the base image from docker labels or history."""
        returncode, stdout, _ = self._run_docker_command(
            ["docker", "inspect", "--format",
             "{{index .Config.Labels \"org.opencontainers.image.base.name\"}}", image_name]
        )
        if returncode == 0 and stdout and stdout != "<no value>":
            return stdout

        returncode, stdout, _ = self._run_docker_command(
            ["docker", "history", "--no-trunc", "--format", "{{.CreatedBy}}", image_name]
        )
        if returncode == 0 and stdout:
            lines = stdout.strip().split("\n")
            for line in reversed(lines):
                if "FROM" in line.upper():
                    return line
        return None

    def validate(self, image_hash: str) -> tuple[ValidationReport, str]:
        """Validate a Docker image against the configured rules."""
        report = ValidationReport(image_name=image_hash)
        checks = self.config.get("checks", {})

        # Check if image exists
        image_info = self._get_image_info(image_hash)
        if image_info is None:
            report.add_result(
                ValidationResult(
                    check_name="image_exists",
                    passed=False,
                    message=f"Image '{image_hash}' not found. Please ensure the image exists locally.",
                )
            )
            return report, format_report(report)

        report.add_result(
            ValidationResult(
                check_name="image_exists",
                passed=True,
                message=f"Image '{image_hash}' found",
            )
        )

        # Get base image info - validation stops if not found
        base_image = self._get_base_image(image_hash)
        if not base_image:
            report.add_result(
                ValidationResult(
                    check_name="base_image",
                    passed=False,
                    message="Could not determine base image. Base image information not found in image metadata.",
                )
            )
            return report, format_report(report)

        # Validate base image path matches expected CPU or GPU path
        is_gpu = GPU_BASE_IMAGE_PATH in base_image
        is_cpu = CPU_BASE_IMAGE_PATH in base_image and not is_gpu

        if not is_gpu and not is_cpu:
            report.add_result(
                ValidationResult(
                    check_name="base_image",
                    passed=False,
                    message=f"Invalid base image path: {base_image}. Expected CPU path: {CPU_BASE_IMAGE_PATH} or GPU path: {GPU_BASE_IMAGE_PATH}",
                )
            )
            return report, format_report(report)

        image_type = "GPU" if is_gpu else "CPU"
        report.add_result(
            ValidationResult(
                check_name="base_image",
                passed=True,
                message=f"Base image ({image_type}): {base_image}",
            )
        )

        # Create context for check handlers
        context = ValidationContext(
            image_hash=image_hash,
            image_info=image_info,
            is_gpu=is_gpu,
        )

        # Run all configured checks
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

        return report, format_report(report)

    def _check_entrypoint(
        self, context: ValidationContext, expected: str
    ) -> ValidationResult:
        """Check if the image entrypoint matches the expected value."""
        entrypoint = context.image_info.get("Config", {}).get("Entrypoint")

        if entrypoint is None:
            return ValidationResult(
                check_name="entrypoint",
                passed=False,
                message=f"No entrypoint defined. Expected: {expected}",
            )

        actual_entrypoint = entrypoint[0] if isinstance(entrypoint, list) else entrypoint

        if actual_entrypoint == expected:
            return ValidationResult(
                check_name="entrypoint",
                passed=True,
                message=f"Entrypoint is correctly set to '{expected}'",
            )
        else:
            return ValidationResult(
                check_name="entrypoint",
                passed=False,
                message=f"Entrypoint mismatch. Expected: {expected}, Actual: {actual_entrypoint}",
            )

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
            ["docker", "run", "--rm", "--entrypoint", "pip", context.image_hash, "list", "--format", "json"]
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
        self, context: ValidationContext, _config: bool
    ) -> ValidationResult:
        """Run 'pip check' to verify no broken dependencies."""
        returncode, stdout, stderr = self._run_docker_command(
            ["docker", "run", "--rm", "--entrypoint", "pip", context.image_hash, "check"]
        )

        if returncode == 0:
            return ValidationResult(
                check_name="dependency_health",
                passed=True,
                message="No broken dependencies found",
            )
        else:
            output = stdout or stderr
            return ValidationResult(
                check_name="dependency_health",
                passed=False,
                message=f"Broken dependencies detected:\n{output}",
            )

    def _check_vulnerabilities(
        self, context: ValidationContext, _config: bool
    ) -> ValidationResult:
        """Run Grype vulnerability scan on the image."""
        returncode, stdout, stderr = self._run_grype_command(
            context.image_hash, context.is_gpu
        )

        if returncode == 0:
            return ValidationResult(
                check_name="vulnerability_scan",
                passed=True,
                message="No high/critical vulnerabilities found",
            )
        else:
            output = stdout or stderr
            return ValidationResult(
                check_name="vulnerability_scan",
                passed=False,
                message=f"High/critical vulnerabilities detected:\n{output}",
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
