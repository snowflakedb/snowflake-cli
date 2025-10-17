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

from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.remote.constants import ComputeResources, SnowflakeCloudType
from snowflake.cli._plugins.remote.utils import (
    ImageSpec,
    format_stage_path,
    get_current_region_id,
    get_node_resources,
    get_regions,
    parse_image_string,
)


class TestImageSpec:
    """Test ImageSpec dataclass."""

    def test_image_spec_full_name(self):
        """Test ImageSpec full_name property."""
        resources = ComputeResources(cpu=2, memory=8, gpu=0)

        spec = ImageSpec(
            repo="/snowflake/images/snowflake_images",
            image_name="st_plat/runtime/x86/runtime_image/snowbooks",
            image_tag="1.7.1",
            resource_requests=resources,
            resource_limits=resources,
        )

        expected_full_name = "/snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:1.7.1"
        assert spec.full_name == expected_full_name


class TestRegionUtils:
    """Test region-related utility functions."""

    def test_get_regions_with_region_group(self):
        """Test get_regions with region groups."""
        mock_session = Mock()
        mock_regions = [
            Mock(
                region_group="aws-us-east-1",
                snowflake_region="us-east-1",
                cloud="aws",
                region="us-east-1",
                display_name="US East (N. Virginia)",
            ),
            Mock(
                region_group="azure-west-us-2",
                snowflake_region="west-us-2",
                cloud="azure",
                region="west-us-2",
                display_name="West US 2",
            ),
        ]
        mock_session.sql.return_value.collect.return_value = mock_regions

        with patch("snowflake.cli.api.console.cli_console") as mock_console:
            regions = get_regions(mock_session)

            assert "aws-us-east-1.us-east-1" in regions
            assert "azure-west-us-2.west-us-2" in regions

            aws_region = regions["aws-us-east-1.us-east-1"]
            assert aws_region["region_group"] == "aws-us-east-1"
            assert aws_region["snowflake_region"] == "us-east-1"
            assert aws_region["cloud"] == SnowflakeCloudType.AWS
            assert aws_region["region"] == "us-east-1"
            assert aws_region["display_name"] == "US East (N. Virginia)"

    def test_get_regions_without_region_group(self):
        """Test get_regions without region groups."""
        mock_session = Mock()
        mock_regions = [
            Mock(
                region_group=None,
                snowflake_region="us-central1",
                cloud="gcp",
                region="us-central1",
                display_name="US Central 1",
            )
        ]
        # Mock hasattr to return False for region_group
        for region in mock_regions:
            region.region_group = None

        mock_session.sql.return_value.collect.return_value = mock_regions

        with patch("snowflake.cli.api.console.cli_console") as mock_console:
            regions = get_regions(mock_session)

            assert "us-central1" in regions

            gcp_region = regions["us-central1"]
            assert gcp_region["region_group"] is None
            assert gcp_region["snowflake_region"] == "us-central1"
            assert gcp_region["cloud"] == SnowflakeCloudType.GCP
            assert gcp_region["region"] == "us-central1"
            assert gcp_region["display_name"] == "US Central 1"

    def test_get_current_region_id(self):
        """Test get_current_region_id function."""
        mock_session = Mock()
        mock_result = Mock()
        mock_result.CURRENT_REGION = "us-east-1"
        mock_session.sql.return_value.collect.return_value = [mock_result]

        region_id = get_current_region_id(mock_session)

        assert region_id == "us-east-1"
        mock_session.sql.assert_called_once_with(
            "SELECT CURRENT_REGION() AS CURRENT_REGION"
        )


class TestNodeResources:
    """Test get_node_resources function."""

    def test_get_node_resources_common_instance_family(self):
        """Test get_node_resources with common instance family."""
        mock_session = Mock()

        # Mock compute pool query
        mock_pool_result = Mock()
        mock_pool_result.__getitem__ = (
            lambda self, key: "CPU_X64_S" if key == "instance_family" else None
        )
        mock_session.sql.return_value.collect.return_value = [mock_pool_result]

        with patch(
            "snowflake.cli._plugins.remote.utils.get_regions"
        ) as mock_get_regions:
            with patch(
                "snowflake.cli._plugins.remote.utils.get_current_region_id"
            ) as mock_get_region_id:
                with patch("snowflake.cli.api.console.cli_console") as mock_console:
                    # Mock region data
                    mock_get_region_id.return_value = "us-east-1"
                    mock_get_regions.return_value = {
                        "us-east-1": {"cloud": SnowflakeCloudType.AWS}
                    }

                    resources = get_node_resources(mock_session, "test_pool")

                    # Should return common instance family resources
                    assert isinstance(resources, ComputeResources)
                    assert resources.cpu > 0
                    assert resources.memory > 0

    def test_get_node_resources_cloud_specific_instance_family(self):
        """Test get_node_resources with cloud-specific instance family."""
        mock_session = Mock()

        # Mock compute pool query with cloud-specific instance family
        mock_pool_result = Mock()
        mock_pool_result.__getitem__ = (
            lambda self, key: "GPU_NV_S" if key == "instance_family" else None
        )
        mock_session.sql.return_value.collect.return_value = [mock_pool_result]

        with patch(
            "snowflake.cli._plugins.remote.utils.get_regions"
        ) as mock_get_regions:
            with patch(
                "snowflake.cli._plugins.remote.utils.get_current_region_id"
            ) as mock_get_region_id:
                with patch("snowflake.cli.api.console.cli_console") as mock_console:
                    # Mock region data
                    mock_get_region_id.return_value = "us-east-1"
                    mock_get_regions.return_value = {
                        "us-east-1": {"cloud": SnowflakeCloudType.AWS}
                    }

                    resources = get_node_resources(mock_session, "gpu_pool")

                    # Should return cloud-specific instance family resources
                    assert isinstance(resources, ComputeResources)
                    assert resources.gpu > 0  # GPU instance should have GPU resources

    def test_get_node_resources_compute_pool_not_found(self):
        """Test get_node_resources when compute pool is not found."""
        mock_session = Mock()
        mock_session.sql.return_value.collect.return_value = []  # Empty result

        with pytest.raises(
            ValueError, match="Compute pool 'nonexistent_pool' not found"
        ):
            get_node_resources(mock_session, "nonexistent_pool")


class TestStagePathUtils:
    """Test stage path validation and formatting utilities."""

    def test_format_stage_path_comprehensive(self):
        """Test format_stage_path with all types of valid inputs."""
        test_cases = [
            # @ prefixed paths (should pass through unchanged)
            ("@my_stage", "@my_stage"),
            ("@db.schema.stage", "@db.schema.stage"),
            ("@stage_with_underscores", "@stage_with_underscores"),
            ("@stage-with-dashes", "@stage-with-dashes"),
            ("@stage123", "@stage123"),
            ("@UPPERCASE_STAGE", "@UPPERCASE_STAGE"),
            ('@"stage with spaces"', '@"stage with spaces"'),
            ('@"stage-with-special@chars"', '@"stage-with-special@chars"'),
            ('@db."schema with spaces".stage', '@db."schema with spaces".stage'),
            ('@"database"."schema"."stage name"', '@"database"."schema"."stage name"'),
            # SnowURL paths (should pass through unchanged)
            ("snow://streamlit/my_stage", "snow://streamlit/my_stage"),
            ("snow://notebook/db.schema.stage", "snow://notebook/db.schema.stage"),
            (
                "snow://streamlit/stage_name/path/to/file",
                "snow://streamlit/stage_name/path/to/file",
            ),
            ("snow://streamlit", "snow://streamlit"),
            (
                "snow://any-resource-type/any.stage.name",
                "snow://any-resource-type/any.stage.name",
            ),
            # Paths without prefix (should get @ added)
            ("my_stage", "@my_stage"),
            ("db.schema.stage", "@db.schema.stage"),
            ("stage_name", "@stage_name"),
            ("UPPERCASE", "@UPPERCASE"),
            ('"stage with spaces"', '@"stage with spaces"'),
            # Trailing slash removal
            ("@my_stage/", "@my_stage"),
            ("@my_stage//", "@my_stage"),
            ("my_stage/", "@my_stage"),
            ("@db.schema.stage/", "@db.schema.stage"),
            ("snow://streamlit/my_stage/", "snow://streamlit/my_stage"),
            ("snow://notebook/db.schema.stage//", "snow://notebook/db.schema.stage"),
            (
                "snow://notebook/db.schema.stage/nested/path/",
                "snow://notebook/db.schema.stage/nested/path",
            ),
        ]

        for input_path, expected in test_cases:
            assert format_stage_path(input_path) == expected

    def test_format_stage_path_invalid_paths(self):
        """Test format_stage_path raises ValueError for invalid paths."""
        invalid_paths = [
            "",  # Empty
            "   ",  # Whitespace only
            "@",  # Just @ symbol
            "@ ",  # @ with trailing space (becomes @ after strip)
            "@  ",  # @ with multiple trailing spaces (becomes @ after strip)
            " @ ",  # @ with leading and trailing spaces (becomes @ after strip)
            "snow://",  # Just snow:// prefix - should raise error
            "snow:// ",  # snow:// with space - should raise error
        ]

        for path in invalid_paths:
            with pytest.raises(ValueError, match="Invalid|cannot be empty|missing"):
                format_stage_path(path)


class TestImageParsing:
    """Test image string parsing functionality."""

    def test_parse_image_string_just_tag(self):
        """Test parsing a string that's just a tag."""
        repo, image_name, tag = parse_image_string("1.7.1")
        assert repo == ""
        assert image_name == ""
        assert tag == "1.7.1"

    def test_parse_image_string_image_with_tag(self):
        """Test parsing image:tag format."""
        repo, image_name, tag = parse_image_string("myimage:latest")
        assert repo == ""
        assert image_name == "myimage"
        assert tag == "latest"

    def test_parse_image_string_repo_image_tag(self):
        """Test parsing repo/image:tag format."""
        repo, image_name, tag = parse_image_string("myrepo/myimage:v1.0")
        assert repo == "myrepo"
        assert image_name == "myimage"
        assert tag == "v1.0"

    def test_parse_image_string_full_registry_path(self):
        """Test parsing full registry path with nested repo."""
        repo, image_name, tag = parse_image_string("registry.com/myrepo/myimage:v1.0")
        assert repo == "registry.com/myrepo"
        assert image_name == "myimage"
        assert tag == "v1.0"

    def test_parse_image_string_repo_image_no_tag(self):
        """Test parsing repo/image without tag."""
        repo, image_name, tag = parse_image_string("myrepo/myimage")
        assert repo == "myrepo"
        assert image_name == "myimage"
        assert tag == ""

    def test_parse_image_string_complex_registry(self):
        """Test parsing complex registry paths."""
        repo, image_name, tag = parse_image_string("docker.io/library/ubuntu:20.04")
        assert repo == "docker.io/library"
        assert image_name == "ubuntu"
        assert tag == "20.04"
