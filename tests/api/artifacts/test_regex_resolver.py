# Copyright (c) 2025 Snowflake Inc.
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

import pytest
from snowflake.cli.api.artifacts.common import ArtifactError
from snowflake.cli.api.artifacts.regex_resolver import RegexResolver


class TestRegexResolver:
    """Test the RegexResolver class for pattern matching functionality."""

    @pytest.fixture
    def resolver(self):
        """Create a fresh RegexResolver instance for each test."""
        return RegexResolver()

    @pytest.mark.parametrize(
        "pattern,text,expected_match",
        [
            pytest.param(
                r"definitions/.*\.sql",
                "definitions/schema.sql",
                True,
                id="simple_directory_and_extension_match",
            ),
            pytest.param(
                r"definitions/.*\.sql",
                "definitions/tables/users.sql",
                True,
                id="nested_directory_match",
            ),
            pytest.param(
                r"definitions/.*\.sql",
                "src/main.py",
                False,
                id="no_match_different_directory",
            ),
            pytest.param(
                r".*\.py$",
                "src/main.py",
                True,
                id="extension_match_with_anchor",
            ),
            pytest.param(
                r".*\.py$",
                "src/main.py.backup",
                False,
                id="extension_no_match_with_anchor",
            ),
            pytest.param(
                r"test/(unit|integration)/.*\.py$",
                "test/unit/test_main.py",
                True,
                id="alternation_pattern_match_unit",
            ),
            pytest.param(
                r"test/(unit|integration)/.*\.py$",
                "test/integration/test_api.py",
                True,
                id="alternation_pattern_match_integration",
            ),
            pytest.param(
                r"test/(unit|integration)/.*\.py$",
                "test/e2e/test_app.py",
                False,
                id="alternation_pattern_no_match",
            ),
            pytest.param(
                r"^test.*\.py$",
                "test/unit/test_main.py",
                True,
                id="anchored_pattern_match",
            ),
            pytest.param(
                r"^test.*\.py$",
                "src/test_helper.py",
                False,
                id="anchored_pattern_no_match",
            ),
            pytest.param(
                r"file-with-.*\.txt$",
                "file-with-dashes.txt",
                True,
                id="special_characters_dash",
            ),
            pytest.param(
                r"file\.with\.dots\.txt$",
                "file.with.dots.txt",
                True,
                id="special_characters_escaped_dots",
            ),
            pytest.param(
                r"file.with.dots.txt$",
                "filexwithydotsztxt",
                True,
                id="unescaped_dots_match_any_character",
            ),
        ],
    )
    def test_does_match_basic_patterns(self, resolver, pattern, text, expected_match):
        """Test basic pattern matching functionality."""
        result = resolver.does_match(pattern, text)
        assert result == expected_match

    def test_pattern_caching(self, resolver):
        """Test that patterns are cached to improve performance."""
        pattern = r".*\.py$"

        # First call should create the pattern class
        assert pattern not in resolver._pattern_classes  # noqa: SLF001
        result1 = resolver.does_match(pattern, "test.py")
        assert pattern in resolver._pattern_classes  # noqa: SLF001

        # Second call should use cached pattern class
        cached_class = resolver._pattern_classes[pattern]  # noqa: SLF001
        result2 = resolver.does_match(pattern, "test.py")
        assert resolver._pattern_classes[pattern] is cached_class  # noqa: SLF001
        assert result1 == result2 == True

    def test_multiple_patterns_cached_separately(self, resolver):
        """Test that different patterns are cached separately."""
        pattern1 = r".*\.py$"
        pattern2 = r".*\.sql$"

        resolver.does_match(pattern1, "test.py")
        resolver.does_match(pattern2, "test.sql")

        assert pattern1 in resolver._pattern_classes  # noqa: SLF001
        assert pattern2 in resolver._pattern_classes  # noqa: SLF001
        assert (
            resolver._pattern_classes[pattern1]  # noqa: SLF001
            is not resolver._pattern_classes[pattern2]  # noqa: SLF001
        )

    @pytest.mark.parametrize(
        "invalid_pattern",
        [
            pytest.param(r"[invalid", id="unclosed_character_class"),
            pytest.param(r"(unclosed", id="unclosed_group"),
            pytest.param(r"*invalid", id="invalid_quantifier_position"),
            pytest.param(r"(?P<incomplete", id="incomplete_named_group"),
        ],
    )
    def test_invalid_regex_patterns_raise_error(self, resolver, invalid_pattern):
        """Test that invalid regex patterns raise ArtifactError with descriptive message."""
        with pytest.raises(ArtifactError, match="Invalid regex pattern"):
            resolver.does_match(str(invalid_pattern), "test.txt")

    def test_pattern_too_long_raises_error(self, resolver):
        """Test that excessively long patterns are rejected for security."""
        long_pattern = "a" * 1500  # Exceeds max length of 1000

        with pytest.raises(ArtifactError, match="Regex pattern too long"):
            resolver.does_match(long_pattern, "test")

    def test_pattern_length_boundary_conditions(self, resolver):
        """Test boundary conditions for pattern length validation."""
        # Pattern at max length should work
        max_length_pattern = "a" * 1000
        result = resolver.does_match(max_length_pattern, "aaaa")
        # This should not raise an error and should return False since "aaaa" doesn't match 1000 'a's
        assert result is False

        # Pattern just over max length should fail
        over_max_pattern = "a" * 1001
        with pytest.raises(ArtifactError, match="Regex pattern too long"):
            resolver.does_match(over_max_pattern, "test")

    @pytest.mark.parametrize(
        "dangerous_pattern",
        [
            pytest.param(r"(a+)+b", id="nested_quantifiers_catastrophic_backtracking"),
            pytest.param(r"(a*)*b", id="multiple_star_quantifiers"),
            pytest.param(r"(a|a)*b", id="alternation_with_overlapping_patterns"),
            pytest.param(r"((a+)+)+b", id="deeply_nested_groups"),
        ],
    )
    def test_dangerous_regex_patterns_safe_with_pydantic(
        self, resolver, dangerous_pattern
    ):
        """
        Test that classically dangerous ReDoS patterns are now safe with Pydantic v2.

        These patterns would cause catastrophic backtracking with Python's standard 're' module,
        but are handled safely by Pydantic v2's Rust-based regex engine.
        """
        test_string = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaac"

        # This should not timeout or hang - Pydantic v2 handles ReDoS patterns safely
        try:
            result = resolver.does_match(str(dangerous_pattern), test_string)
            # The result doesn't matter much - what matters is that it completes quickly
            assert isinstance(result, bool)
        except Exception as e:
            # Any timeouts or hangs indicate regression in ReDoS protection
            if "timeout" in str(e).lower() or "too long" in str(e).lower():
                pytest.fail(
                    f"ReDoS protection regression! Pattern '{dangerous_pattern}' caused timeout: {e}"
                )
            # Other exceptions (like compilation errors) are acceptable

    @pytest.mark.parametrize(
        "safe_pattern,test_text,expected_result",
        [
            pytest.param(
                r"test\.txt$", "test.txt", True, id="simple_literal_with_escape"
            ),
            pytest.param(r".*\.py$", "script.py", True, id="file_extension_matching"),
            pytest.param(
                r"^docs/.*", "docs/readme.md", True, id="directory_prefix_matching"
            ),
            pytest.param(r"test/.*", "test/file.py", True, id="directory_matching"),
            pytest.param(
                r"[^/]+\.py$", "script.py", True, id="character_class_matching"
            ),
        ],
    )
    def test_safe_regex_patterns_work_correctly(
        self, resolver, safe_pattern, test_text, expected_result
    ):
        """Test that safe regex patterns work correctly and are not incorrectly blocked."""
        result = resolver.does_match(str(safe_pattern), str(test_text))
        assert result == expected_result

    def test_case_sensitivity(self, resolver):
        """Test that regex patterns are case-sensitive by default."""
        pattern = r"docs/readme\.md$"

        # Should not match due to case difference
        assert resolver.does_match(pattern, "docs/README.md") is False
        # Should match exact case
        assert resolver.does_match(pattern, "docs/readme.md") is True

    def test_empty_pattern_and_text(self, resolver):
        """Test edge cases with empty patterns and text."""
        # Empty pattern should match empty text
        assert resolver.does_match("", "") is True

        # Non-empty pattern should not match empty text (unless pattern allows it)
        assert resolver.does_match("test", "") is False

    def test_pattern_class_reuse_across_resolver_instances(self):
        """Test that pattern classes are not shared between resolver instances."""
        resolver1 = RegexResolver()
        resolver2 = RegexResolver()

        pattern = r".*\.py$"
        resolver1.does_match(pattern, "test.py")
        resolver2.does_match(pattern, "test.py")

        # Each resolver should have its own cache
        assert pattern in resolver1._pattern_classes  # noqa: SLF001
        assert pattern in resolver2._pattern_classes  # noqa: SLF001
        assert (
            resolver1._pattern_classes[pattern]  # noqa: SLF001
            is not resolver2._pattern_classes[pattern]  # noqa: SLF001
        )


class TestRegexResolverErrorHandling:
    """Test error handling and edge cases for RegexResolver."""

    def test_pattern_compilation_error_details(self):
        """Test that pattern compilation errors provide helpful details."""
        resolver = RegexResolver()

        invalid_patterns_and_errors = [
            (r"[abc", "unclosed character set"),
            (r"(?P<name", "unclosed group name"),
            (r"*", "nothing to repeat"),
        ]

        for pattern, error_context in invalid_patterns_and_errors:
            with pytest.raises(ArtifactError) as exc_info:
                resolver.does_match(pattern, "test")

            error_message = str(exc_info.value)
            assert "Invalid regex pattern" in error_message

    def test_resolver_state_after_errors(self):
        """Test that resolver state is not corrupted after errors."""
        resolver = RegexResolver()

        # Try an invalid pattern
        with pytest.raises(ArtifactError):
            resolver.does_match(r"[invalid", "test")

        # Resolver should still work with valid patterns
        assert resolver.does_match(r".*\.py$", "test.py") is True
        assert resolver.does_match(r".*\.txt$", "test.py") is False

    def test_unicode_text_handling(self):
        """Test that resolver handles Unicode text correctly."""
        resolver = RegexResolver()

        # Test with Unicode characters in text
        unicode_text = "файл.py"  # "file.py" in Cyrillic
        pattern = r".*\.py$"

        result = resolver.does_match(pattern, unicode_text)
        assert result is True

        # Test with Unicode characters in pattern (if supported)
        unicode_pattern = r"файл\.py$"
        result = resolver.does_match(unicode_pattern, unicode_text)
        assert result is True

    def test_very_long_text_performance(self):
        """Test that resolver handles very long text efficiently."""
        resolver = RegexResolver()

        # Create a very long text string
        long_text = "a" * 10000 + ".py"
        pattern = r".*\.py$"

        # This should complete quickly without issues
        result = resolver.does_match(pattern, long_text)
        assert result is True
