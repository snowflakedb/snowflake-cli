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

import copy
from typing import Any, Optional

from jinja2 import Environment, TemplateSyntaxError, nodes
from packaging.version import Version
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CycleDetectedError, InvalidTemplate
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectProperties,
    build_project_definition,
)
from snowflake.cli.api.project.schemas.updatable_model import context
from snowflake.cli.api.rendering.jinja import CONTEXT_KEY, FUNCTION_KEY
from snowflake.cli.api.rendering.project_definition_templates import (
    get_client_side_jinja_env,
)
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts, traverse
from snowflake.cli.api.utils.graph import Graph, Node
from snowflake.cli.api.utils.models import ProjectEnvironment
from snowflake.cli.api.utils.templating_functions import get_templating_functions
from snowflake.cli.api.utils.types import Context, Definition


class TemplatedEnvironment:
    """
    This class is a utility class
    that encapsulates some of the Jinja Templating functionality.
    """

    def __init__(self, env: Environment):
        self._jinja_env: Environment = env

    def render(self, template_value: Any, context: Context) -> Any:
        if not self.get_referenced_vars(template_value):
            return template_value

        jinja_template = self._jinja_env.from_string(str(template_value))
        return jinja_template.render(context)

    def get_referenced_vars(self, template_value: Any) -> set[TemplateVar]:
        template_str = str(template_value)
        try:
            ast = self._jinja_env.parse(template_str)
        except TemplateSyntaxError as e:
            raise InvalidTemplate(
                f"Error parsing template from project definition file. Value: '{template_str}'. Error: {e}"
            ) from e

        return self._get_referenced_vars(ast, template_str)

    def _get_referenced_vars(
        self,
        ast_node: nodes.Template,
        template_value: str,
        current_attr_chain: Optional[list[str]] = None,
    ) -> set[TemplateVar]:
        """
        Traverse Jinja AST to find the variable chain referenced by the template.
        A variable like ctx.env.test is internally represented in the AST tree as
        Getattr Node (attr='test') -> Getattr Node (attr='env') -> Name Node (name='ctx')
        """
        all_referenced_vars: set[TemplateVar] = set()
        if isinstance(ast_node, nodes.Getattr):
            current_attr_chain = [ast_node.attr] + (current_attr_chain or [])  # type: ignore[attr-defined]
        elif isinstance(ast_node, nodes.Name):
            current_attr_chain = [ast_node.name] + (current_attr_chain or [])  # type: ignore[attr-defined]
            all_referenced_vars.add(TemplateVar(current_attr_chain))
            current_attr_chain = None
        elif (
            not isinstance(
                ast_node,
                (
                    nodes.Template,
                    nodes.TemplateData,
                    nodes.Output,
                    nodes.Call,
                    nodes.Const,
                    nodes.Filter,
                ),
            )
            or current_attr_chain is not None
        ):
            raise InvalidTemplate(f"Unexpected template syntax in {template_value}")

        for child_node in ast_node.iter_child_nodes():
            all_referenced_vars.update(
                self._get_referenced_vars(
                    child_node, template_value, current_attr_chain
                )
            )

        return all_referenced_vars


class TemplateVar:
    """
    This class tracks template variable information.
    For a variable like ctx.env.var, this class will track
    the chain of keys referenced by this variable (ctx, env, var),
    as well as the value of this variable. (e.g. ctx.env.var = "hello_<% ctx.definition_version %>")

    The value of this variable is divided into 2 parts.
    The templated value (e.g. "hello_<% ctx.definition %>"),
    as well as the rendered_value (e.g. "hello_1.1")
    """

    def __init__(self, vars_chain):
        self._vars_chain: list[str] = list(vars_chain)
        self.templated_value: Optional[Any] = None
        self.rendered_value: Optional[Any] = None

    @property
    def key(self) -> str:
        return ".".join(self._vars_chain)

    @property
    def is_env_var(self) -> bool:
        return (
            len(self._vars_chain) == 3
            and self._vars_chain[0] == CONTEXT_KEY
            and self._vars_chain[1] == "env"
        )

    def get_env_var_name(self) -> str:
        if not self.is_env_var:
            raise KeyError(
                f"Referenced variable {self.key} is not an environment variable"
            )
        return self._vars_chain[2]

    def add_to_context(self, context: Context) -> None:
        """
        Takes a multi-level context dict as input. Modifies the context dict with the rendered value of this variable.

        If the variable has multi-levels (e.g. ctx.env), recursively traverse the dictionary
        to set the inner level's key to the rendered value of this variable.

        Example: vars chain contains ['ctx', 'env', 'x'], and context is {}, and rendered_value is 'val'.
        At the end of this call, context content will be: {'ctx': {'env': {'x': 'val'}}}
        """
        current_dict_level = context
        last_element_index = len(self._vars_chain) - 1
        for index, var in enumerate(self._vars_chain):
            if index == last_element_index:
                current_dict_level[var] = self.rendered_value
            else:
                current_dict_level.setdefault(var, {})
                current_dict_level = current_dict_level[var]

    def read_from_context(self, context: Context) -> Any:
        """
        Takes a multi-level context dict as input.

        If the variable has multi-levels (e.g. ctx.env), recursively traverse the dictionary
        to find the key that the variable points to.

        Returns the value in that location.

        Raise InvalidTemplate if the variable is None or not found.
        """
        current_dict_level = context
        for key in self._vars_chain:
            if (
                not isinstance(current_dict_level, dict)
                or key not in current_dict_level
            ):
                raise InvalidTemplate(f"Could not find template variable {self.key}")
            current_dict_level = current_dict_level[key]

        value = current_dict_level

        if value is None:
            raise InvalidTemplate(f"Template variable {self.key} does not have a value")

        if isinstance(value, (dict, list)):
            raise InvalidTemplate(
                f"Template variable {self.key} does not have a scalar value"
            )

        return value

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


def _build_dependency_graph(
    env: TemplatedEnvironment,
    all_vars: set[TemplateVar],
    context: Context,
    environment_overrides: ProjectEnvironment,
) -> Graph[TemplateVar]:
    dependencies_graph = Graph[TemplateVar]()
    for variable in all_vars:
        dependencies_graph.add(Node[TemplateVar](key=variable.key, data=variable))
    for variable in all_vars:
        # If variable is found in os.environ or from cli override, then use the value as is
        # skip rendering by pre-setting the rendered_value attribute
        if variable.is_env_var and variable.get_env_var_name() in environment_overrides:
            env_value = environment_overrides.get(variable.get_env_var_name())
            variable.rendered_value = env_value
            variable.templated_value = env_value
        else:
            variable.templated_value = variable.read_from_context(context)
            dependencies_vars = env.get_referenced_vars(variable.templated_value)

            for referenced_var in dependencies_vars:
                dependencies_graph.add_directed_edge(variable.key, referenced_var.key)

    return dependencies_graph


def _render_graph_node(env: TemplatedEnvironment, node: Node[TemplateVar]) -> None:
    if node.data.rendered_value is not None:
        # Do not re-evaluate resolved nodes like env variable nodes
        # which might contain template-like values, or non-string nodes
        return

    current_context: Context = {}
    for dep_node in node.neighbors:
        dep_node.data.add_to_context(current_context)

    node.data.rendered_value = env.render(node.data.templated_value, current_context)


def _validate_env_section(env_section: dict):
    if not isinstance(env_section, dict):
        raise InvalidTemplate(
            "env section in project definition file should be a mapping"
        )
    for variable, value in env_section.items():
        if value is None or isinstance(value, (dict, list)):
            raise InvalidTemplate(
                f"Variable {variable} in env section of project definition file should be a scalar"
            )


def _get_referenced_vars_in_definition(
    template_env: TemplatedEnvironment, definition: Definition
):
    referenced_vars = set()

    def find_any_template_vars(element):
        referenced_vars.update(template_env.get_referenced_vars(element))

    traverse(definition, visit_action=find_any_template_vars)

    return referenced_vars


def _template_version_warning():
    cc.warning(
        "Ignoring template pattern in project definition file. "
        "Update 'definition_version' to 1.1 or later in snowflake.yml to enable template expansion."
    )


def _add_defaults_to_definition(original_definition: Definition) -> Definition:
    with context({"skip_validation_on_templates": True}):
        # pass a flag to Pydantic to skip validation for templated scalars
        # populate the defaults
        project_definition = build_project_definition(**original_definition)

    definition_with_defaults = project_definition.model_dump(
        exclude_none=True, warnings=False, by_alias=True
    )
    # The main purpose of the above operation was to populate defaults from Pydantic.
    # By merging the original definition back in, we ensure that any transformations
    # that Pydantic would have performed are undone.
    deep_merge_dicts(definition_with_defaults, original_definition)
    return definition_with_defaults


def render_definition_template(
    original_definition: Optional[Definition], context_overrides: Context
) -> ProjectProperties:
    """
    Takes a definition file as input. An arbitrary structure containing dict|list|scalars,
    with the top level being a dictionary.
    Requires item 'definition_version' to be set to a version of 1.1 and higher.

    Searches for any templating in all of the scalar fields, and attempts to resolve them
    from the definition structure itself or from the environment variable.

    Environment variables take precedence during the rendering process.
    """

    # copy input to protect it from update
    definition = copy.deepcopy(original_definition)

    # collect all the override --env variables passed through CLI input
    override_env = context_overrides.get(CONTEXT_KEY, {}).get("env", {})

    # set up Project Environment with empty default_env because
    # default env section from project definition file is still templated at this time
    environment_overrides = ProjectEnvironment(
        default_env={}, override_env=override_env
    )

    if definition is None:
        return ProjectProperties(None, {CONTEXT_KEY: {"env": environment_overrides}})

    template_env = TemplatedEnvironment(get_client_side_jinja_env())

    if "definition_version" not in definition or Version(
        definition["definition_version"]
    ) < Version("1.1"):
        try:
            referenced_vars = _get_referenced_vars_in_definition(
                template_env, definition
            )
            if referenced_vars:
                _template_version_warning()
        except Exception:
            # also warn on Exception, as it means the user is incorrectly attempting to use templating
            _template_version_warning()

        project_definition = build_project_definition(**definition)
        project_context = {CONTEXT_KEY: definition}
        project_context[CONTEXT_KEY]["env"] = environment_overrides
        return ProjectProperties(project_definition, project_context)

    definition = _add_defaults_to_definition(definition)
    project_context = {CONTEXT_KEY: definition}

    _validate_env_section(definition.get("env", {}))

    # add available templating functions
    project_context[FUNCTION_KEY] = get_templating_functions()

    referenced_vars = _get_referenced_vars_in_definition(template_env, definition)

    dependencies_graph = _build_dependency_graph(
        template_env, referenced_vars, project_context, environment_overrides
    )

    def on_cycle_action(node: Node[TemplateVar]):
        raise CycleDetectedError(f"Cycle detected in template variable {node.data.key}")

    dependencies_graph.dfs(
        visit_action=lambda node: _render_graph_node(template_env, node),
        on_cycle_action=on_cycle_action,
    )

    # now that we determined the values of all templated vars,
    # use these resolved values as a fresh context to resolve definition
    final_context: Context = {}
    for node in dependencies_graph.get_all_nodes():
        node.data.add_to_context(final_context)

    traverse(
        definition,
        update_action=lambda val: template_env.render(val, final_context),
    )

    project_definition = build_project_definition(**definition)

    # Use the values originally provided by the user as the template context
    # This intentionally doesn't reflect any field changes made by
    # validators, to minimize user surprise when templating values
    project_context[CONTEXT_KEY] = definition

    # Use `ProjectEnvironment` in project context in order to
    # handle env variables overrides from OS env and from CLI arguments.
    project_context[CONTEXT_KEY]["env"] = ProjectEnvironment(
        default_env=project_context[CONTEXT_KEY].get("env"), override_env=override_env
    )
    return ProjectProperties(project_definition, project_context)


def raw_project_properties(definition: Definition) -> ProjectProperties:
    """
    Returns the raw project definition data without any templating.
    """
    return ProjectProperties(build_project_definition(**definition), {})
