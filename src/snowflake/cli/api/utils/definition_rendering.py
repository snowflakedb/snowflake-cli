from __future__ import annotations

import copy
import os
from typing import Any, Optional

from jinja2 import Environment, UndefinedError, nodes
from packaging.version import Version
from snowflake.cli.api.exceptions import CycleDetectedError, InvalidTemplate
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts, traverse
from snowflake.cli.api.utils.graph import Graph, Node
from snowflake.cli.api.utils.rendering import CONTEXT_KEY, get_snowflake_cli_jinja_env


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
        self.templated_value: Optional[str] = None
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

    def add_to_context(self, context: dict[str, Any]) -> None:
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

    def read_from_context(self, context: dict[str, Any]) -> Any:
        """
        Takes a multi-level context dict as input.

        If the variable has multi-levels (e.g. ctx.env), recursively traverse the dictionary
        to find the key that the variable points to.

        Returns the value in that location.

        Raise UndefinedError if the variable is None or not found.
        """
        current_dict_level = context
        for key in self._vars_chain:
            if (
                not isinstance(current_dict_level, dict)
                or key not in current_dict_level
            ):
                raise UndefinedError(f"Could not find template variable {self.key}")
            current_dict_level = current_dict_level[key]

        value = current_dict_level
        if value is None or isinstance(value, (dict, list)):
            raise UndefinedError(
                f"Template variable {self.key} does not contain a valid value"
            )

        return value

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return self.key == other.key


def _get_referenced_vars(
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
        not isinstance(ast_node, (nodes.Template, nodes.TemplateData, nodes.Output))
        or current_attr_chain is not None
    ):
        raise InvalidTemplate(f"Unexpected templating syntax in {template_value}")

    for child_node in ast_node.iter_child_nodes():
        all_referenced_vars.update(
            _get_referenced_vars(child_node, template_value, current_attr_chain)
        )

    return all_referenced_vars


def _get_referenced_vars_from_str(
    env: Environment, template_str: str
) -> set[TemplateVar]:
    ast = env.parse(template_str)
    return _get_referenced_vars(ast, template_str)


def _build_dependency_graph(
    env: Environment, all_vars: set[TemplateVar], context: dict[str, Any]
) -> Graph[TemplateVar]:
    dependencies_graph = Graph[TemplateVar]()
    for variable in all_vars:
        dependencies_graph.add(Node[TemplateVar](key=variable.key, data=variable))

    for variable in all_vars:
        if variable.is_env_var and variable.get_env_var_name() in os.environ:
            # If variable is found in os.environ, then use the value as is
            # skip rendering by pre-setting the rendered_value attribute
            env_value = os.environ.get(variable.get_env_var_name())
            variable.rendered_value = env_value
            variable.templated_value = env_value
        else:
            value: Any = variable.read_from_context(context)
            variable.templated_value = str(value)
            dependencies_vars = _get_referenced_vars_from_str(
                env, variable.templated_value
            )
            if len(dependencies_vars) == 0:
                variable.rendered_value = value

            for referenced_var in dependencies_vars:
                dependencies_graph.add_directed_edge(variable.key, referenced_var.key)

    return dependencies_graph


def _render_graph_node(jinja_env: Environment, node: Node[TemplateVar]) -> None:
    if node.data.rendered_value is not None:
        # Do not re-evaluate resolved nodes like env variable nodes
        # which might contain template-like values, or non-string nodes
        return

    current_context: dict[str, Any] = {}
    for dep_node in node.neighbors:
        dep_node.data.add_to_context(current_context)

    template = jinja_env.from_string(node.data.templated_value)
    node.data.rendered_value = template.render(current_context)


def _render_dict_element(
    jinja_env: Environment, context: dict[str, Any], element: str
) -> str:
    if _get_referenced_vars_from_str(jinja_env, element):
        template = jinja_env.from_string(element)
        return template.render(context)
    return element


def render_definition_template(original_definition: dict[str, Any]) -> dict[str, Any]:
    """
    Takes a definition file as input. An arbitrary structure containing dict|list|scalars,
    with the top level being a dictionary.
    Requires item 'definition_version' to be set to a version of 1.1 and higher.

    Searches for any templating in all of the scalar fields, and attempts to resolve them
    from the definition structure itself or from the environment variable.

    Environment variables take precedence during the rendering process.
    """

    # protect input from update
    definition = copy.deepcopy(original_definition)

    if "definition_version" not in definition or Version(
        definition["definition_version"]
    ) < Version("1.1"):
        return definition

    jinja_env = get_snowflake_cli_jinja_env()
    project_context = {CONTEXT_KEY: definition}

    referenced_vars = set()

    def find_any_template_vars(element):
        referenced_vars.update(_get_referenced_vars_from_str(jinja_env, element))

    traverse(definition, visit_action=find_any_template_vars)

    dependencies_graph = _build_dependency_graph(
        jinja_env, referenced_vars, project_context
    )

    def on_cycle_action(node: Node[TemplateVar]):
        raise CycleDetectedError(
            f"Cycle detected in templating variable {node.data.key}"
        )

    dependencies_graph.dfs(
        visit_action=lambda node: _render_graph_node(jinja_env, node),
        on_cycle_action=on_cycle_action,
    )

    # now that we determined the values of all templated vars,
    # use these resolved values as a fresh context to resolve definition
    final_context: dict = {}
    for node in dependencies_graph.get_all_nodes():
        node.data.add_to_context(final_context)

    traverse(
        definition,
        update_action=lambda val: _render_dict_element(jinja_env, final_context, val),
    )
    deep_merge_dicts(definition, {"env": dict(os.environ)})

    return definition
