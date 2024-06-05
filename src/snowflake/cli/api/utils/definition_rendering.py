from __future__ import annotations

import os

from jinja2 import Environment, UndefinedError, nodes
from packaging.version import Version
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts, deep_traverse
from snowflake.cli.api.utils.graph import Graph, Node
from snowflake.cli.api.utils.rendering import CONTEXT_KEY, get_snowflake_cli_jinja_env


class Variable:
    def __init__(self, vars_chain):
        self._vars_chain = list(vars_chain)
        self.templated_value = None
        self.rendered_value = None

    def get_key(self):
        return ".".join(self._vars_chain)

    def is_env_var(self):
        return (
            len(self._vars_chain) == 3
            and self._vars_chain[0] == CONTEXT_KEY
            and self._vars_chain[1] == "env"
        )

    def get_env_var_name(self) -> str:
        if not self.is_env_var():
            raise KeyError(
                f"Referenced variable {self.get_key()} is not an environment variable"
            )
        return self._vars_chain[2]

    def store_in_context(self, context: dict, value):
        """
        Takes a generic context dict to modify, and a value

        Traverse through the multi-level dictionary to the location where this variables goes to.
        Sets this location to the content of value.

        Example: vars chain contains ['ctx', 'env', 'x'], and context is {}, and value is 'val'.
        At the end of this call, context content will be: {'ctx': {'env': {'x': 'val'}}}
        """
        current_dict_level = context
        for i, var in enumerate(self._vars_chain):
            if i == len(self._vars_chain) - 1:
                current_dict_level[var] = value
            else:
                current_dict_level.setdefault(var, {})
                current_dict_level = current_dict_level[var]

    def read_from_context(self, context):
        """
        Takes a context dict as input.

        Traverse through the multi-level dictionary to the location where this variable goes to.
        Returns the value in that location.

        Raise UndefinedError if the variable is None or not found.
        """
        current_dict_level = context
        for key in self._vars_chain:
            if (
                not isinstance(current_dict_level, dict)
                or key not in current_dict_level
            ):
                raise UndefinedError(
                    f"Could not find template variable {self.get_key()}"
                )
            current_dict_level = current_dict_level[key]

        value = current_dict_level
        if value == None or isinstance(value, dict) or isinstance(value, list):
            raise UndefinedError(
                f"Template variable {self.get_key()} does not contain a valid value"
            )

        return value

    def __hash__(self):
        return hash(self.get_key())

    def __eq__(self, other):
        return self.get_key() == other.get_key()


def _get_referenced_vars(ast_node, current_attr_chain: list[str] = []) -> set[Variable]:
    """
    Traverse Jinja AST to find the variable chain referenced by the template.
    A variable like ctx.env.test is internally represented in the AST tree as
    Getattr Node (attr='test') -> Getattr Node (attr='env') -> Name Node (name='ctx')
    """
    all_referenced_vars = set()
    if isinstance(ast_node, nodes.Getattr):
        current_attr_chain = [getattr(ast_node, "attr")] + current_attr_chain
    elif isinstance(ast_node, nodes.Name):
        current_attr_chain = [getattr(ast_node, "name")] + current_attr_chain
        all_referenced_vars.add(Variable(current_attr_chain))

    for child_node in ast_node.iter_child_nodes():
        all_referenced_vars.update(_get_referenced_vars(child_node, current_attr_chain))

    return all_referenced_vars


def _get_referenced_vars_from_str(env: Environment, template_str: str) -> set[Variable]:
    ast = env.parse(template_str)
    return _get_referenced_vars(ast)


def _build_dependency_graph(
    env: Environment, all_vars: set[Variable], context
) -> Graph[Variable]:
    dependencies_graph = Graph[Variable]()
    for variable in all_vars:
        dependencies_graph.add(Node[Variable](key=variable.get_key(), data=variable))

    for variable in all_vars:
        if variable.is_env_var() and variable.get_env_var_name() in os.environ:
            # If variable is found in os.environ, then use the value as is
            # skip rendering by pre-setting the rendered_value attribute
            env_value = os.environ.get(variable.get_env_var_name())
            variable.rendered_value = env_value
            variable.templated_value = env_value
        else:
            variable.templated_value = str(variable.read_from_context(context))
            dependencies_vars = _get_referenced_vars_from_str(
                env, variable.templated_value
            )
            for referenced_var in dependencies_vars:
                dependencies_graph.add_directed_edge(
                    variable.get_key(), referenced_var.get_key()
                )

    return dependencies_graph


def _render_graph_node(jinja_env: Environment, node: Node[Variable]):
    if node.data.rendered_value is not None:
        # Do not re-evaluate resolved nodes like env variable nodes,
        # which might contain template-like values
        return

    current_context: dict = {}
    for dep_node in node.neighbors:
        dep_node.data.store_in_context(current_context, dep_node.data.rendered_value)

    template = jinja_env.from_string(node.data.templated_value)
    node.data.rendered_value = template.render(current_context)


def _render_dict_element(jinja_env: Environment, context, element):
    if _get_referenced_vars_from_str(jinja_env, element):
        template = jinja_env.from_string(element)
        return template.render(context)
    return element


def render_definition_template(definition: dict):
    if "definition_version" not in definition or Version(
        definition["definition_version"]
    ) < Version("1.1"):
        return definition

    jinja_env = get_snowflake_cli_jinja_env()
    pdf_context = {CONTEXT_KEY: definition}

    referenced_vars = set()

    def find_any_template_vars(element):
        referenced_vars.update(_get_referenced_vars_from_str(jinja_env, element))

    deep_traverse(definition, visit_action=find_any_template_vars)

    dependencies_graph = _build_dependency_graph(
        jinja_env, referenced_vars, pdf_context
    )

    dependencies_graph.dfs(
        visit_action=lambda node: _render_graph_node(jinja_env, node)
    )

    # now that we determined the values of all tempalted vars,
    # use these resolved values as a fresh context to resolve definition
    final_context: dict = {}
    for node in dependencies_graph.get_all_nodes():
        node.data.store_in_context(final_context, node.data.rendered_value)

    deep_traverse(
        definition,
        update_action=lambda val: _render_dict_element(jinja_env, final_context, val),
    )
    deep_merge_dicts(definition, {"env": dict(os.environ)})

    return definition
