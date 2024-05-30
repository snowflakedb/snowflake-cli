from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass
from typing import Optional

import yaml
from jinja2 import Environment, UndefinedError, nodes
from snowflake.cli.api.utils.graph import Graph
from snowflake.cli.api.utils.rendering import get_snowflake_cli_jinja_env


class Variable:
    def __init__(self, vars_chain):
        self._vars_chain = list(vars_chain)
        self.value: str | None = None

    def get_vars_hierarchy(self) -> deque[str]:
        return deque(self._vars_chain)

    def get_key(self):
        return ".".join(self._vars_chain)

    def __hash__(self):
        return hash(self.get_key())

    def __eq__(self, other):
        return self.get_key() == other.get_key()


@dataclass(eq=False)
class TemplateGraphNode(Graph.Node):
    templated_value: str | None = None
    rendered_value: str | None = None
    variable: Variable | None = None

    def store_in_context(self, context):
        # if graph node is ctx.env.test with value x, context will be built as {'ctx': {'env': {'test': 'x'}}}
        vars_chain = self.variable.get_vars_hierarchy()
        self._store_in_context_recursive(context, vars_chain)

    def _store_in_context_recursive(self, context, vars_chain: deque):
        if len(vars_chain) == 0:
            return self.rendered_value

        current_level_key = vars_chain.popleft()
        current_value_in_key = (
            context[current_level_key] if current_level_key in context else {}
        )
        context[current_level_key] = self._store_in_context_recursive(
            current_value_in_key, vars_chain
        )
        return context


def _get_referenced_vars(
    ast_node, variable_attr_chain: deque = deque()
) -> set[Variable]:
    # Var nodes end in Name node, and starts with Getattr node.
    # Example: ctx.env.test will look like Getattr(test) -> Getattr(env) -> Name(ctx)
    all_referenced_vars = set()

    variable_appended = False
    if isinstance(ast_node, nodes.Getattr):
        variable_attr_chain.appendleft(getattr(ast_node, "attr"))
        variable_appended = True
    elif isinstance(ast_node, nodes.Name):
        variable_attr_chain.appendleft(getattr(ast_node, "name"))
        variable_appended = True
        all_referenced_vars.add(Variable(variable_attr_chain))

    for child_node in ast_node.iter_child_nodes():
        all_referenced_vars.update(
            _get_referenced_vars(child_node, variable_attr_chain)
        )

    if variable_appended:
        variable_attr_chain.popleft()

    return all_referenced_vars


def _get_referenced_vars_from_str(
    env: Environment, template_str: Optional[str]
) -> set[Variable]:
    if template_str == None:
        return set()

    ast = env.parse(template_str)
    return _get_referenced_vars(ast)


def _get_value_from_var_path(env: Environment, context: dict, var_path: str):
    # given a variable path (e.g. ctx.env.test), return evaluated value based on context
    # fall back to env variables and escape them so we stop the chain of variables
    ref_str = "<% " + var_path + " %>"
    template = env.from_string(ref_str)
    try:
        # check in env variables
        return f"{env.block_start_string} raw {env.block_end_string}{template.render({'ctx': {'env': os.environ}})}{env.block_start_string} endraw {env.block_end_string}"
    except UndefinedError as e:
        try:
            return template.render(context)
        except:
            raise UndefinedError("Could not find template variable " + var_path)


def _build_dependency_graph(env, all_vars: set[Variable], context_without_env) -> Graph:
    dependencies_graph = Graph()
    for variable in all_vars:
        dependencies_graph.add(
            TemplateGraphNode(key=variable.get_key(), variable=variable)
        )

    for variable in all_vars:
        node: TemplateGraphNode = dependencies_graph.get(key=variable.get_key())
        node.templated_value = _get_value_from_var_path(
            env, context_without_env, variable.get_key()
        )
        dependencies_vars = _get_referenced_vars_from_str(env, node.templated_value)

        for referenced_var in dependencies_vars:
            dependencies_graph.add_dependency(
                variable.get_key(), referenced_var.get_key()
            )

    return dependencies_graph


def render_definition_template(definition):
    jinja_env = get_snowflake_cli_jinja_env()
    pdf_context = {"ctx": definition}
    pdf_yaml_str = yaml.dump(definition)

    referenced_vars = _get_referenced_vars_from_str(jinja_env, pdf_yaml_str)

    dependencies_graph = _build_dependency_graph(
        jinja_env, referenced_vars, pdf_context
    )

    def evaluate_node(node: TemplateGraphNode):
        current_context: dict = {}
        dep_node: TemplateGraphNode
        for dep_node in node.dependencies:
            dep_node.store_in_context(current_context)

        template = jinja_env.from_string(node.templated_value)
        node.rendered_value = template.render(current_context)

    dependencies_graph.dfs(visit_action=evaluate_node)

    # now that we determined value of all referenced vars, use these resolved values as a fresh context to resolve project file later
    final_context = {}
    node: TemplateGraphNode
    for node in dependencies_graph.get_all_nodes():
        node.store_in_context(final_context)

    # resolve combined project file based on final context
    template = jinja_env.from_string(pdf_yaml_str)
    rendered_template = template.render(final_context)
    rendered_definition = yaml.load(rendered_template, Loader=yaml.loader.BaseLoader)
    return rendered_definition
