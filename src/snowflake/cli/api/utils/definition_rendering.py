from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from typing import List

import yaml
from jinja2 import Environment, UndefinedError, nodes
from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
from snowflake.cli.api.utils.rendering import get_snowflake_cli_jinja_env


@dataclass
class GraphNode:
    path: str
    value: str | None = None


def _get_referenced_vars(ast_node, attr_chain: List[str] = []) -> List[List[str]]:
    # Var nodes end in Name node, and starts with Getattr node.
    # Example: ctx.env.test will look like Getattr(test) -> Getattr(env) -> Name(ctx)
    all_referenced_vars = []

    if isinstance(ast_node, nodes.Getattr):
        attr_chain = [getattr(ast_node, "attr")] + attr_chain
    elif isinstance(ast_node, nodes.Name):
        all_referenced_vars = [[getattr(ast_node, "name")] + attr_chain]

    for child_node in ast_node.iter_child_nodes():
        all_referenced_vars = all_referenced_vars + _get_referenced_vars(
            child_node, attr_chain
        )

    return all_referenced_vars


def _get_referenced_vars_from_str(env: Environment, template_str: str) -> List[str]:
    ast = env.parse(template_str)
    referenced_vars = _get_referenced_vars(ast)
    result = [".".join(vars_parts) for vars_parts in referenced_vars]
    return result


def _get_value_from_var_path(env: Environment, context: dict, var_path: str):
    # given a variable path (e.g. ctx.env.test), return evaluated value based on context
    # fall back to env variables and escape them so we stop the chain of variables
    ref_str = "<% " + var_path + " %>"
    template = env.from_string(ref_str)
    try:
        return template.render(context)
    except UndefinedError as e:
        # check in env variables
        try:
            return f"{env.block_start_string} raw {env.block_end_string}{template.render({'ctx': {'env': os.environ}})}{env.block_start_string} endraw {env.block_end_string}"
        except:
            raise UndefinedError("Could not find template variable " + var_path)


def _find_node_with_no_dependencies(graph):
    for node, deps in graph.items():
        if len(deps) == 0:
            return node
    return None


def _remove_node_from_graph(graph, node):
    del graph[node]
    for n, deps in graph.items():
        if node in deps:
            deps.remove(node)


def _check_for_cycles(original_graph):
    graph = copy.deepcopy(original_graph)
    while len(graph) > 0:
        node = _find_node_with_no_dependencies(graph)
        if node == None:
            raise RecursionError("Cycle detected in project definition file template")
        _remove_node_from_graph(graph, node)


def _build_dependency_graph(env, referenced_vars, context_without_env):
    dependencies_graph = {}
    for node in referenced_vars:
        dependencies_graph[node] = []

    for node in referenced_vars:
        value = _get_value_from_var_path(env, context_without_env, node)
        depends_on = _get_referenced_vars_from_str(env, value)
        for dependency in depends_on:
            if not dependency in dependencies_graph:
                raise RuntimeError(
                    f"unexpected dependency {dependency} not in {dependencies_graph.keys()}"
                )

        dependencies_graph[node] = depends_on
    return dependencies_graph


def _fill_context_recursive(context, attrs, value):
    if len(attrs) == 0:
        return

    if len(attrs) == 1:
        context[attrs[0]] = value
        return

    if attrs[0] not in context:
        context[attrs[0]] = {}

    _fill_context_recursive(context[attrs[0]], attrs[1:], value)


def _fill_context(context_to_be_filled, graph_node: GraphNode):
    # if graph node is ctx.env.test with value x, context will be built as {'ctx': {'env': {'test': 'x'}}}
    value = graph_node.value
    attrs = graph_node.path.split(".")
    _fill_context_recursive(context_to_be_filled, attrs, value)


def _resolve_node_values(
    jinja_env, dependencies_graph, node: GraphNode, graph_nodes_map, context_with_env
):
    if node.value:
        return node.value
    dependencies = dependencies_graph[node.path]

    my_context: dict = {}
    for dep in dependencies:
        dep_node: GraphNode = graph_nodes_map[dep]
        if not dep_node.value:
            dep_node.value = _resolve_node_values(
                jinja_env,
                dependencies_graph,
                dep_node,
                graph_nodes_map,
                context_with_env,
            )
        _fill_context(my_context, dep_node)
    if len(dependencies) == 0:
        my_context = context_with_env

    value = _get_value_from_var_path(jinja_env, context_with_env, node.path)
    template = jinja_env.from_string(value)
    node.value = template.render(my_context)
    return node.value


def render_project_template(project):
    jinja_env = get_snowflake_cli_jinja_env()
    context_without_env = {"ctx": project.model_dump(exclude_unset=True)}
    context_with_env = {"ctx": project}
    combined_pdf_as_str = yaml.dump(project.model_dump(exclude_unset=True))

    referenced_vars = _get_referenced_vars_from_str(jinja_env, combined_pdf_as_str)

    # build dependency graph without env variables, because these cannot reference other vars.
    # env vars are used as backup as leaf nodes (just for graph purposes)
    dependencies_graph = _build_dependency_graph(
        jinja_env, referenced_vars, context_without_env
    )
    _check_for_cycles(dependencies_graph)

    # store extra node information (var_path to GraphNode object map)
    graph_nodes_map = {}
    for path in dependencies_graph.keys():
        graph_nodes_map[path] = GraphNode(path=path)

    # recursively resolve node values of the graph
    for referenced_var in graph_nodes_map.values():
        _resolve_node_values(
            jinja_env,
            dependencies_graph,
            referenced_var,
            graph_nodes_map,
            context_with_env,
        )

    # now that we determined value of all referenced vars, use these resolved values as a fresh context to resolve project file later
    final_context = {}
    for referenced_var in graph_nodes_map.values():
        _fill_context(final_context, referenced_var)

    # resolve combined project file based on final context
    template = jinja_env.from_string(combined_pdf_as_str)
    rendered_template = template.render(final_context)
    rendered_definition = yaml.load(rendered_template, Loader=yaml.loader.BaseLoader)
    rendered_project = ProjectDefinition(**rendered_definition)

    return rendered_project
