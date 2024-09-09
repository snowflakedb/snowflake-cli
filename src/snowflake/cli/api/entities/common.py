import inspect
import logging
from typing import Callable, Generic, List, Type, TypeVar, get_args

from snowflake.cli._plugins.workspace.action_context import ActionContext
from snowflake.cli.api.entities.actions import EntityAction
from snowflake.cli.api.sql_execution import SqlExecutor

logger = logging.getLogger(__name__)


T = TypeVar("T")


class EntityBase(Generic[T]):
    """
    Base class for the fully-featured entity classes.
    """

    class __metaclass__(type):  # noqa: N801
        def __new__(cls, name, bases, attr_dict):
            """
            Discover @EntityAction.impl() annotations.
            """
            kls = type.__new__(cls, name, bases, attr_dict)

            kls._entity_actions_attrs = {}  # noqa: SLF001
            for (key, func) in attr_dict.items():
                action = func.entity_action
                if action:
                    logger.warning(
                        "Found entity action %s (%s) on %s", key, action, name
                    )
                    kls._entity_actions_attrs[action] = key  # noqa: SLF001
                    pass

            return kls

    def __init__(self, entity_model: T):
        self._entity_model = entity_model

    @classmethod
    def get_entity_model_type(cls) -> Type[T]:
        """
        Returns the generic model class specified in each entity class.

        For example, calling ApplicationEntity.get_entity_model_type() will return the ApplicationEntityModel class.
        """
        return get_args(cls.__orig_bases__[0])[0]  # type: ignore[attr-defined]

    @classmethod
    def supports(cls, action: EntityAction) -> bool:
        """
        Checks whether this entity supports the given action.
        An entity is considered to support an action if it implements a method with the action name.
        """
        return callable(getattr(cls, action.attr_name, None))

    @classmethod
    def get_action_callable(cls, action: EntityAction) -> Callable:
        """
        Returns a generic action callable that is _not_ bound to a particular entity.
        """
        attr = getattr(cls, action.attr_name)  # raises KeyError
        if not callable(attr):
            raise ValueError(f"{action} method exists but is not callable")
        return attr

    @classmethod
    def get_action_params_as_inspect(
        cls, action: EntityAction
    ) -> List[inspect.Parameter]:
        """
        Combines base action parameters with those registered directly on the @ActionImplementation
        """
        raise NotImplementedError()

    def perform(self, action: EntityAction, action_ctx: ActionContext, *args, **kwargs):
        """Performs the requested action."""
        fn = getattr(self, action.attr_name)

        # TODO: move into decorator that populates self._entity_actions_attrs
        orig_sig = inspect.signature(fn)
        merged_params = []
        for param in orig_sig.parameters.values():
            base_action_param = action.params_map.get(param.name, None)
            if (
                base_action_param
                and param.default == inspect.Parameter.empty
                and base_action_param.typer_param.default
            ):
                merged_params.append(
                    param.replace(default=base_action_param.typer_param.default)
                )
            else:
                merged_params.append(param)

        sig = orig_sig.replace(parameters=merged_params)
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()

        return fn(action_ctx, *bound.args, **bound.kwargs)


def get_sql_executor() -> SqlExecutor:
    """Returns an SQL Executor that uses the connection from the current CLI context"""
    return SqlExecutor()
