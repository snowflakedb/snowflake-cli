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

from abc import ABC, abstractmethod
from inspect import signature
from typing import Any, List, Optional, Tuple

import typer
from click import ClickException
from snowflake.cli.api.exceptions import IncompatibleParametersError


class _OverrideableParameter(ABC):
    """
    Class that allows you to generate instances of typer.models.OptionInfo with some default properties while allowing
    specific values to be overridden.

    Custom parameters:
    - mutually_exclusive (Tuple[str]|List[str]): A list of parameter names that this Option is not compatible with. If this Option has
     a truthy value and any of the other parameters in the mutually_exclusive list has a truthy value, a
     ClickException will be thrown. Note that mutually_exclusive can contain an option's own name but does not require
     it.
    """

    def __init__(
        self,
        default: Any = ...,
        *param_decls: str,
        mutually_exclusive: Optional[List[str] | Tuple[str]] = None,
        **kwargs,
    ):
        self.default = default
        self.param_decls = param_decls
        self.mutually_exclusive = mutually_exclusive
        self.kwargs = kwargs

    def __call__(self, **kwargs) -> typer.models.ParameterInfo:
        """
        Returns a typer.models.OptionInfo instance initialized with the specified default values along with any overrides
        from kwargs. Note that if you are overriding param_decls, you must pass an iterable of strings, you cannot use
        positional arguments like you can with typer.Option. Does not modify the original instance.
        """
        default = kwargs.get("default", self.default)
        param_decls = kwargs.get("param_decls", self.param_decls)
        mutually_exclusive = kwargs.get("mutually_exclusive", self.mutually_exclusive)
        if not isinstance(param_decls, list) and not isinstance(param_decls, tuple):
            raise TypeError("param_decls must be a list or tuple")
        passed_kwargs = self.kwargs.copy()
        passed_kwargs.update(kwargs)
        if passed_kwargs.get("callback", None) or mutually_exclusive:
            passed_kwargs["callback"] = self._callback_factory(
                passed_kwargs.get("callback", None), mutually_exclusive
            )
        for non_kwarg in ["default", "param_decls", "mutually_exclusive"]:
            passed_kwargs.pop(non_kwarg, None)

        return self.get_parameter(default, *param_decls, **passed_kwargs)

    @abstractmethod
    def get_parameter(
        self, default: Any = None, *param_decls: str, **kwargs
    ) -> typer.models.ParameterInfo:
        pass

    class InvalidCallbackSignature(ClickException):
        def __init__(self, callback):
            super().__init__(
                f"Signature {signature(callback)} is not valid for an OverrideableOption callback function. Must have "
                f"at most one parameter with each of the following types: (typer.Context, typer.CallbackParam, "
                f"Any Other Type)"
            )

    def _callback_factory(
        self, callback, mutually_exclusive: Optional[List[str] | Tuple[str]]
    ):
        callback = callback if callback else lambda x: x

        # inspect existing_callback to make sure signature is valid
        existing_params = signature(callback).parameters
        # at most one parameter with each type in [typer.Context, typer.CallbackParam, any other type]
        limits = [
            lambda x: x == typer.Context,
            lambda x: x == typer.CallbackParam,
            lambda x: x != typer.Context and x != typer.CallbackParam,
        ]
        for limit in limits:
            if len([v for v in existing_params.values() if limit(v.annotation)]) > 1:
                raise self.InvalidCallbackSignature(callback)

        def generated_callback(ctx: typer.Context, param: typer.CallbackParam, value):
            if mutually_exclusive:
                for name in mutually_exclusive:
                    if value and ctx.params.get(
                        name, False
                    ):  # if the current parameter is set to True and a previous parameter is also Truthy
                        curr_opt = param.opts[0]
                        other_opt = [x for x in ctx.command.params if x.name == name][
                            0
                        ].opts[0]
                        raise IncompatibleParametersError([curr_opt, other_opt])

            # pass args to existing callback based on its signature (this is how Typer infers callback args)
            passed_params = {}
            for existing_param in existing_params:
                annotation = existing_params[existing_param].annotation
                if annotation == typer.Context:
                    passed_params[existing_param] = ctx
                elif annotation == typer.CallbackParam:
                    passed_params[existing_param] = param
                else:
                    passed_params[existing_param] = value
            return callback(**passed_params)

        return generated_callback


class OverrideableArgument(_OverrideableParameter):
    def get_parameter(
        self, default: Any = ..., *param_decls: str, **kwargs
    ) -> typer.models.ArgumentInfo:
        return typer.Argument(default, *param_decls, **kwargs)


# OverrideableOption doesn't work with flags with type List[Any] and default None, because typer executes the callback
# function which converts the default value iterating over it, but None is not iterable.
class OverrideableOption(_OverrideableParameter):
    def get_parameter(
        self, default: Any = ..., *param_decls: str, **kwargs
    ) -> typer.models.OptionInfo:
        return typer.Option(default, *param_decls, **kwargs)
