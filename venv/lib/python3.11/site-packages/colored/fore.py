#!/usr/bin/env python
# -*- coding: utf-8 -*-


from .colors import names


class fore:

    ESC = '\x1b[38;5;'
    END = 'm'
    for num, color in enumerate(names):
        vars()[color] = f'{ESC}{num}{END}'
