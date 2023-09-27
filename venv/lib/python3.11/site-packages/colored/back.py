#!/usr/bin/env python
# -*- coding: utf-8 -*-


from .colors import names


class back:

    ESC = '\x1b[48;5;'
    END = 'm'
    for num, color in enumerate(names):
        vars()[color] = f'{ESC}{num}{END}'
