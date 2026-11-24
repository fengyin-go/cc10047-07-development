"""Pytest 引导：无论从哪个工作目录调用 pytest，都把 backend 目录加入 sys.path。"""
from __future__ import annotations

import os
import sys

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)
