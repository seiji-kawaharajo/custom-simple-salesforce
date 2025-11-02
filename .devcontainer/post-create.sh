#!/bin/bash
export UV_VENV_CLEAR=1
export UV_LINK_MODE=copy

uv venv
uv sync --dev

uv run pre-commit install --install-hooks