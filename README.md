# custom-simple-salesforce

## Documentation 📚

This project's documentation is automatically built and deployed to GitHub Pages.

[![Deploy MkDocs](https://github.com/seiji-kawaharajo/custom-simple-salesforce/actions/workflows/deploy-docs.yml/badge.svg)](https://github.com/seiji-kawaharajo/custom-simple-salesforce/actions/workflows/deploy-docs.yml)

**[➡️ View the live documentation here!](https://seiji-kawaharajo.github.io/custom-simple-salesforce/)**

## develop setup

### pre-comit active

```bash
pre-commit install
```

### venv active

```bash
source /workspaces/custom-simple-salesforce/.venv/bin/activate
```

### mkdocs

#### developer server

```bash
uv run mkdocs serve
```

#### build

```bash
uv run mkdocs build
```