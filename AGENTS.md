# Repository Guidelines

## Project Structure & Module Organization
Core code lives in `src/zotero_arxiv_daily/`. `main.py` is the Hydra entrypoint, `executor.py` drives the workflow, and the `retriever/` and `reranker/` packages separate paper collection from relevance scoring. Runtime configuration is stored in `config/` with `default.yaml`, `base.yaml`, and `custom.yaml`. Tests mirror the package layout under `tests/`, with shared fixtures in `tests/conftest.py` and mock service assets in `tests/utils/`. Repository images and setup screenshots live in `assets/`.

## Build, Test, and Development Commands
Use `uv` with Python 3.13.

- `uv sync --group dev`: install runtime and test dependencies.
- `uv run src/zotero_arxiv_daily/main.py`: run the app locally with Hydra config loading from `config/`.
- `uv run pytest`: run the full test suite used by CI.
- `uv run pytest tests/reranker/test_api_reranker.py`: run a focused test module while iterating.

CI in `.github/workflows/ci.yml` starts MailHog and a mock OpenAI service, so local test runs are easiest when matching those defaults from `tests/conftest.py`.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, `snake_case` for modules, functions, and variables, and `PascalCase` for classes such as `Executor` or `ArxivRetriever`. Keep imports grouped and avoid mixing retrieval, reranking, and email concerns in the same module. Prefer small helper functions over inline branching in `main.py`-level orchestration. No formatter is enforced in `pyproject.toml`, so keep changes PEP 8-aligned and consistent with nearby files.

## Testing Guidelines
Write tests with `pytest` and name files `test_*.py`. Place new tests beside the area they cover, for example `tests/retriever/` for retriever logic. Reuse fixtures from `tests/conftest.py` instead of hardcoding SMTP or API endpoints. Cover both happy paths and failure handling for external-service boundaries.

## Commit & Pull Request Guidelines
Recent commits use short, imperative subjects such as `Add timeouts for paper conversion` and `Suppress noisy onnxruntime warnings`. Keep commit titles concise, capitalized, and focused on one change. Open pull requests against the `dev` branch, not `main`. Include a clear summary, note config or workflow changes, link related issues, and attach screenshots only when README or email-rendered output changes.

## Security & Configuration Tips
Do not commit real Zotero, SMTP, or API secrets. Keep overrides in environment variables or untracked local config. When changing config keys, update both `README.md` examples and the matching workflow assumptions.
