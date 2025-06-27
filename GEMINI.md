# Gemini Project Guidelines for mongodb-typegen

## Tooling and Commands

- **Package Management & Script Execution:** Always use `uv` for all Python-related operations.
  - Running tests: `uv run pytest`
  - Running tests with coverage: `uv run pytest --cov=src/mongodb_typegen --cov-report=term-missing tests/`
  - Installing packages: `uv pip install <package_name>`

- **Building the Package:** The project uses `hatchling`. To build the distribution files (`sdist` and `wheel`), use the `build` module.
  - Command: `uv run python -m build`

- **Publishing the Package:** Use `twine` to upload the package to PyPI.
  - Command: `uv run twine upload dist/*`

## Code Generation Conventions

- **`TypedDict` Syntax:** Always use the functional syntax for `TypedDict` (e.g., `MyType = TypedDict("MyType", {"field-with-space": str})`). This is critical because field names in MongoDB can contain characters that are invalid as Python identifiers. Do not use the class-based syntax.

- **Optional vs. NotRequired:**
  - Use `NotRequired[T]` for fields that may be entirely absent from a document.
  - Use `Optional[T]` for fields that are always present but whose value can be `None`.

- **Field Name Integrity:** The keys in the generated `TypedDict` definitions **must** be identical to the field names in the MongoDB collection. Do not modify them.

- **Class Naming:** The `to_pascal_case` function is used to generate the `TypedDict` class names from collection names.