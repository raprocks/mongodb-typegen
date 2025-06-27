"""
MongoDB TypedDict Code Generator

A library that connects to MongoDB, analyzes collections, and generates
TypedDict classes based on the document schemas found in the database.
"""

import pymongo
from pymongo.database import Database
import pymongo.errors
from bson.objectid import ObjectId
from datetime import datetime
from collections import defaultdict, deque
import keyword
import click
import sys
import os
from pathlib import Path
from typing import Any, Set, Dict, List, Optional, Union

# PEP 655
try:
    from typing import NotRequired
except ImportError:
    from typing_extensions import NotRequired


def to_pascal_case(name: str) -> str:
    """Converts a snake_case or kebab-case string to PascalCase for class names."""
    if not name:
        return ""
    # Check if it's already mostly PascalCase
    if ' ' not in name and '_' not in name and '-' not in name and name[0].isupper():
        return name
    return name.replace('_', ' ').replace('-', ' ').title().replace(' ', '')

class MongoTypedDictGenerator:
    """
    Connects to a MongoDB database, inspects collections, and generates
    Python TypedDict classes representing the data schemas.
    """

    def __init__(self, mongo_uri: str, db_name: str, verbose: bool = False):
        """
        Initializes the generator.

        Args:
            mongo_uri: The connection string for the MongoDB instance.
            db_name: The name of the database to inspect.
            verbose: Enable verbose output.
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.verbose = verbose
        self.client: Optional[pymongo.MongoClient] = None
        self.db: Optional[Database] = None
        self.generated_classes: Dict[str, str] = {}
        self.class_dependencies: Dict[str, Set[str]] = defaultdict(set)
        self.imports: Set[str] = {
            "from typing import TypedDict, List, Optional, Union, Any",
            "from datetime import datetime",
            "from bson.objectid import ObjectId",
            "try:",
            "    from typing import NotRequired",
            "except ImportError:",
            "    from typing_extensions import NotRequired"
        }

    def connect(self):
        """Connects to the MongoDB database."""
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db: Database = self.client[self.db_name]
            self.client.server_info()  # Check connection
            self._log(f"Successfully connected to MongoDB server at {self.mongo_uri}")
            self._log(f"Using database: '{self.db_name}'")
        except pymongo.errors.ConnectionFailure as e:
            raise ConnectionError(f"Could not connect to MongoDB. Please check your URI.") from e

    def disconnect(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            self._log("MongoDB connection closed.")

    def _log(self, message: str, **kwargs):
        """Prints a message if not in quiet mode."""
        if self.verbose:
            click.echo(message, **kwargs)

    def list_collections(self) -> List[str]:
        """Lists all collection names in the database."""
        if not self.db:
            raise ConnectionError("Not connected to the database.")
        return self.db.list_collection_names()

    def sample_documents(self, collection_name: str, sample_size: int) -> List[Dict]:
        """Samples documents from a collection."""
        if not self.db:
            raise ConnectionError("Not connected to the database.")
        self._log(f"Sampling {sample_size} documents from '{collection_name}'...")
        try:
            # Use aggregation pipeline for efficient random sampling
            pipeline = [{"$sample": {"size": sample_size}}]
            docs = list(self.db[collection_name].aggregate(pipeline))
            self._log(f"  -> Found {len(docs)} documents.")
            return docs
        except Exception as e:
            click.echo(f"Warning: Could not sample documents from collection '{collection_name}'. Skipping. Error: {e}", err=True)
            return []

    def _map_value_to_type_str(self, value: Any, field_name: str, collection_name: str) -> str:
        """
        Recursively maps a Python value to its type hint string.
        Handles nested dictionaries by generating new TypedDicts.
        """
        if value is None:
            return "Any"  # Represents a literal None value
        if isinstance(value, str):
            return "str"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, datetime):
            return "datetime"
        if isinstance(value, ObjectId):
            return "ObjectId"
        if isinstance(value, list):
            if not value:
                return "List[Any]"
            list_item_types = {self._map_value_to_type_str(item, field_name, collection_name) for item in value}
            # Filter out "Any" if other types are present, as it means the value can be None
            if len(list_item_types) > 1:
                list_item_types.discard("Any")
            
            if not list_item_types:
                return "List[Any]"
            if len(list_item_types) == 1:
                return f"List[{list_item_types.pop()}]"
            else:
                # Using sorted for consistent ordering in Union
                return f"List[Union[{', '.join(sorted(list(list_item_types)))}]]"
        if isinstance(value, dict):
            # Generate a unique, descriptive name for the nested TypedDict
            parent_class_name = to_pascal_case(collection_name)
            nested_class_name = f"{parent_class_name}{to_pascal_case(field_name)}"
            
            # Track dependency
            self.class_dependencies[parent_class_name].add(nested_class_name)

            if nested_class_name not in self.generated_classes:
                # Placeholder to prevent infinite recursion
                self.generated_classes[nested_class_name] = "" # Placeholder to prevent infinite recursion
                nested_schema = self._infer_schema_from_docs([value], collection_name)
                class_def = self._create_typeddict_str(nested_class_name, nested_schema)
                self.generated_classes[nested_class_name] = class_def
            return nested_class_name
        return "Any"

    def _infer_schema_from_docs(self, docs: List[Dict], collection_name: str) -> Dict[str, Dict[str, Any]]:
        """Infers a schema from a list of documents."""
        field_data = defaultdict(lambda: {"types": set(), "count": 0})
        total_docs = len(docs)
        
        # Get a set of all keys across all documents to correctly identify missing fields
        all_keys = set()
        for doc in docs:
            all_keys.update(doc.keys())

        for doc in docs:
            for key in all_keys:
                if key in doc:
                    field_data[key]["count"] += 1
                    value = doc[key]
                    type_str = self._map_value_to_type_str(value, key, collection_name)
                    field_data[key]["types"].add(type_str)
        
        final_schema = {}
        for key, data in field_data.items():
            # A field is optional if its count is less than the total number of documents
            is_optional = data["count"] < total_docs
            types = data["types"]
            
            # If a field never appeared (e.g., only in some docs), its types will be empty
            if not types:
                types.add("Any")

            final_schema[key] = {"types": types, "is_optional": is_optional}
        return final_schema

    def _create_typeddict_str(self, class_name: str, schema: Dict) -> str:
        """Creates the string representation of a TypedDict class."""
        fields = []
        for name, properties in sorted(schema.items()):
            types = sorted(list(properties["types"]))
            is_optional = properties["is_optional"]
            can_be_none = "Any" in types

            # Refine types: remove "Any" if other types are present
            if len(types) > 1:
                types = [t for t in types if t != "Any"]

            # Create the base type hint
            type_hint = f"Union[{', '.join(types)}]" if len(types) > 1 else types[0]

            # The _id field is special, it's often present but we handle it explicitly
            if name == "_id":
                type_hint = "ObjectId"
            
            # Determine the correct annotation: NotRequired, Optional, or regular
            if is_optional:
                # The key may not be present
                if can_be_none:
                    # The key may not be present, and if it is, its value can be None
                    type_hint = f"NotRequired[Optional[{type_hint}]]"
                else:
                    # The key may not be present, but if it is, its value is not None
                    type_hint = f"NotRequired[{type_hint}]"
            elif can_be_none:
                # The key is always present, but its value can be None
                type_hint = f"Optional[{type_hint}]"

            # Use repr(name) to handle names with spaces or special characters
            fields.append(f"    {repr(name)}: {type_hint}")

        if not fields:
            return f'{class_name} = TypedDict("{class_name}", {{}})'
            
        fields_str = "\n".join(fields)
        return f'{class_name} = TypedDict("{class_name}", {{\n{fields_str}\n}})'

    def _topological_sort(self) -> List[str]:
        """Sorts classes based on their dependencies."""
        sorted_order = []
        in_degree = {u: 0 for u in self.generated_classes}
        
        # Invert dependencies to build the graph correctly for topological sort
        adj = defaultdict(list)
        for u, deps in self.class_dependencies.items():
            for v in deps:
                if u in self.generated_classes and v in self.generated_classes:
                    adj[v].append(u)
                    in_degree[u] += 1
        
        # Queue for nodes with no incoming edges
        queue = deque([u for u in self.generated_classes if in_degree[u] == 0])
        
        while queue:
            u = queue.popleft()
            sorted_order.append(u)
            
            for v in adj[u]:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)
        
        # If there is a cycle, the sort will be incomplete
        if len(sorted_order) == len(self.generated_classes):
            return sorted_order
        else:
            # Fallback to alphabetical sort if a cycle is detected (should be rare)
            return sorted(self.generated_classes.keys())

    def generate_models_for_collections(self, collection_names: List[str], sample_size: int = 100) -> str:
        """Generates all TypedDict models for a list of collections."""
        self.generated_classes = {} # Reset for each run
        self.class_dependencies = defaultdict(set)
        
        with click.progressbar(
            collection_names, 
            label='Generating models',
            item_show_func=lambda item: f"Collection: {item}" if item else ""
        ) as bar:
            for name in bar:
                docs = self.sample_documents(name, sample_size)
                if not docs:
                    self._log(f"  Collection '{name}' is empty or could not be read. Skipping.")
                    continue
                
                class_name = to_pascal_case(name)
                if class_name in self.generated_classes:
                    continue

                schema = self._infer_schema_from_docs(docs, name)
                class_def = self._create_typeddict_str(class_name, schema)
                self.generated_classes[class_name] = class_def
        
        header = "# This file was auto-generated by mongodb-typegen. Do not edit manually.\n\n"
        imports_str = "\n".join(sorted(list(self.imports))) + "\n\n"
        
        # Order classes with dependencies first
        sorted_class_names = self._topological_sort()
        all_class_defs = "\n\n\n".join(self.generated_classes[key] for key in sorted_class_names if key in self.generated_classes)

        return header + imports_str + all_class_defs

    def generate_preview_for_collection(self, collection_name: str, sample_size: int = 10) -> Dict:
        """Generates a schema preview for a single collection."""
        docs = self.sample_documents(collection_name, sample_size)
        if not docs:
            return {}
        
        schema = self._infer_schema_from_docs(docs, collection_name)
        typed_dict = self._create_typeddict_str(to_pascal_case(collection_name), schema)
        
        return {"schema": schema, "typed_dict": typed_dict, "docs_sampled": len(docs)}


@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.version_option(version='1.0.0', prog_name='mongodb-typegen')
def cli():
    """A CLI tool to generate Python TypedDict models from a MongoDB database."""
    pass

def common_db_options(func):
    """A decorator to add common database options."""
    func = click.option(
        '--uri', '-u',
        default='mongodb://localhost:27017/',
        help='MongoDB connection string.',
        show_default=True
    )(func)
    func = click.option(
        '--db', '-d',
        required=True,
        help='Name of the MongoDB database to analyze.'
    )(func)
    return func

def handle_exceptions(func):
    """A decorator to handle common exceptions."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError as e:
            click.echo(f"Error: {e}", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"An unexpected error occurred: {e}", err=True)
            sys.exit(1)
    return wrapper

@cli.command('generate')
@common_db_options
@click.option(
    '--out', '-o',
    type=click.Path(dir_okay=False, writable=True),
    default='generated_models.py',
    help='Output file path for generated models.',
    show_default=True
)
@click.option(
    '--sample-size', '-s',
    default=100,
    help='Number of documents to sample per collection.',
    show_default=True,
    type=click.IntRange(1, 10000)
)
@click.option(
    '--collections', '-c',
    help='Comma-separated list of specific collections to process (default: all).'
)
@click.option(
    '--exclude', '-e',
    help='Comma-separated list of collections to exclude.'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be generated without writing to a file.'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Enable verbose output.'
)
@click.option(
    '--quiet', '-q',
    is_flag=True,
    help='Suppress all output except errors.'
)
@handle_exceptions
def generate(uri, db, out, sample_size, collections, exclude, dry_run, verbose, quiet):
    """Generate TypedDict models from collections."""
    if quiet and verbose:
        click.echo("Error: --quiet and --verbose cannot be used together.", err=True)
        sys.exit(1)

    if not dry_run and os.path.exists(out):
        if not click.confirm(f"Output file '{out}' already exists. Overwrite?"):
            click.echo("Aborted.")
            sys.exit(0)

    # The real 'verbose' flag is the opposite of 'quiet'
    is_verbose = not quiet

    generator = MongoTypedDictGenerator(mongo_uri=uri, db_name=db, verbose=is_verbose)
    generator.connect()

    all_collections = generator.list_collections()
    target_collections = all_collections

    if collections:
        target_collections = [c.strip() for c in collections.split(',')]
        missing = set(target_collections) - set(all_collections)
        if missing:
            click.echo(f"Warning: Specified collections not found: {', '.join(missing)}", err=True)
        target_collections = [c for c in target_collections if c in all_collections]

    if exclude:
        excluded_collections = {c.strip() for c in exclude.split(',')}
        target_collections = [c for c in target_collections if c not in excluded_collections]

    if not target_collections:
        click.echo("No collections to process.", err=True)
        sys.exit(1)
    
    if is_verbose:
        click.echo(f"Processing {len(target_collections)} collections: {', '.join(target_collections)}")

    generated_code = generator.generate_models_for_collections(target_collections, sample_size)
    generator.disconnect()

    if not generated_code.strip() or not generator.generated_classes:
        click.echo("\nGeneration finished with no output.", err=True)
        return

    if dry_run:
        click.echo("\n--- Generated Code (Dry Run) ---")
        click.echo(generated_code)
    else:
        try:
            Path(out).parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                f.write(generated_code)
            if is_verbose:
                click.echo(f"\n✅ Models successfully generated and saved to '{out}'")
        except IOError as e:
            click.echo(f"Error writing to file '{out}': {e}", err=True)
            sys.exit(1)

@cli.command('list-collections')
@common_db_options
@handle_exceptions
def list_collections(uri, db):
    """List all collections in the specified database."""
    generator = MongoTypedDictGenerator(mongo_uri=uri, db_name=db)
    generator.connect()
    collections = generator.list_collections()
    generator.disconnect()
    
    if collections:
        click.echo(f"Collections in database '{db}':")
        for i, collection in enumerate(collections, 1):
            click.echo(f"  {i:2d}. {collection}")
    else:
        click.echo(f"No collections found in database '{db}'.")

@cli.command('preview')
@common_db_options
@click.argument('collection_name')
@click.option('--sample-size', '-s', default=10, type=click.IntRange(1, 1000), show_default=True)
@handle_exceptions
def preview(uri, db, collection_name, sample_size):
    """Preview the inferred schema and TypedDict for a single collection."""
    generator = MongoTypedDictGenerator(mongo_uri=uri, db_name=db, verbose=True)
    generator.connect()
    
    if collection_name not in generator.list_collections():
        click.echo(f"Error: Collection '{collection_name}' not found in database '{db}'.", err=True)
        sys.exit(1)

    result = generator.generate_preview_for_collection(collection_name, sample_size)
    generator.disconnect()

    if not result:
        click.echo(f"Could not generate a preview for '{collection_name}'. It may be empty.", err=True)
        sys.exit(1)

    click.secho(f"\nSchema preview for '{collection_name}' (sampled {result['docs_sampled']} docs):", bold=True)
    for field, info in sorted(result['schema'].items()):
        status = "optional" if info['is_optional'] else "required"
        types_str = ", ".join(sorted(list(info['types'])))
        click.echo(f"  - {field}:")
        click.echo(f"    Types: {click.style(types_str, fg='cyan')}")
        click.echo(f"    Status: {status}")

    click.secho("\nGenerated TypedDict:", bold=True)
    click.echo(result['typed_dict'])


if __name__ == "__main__":
    cli()
