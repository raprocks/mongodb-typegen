import pymongo
from pymongo.database import Database
import pymongo.errors
from bson.objectid import ObjectId
from datetime import datetime
from collections import defaultdict
import keyword
import argparse
from typing import Any, Set, Dict, List, Optional, Union

def to_pascal_case(name: str) -> str:
    """Converts a snake_case or kebab-case string to PascalCase for class names."""
    return name.replace('_', ' ').replace('-', ' ').title().replace(' ', '')

class MongoTypedDictGenerator:
    """
    Connects to a MongoDB database, inspects collections, and generates
    Python TypedDict classes representing the data schemas.
    """

    def __init__(self, mongo_uri: str, db_name: str):
        """
        Initializes the generator and connects to the MongoDB database.

        Args:
            mongo_uri: The connection string for the MongoDB instance.
            db_name: The name of the database to inspect.
        """
        try:
            self.client = pymongo.MongoClient(mongo_uri)
            self.db: Database = self.client[db_name]
            # Check connection
            self.client.server_info()
            print(f"Successfully connected to MongoDB server at {mongo_uri}")
            print(f"Using database: '{db_name}'")
        except pymongo.errors.ConnectionFailure as e:
            print(f"Error: Could not connect to MongoDB. Please check your URI.")
            raise e

        self.generated_classes: Dict[str, str] = {}
        self.imports: Set[str] = {
            "from typing import TypedDict, List, Optional, Union, Any",
            "from datetime import datetime",
            "from bson.objectid import ObjectId"
        }

    def _map_value_to_type_str(self, value: Any, field_name: str, collection_name: str) -> str:
        """
        Recursively maps a Python value to its type hint string.
        Handles nested dictionaries by generating new TypedDicts.

        Args:
            value: The value from a MongoDB document.
            field_name: The name of the field, used for naming nested TypedDicts.
            collection_name: The name of the parent collection.

        Returns:
            A string representing the Python type hint (e.g., "str", "List[int]").
        """
        if value is None:
            return "Any"
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
            list_item_types = {t for t in list_item_types if t != "Any"}
            if not list_item_types:
                return "List[Any]"
            if len(list_item_types) == 1:
                return f"List[{list_item_types.pop()}]"
            else:
                return f"List[Union[{', '.join(sorted(list(list_item_types)))}]]"
        if isinstance(value, dict):
            nested_class_name = f"{to_pascal_case(collection_name)}{to_pascal_case(field_name)}"
            if nested_class_name not in self.generated_classes:
                self.generated_classes[nested_class_name] = ""
                nested_schema = self._infer_schema_from_docs([value], collection_name)
                class_def = self._create_typeddict_str(nested_class_name, nested_schema)
                self.generated_classes[nested_class_name] = class_def
            return nested_class_name
        return "Any"

    def _infer_schema_from_docs(self, docs: List[Dict], collection_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Infers a schema from a list of documents.

        Args:
            docs: A list of documents from a collection.
            collection_name: The name of the collection being processed.

        Returns:
            A dictionary representing the schema.
        """
        field_data = defaultdict(lambda: {"types": set(), "count": 0})
        total_docs = len(docs)
        for doc in docs:
            for key, value in doc.items():
                field_data[key]["count"] += 1
                type_str = self._map_value_to_type_str(value, key, collection_name)
                field_data[key]["types"].add(type_str)

        final_schema = {}
        for key, data in field_data.items():
            is_optional = data["count"] < total_docs
            types = data["types"]
            if not types:
                types.add("Any")
            final_schema[key] = {
                "types": types,
                "is_optional": is_optional,
            }
        return final_schema

    def _create_typeddict_str(self, class_name: str, schema: Dict) -> str:
        """
        Creates the string representation of a TypedDict class from a schema
        using the functional syntax to support arbitrary key names.

        Args:
            class_name: The name of the TypedDict class.
            schema: The inferred schema dictionary.

        Returns:
            A string containing the full TypedDict class definition.
        """
        fields = []
        for name, properties in schema.items():
            types = sorted(list(properties["types"]))
            is_optional = properties["is_optional"]
            type_hint = f"Union[{', '.join(types)}]" if len(types) > 1 else types[0]
            if is_optional:
                type_hint = f"Optional[{type_hint}]"
            if name == "_id":
                type_hint = "ObjectId"
            fields.append(f"    {repr(name)}: {type_hint}")

        if not fields:
            return f'{class_name} = TypedDict("{class_name}", {{}})'
            
        fields_str = ",\n".join(fields)
        class_def = f'{class_name} = TypedDict("{class_name}", {{\n{fields_str}\n}})'
        return class_def

    def generate_models(self, sample_size: int = 100) -> str:
        """
        The main method to generate all TypedDict models for the database.

        Args:
            sample_size: The number of documents to sample from each collection.

        Returns:
            A string containing the complete Python file with all generated models.
        """
        print("\nStarting model generation...")
        try:
            collection_names = self.db.list_collection_names()
            print(f"Found {len(collection_names)} collections: {collection_names}")
        except Exception as e:
            print(f"Error listing collections: {e}")
            return ""

        for name in collection_names:
            print(f"\nProcessing collection: '{name}'...")
            try:
                docs = list(self.db[name].find(limit=sample_size))
            except Exception as e:
                print(f"  Could not read from collection '{name}'. Skipping. Error: {e}")
                continue

            if not docs:
                print(f"  Collection '{name}' is empty or could not be read. Skipping.")
                continue
            
            print(f"  Analyzing {len(docs)} documents...")
            class_name = to_pascal_case(name)
            if class_name in self.generated_classes:
                continue

            schema = self._infer_schema_from_docs(docs, name)
            class_def = self._create_typeddict_str(class_name, schema)
            self.generated_classes[class_name] = class_def
            print(f"  Successfully generated schema for '{class_name}'.")
        
        header = "# This file was auto-generated by MongoTypedDictGenerator. Do not edit manually.\n\n"
        imports_str = "\n".join(sorted(list(self.imports))) + "\n\n"
        all_class_defs = "\n\n\n".join(self.generated_classes.values())

        return header + imports_str + all_class_defs

    def close_connection(self):
        """Closes the MongoDB connection."""
        self.client.close()
        print("\nMongoDB connection closed.")

def main():
    """
    Main execution function with command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Generate Python TypedDict models from a MongoDB database schema.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--uri",
        type=str,
        default="mongodb://localhost:27017/",
        help="The connection URI for the MongoDB instance."
    )
    parser.add_argument(
        "--db",
        type=str,
        required=True,
        help="The name of the database to inspect."
    )
    parser.add_argument(
        "-s", "--sample-size",
        type=int,
        default=100,
        help="The number of documents to inspect per collection."
    )
    parser.add_argument(
        "-o", "--output-file",
        type=str,
        default="generated_models.py",
        help="The path to the output file for the generated models."
    )
    parser.add_argument(
        '--setup-dummy-data', 
        action='store_true', 
        help='Set up a dummy database with sample data for demonstration.'
    )

    args = parser.parse_args()

    print("--- MongoDB TypedDict Generator ---")

    if args.setup_dummy_data:
        print("Setting up a dummy database for demonstration purposes...")
        try:
            client = pymongo.MongoClient(args.uri)
            db_name_for_dummy = args.db
            client.drop_database(db_name_for_dummy) # Clean slate
            db = client[db_name_for_dummy]
            db.users.insert_many([
                {"full name": "Alice", "email": "alice@example.com", "age": 30, "is_active": True, "created_at": datetime.now(), "profile": {"bio": "Developer", "website": "https://a.com"}, "tags": ["python"]},
                {"_id": ObjectId(), "full name": "Bob", "email": "bob@example.com", "age": None, "is_active": False, "created_at": datetime.now(), "profile": {"bio": "Scientist"}, "tags": ["pandas"], "company_id": 1}
            ])
            db.products.insert_many([
                {"product name": "Laptop", "price": 1200.50, "in_stock": True},
                {"product name": "Mouse", "price": 25, "in_stock": False, "specs": {"dpi": 1600}},
            ])
            client.close()
            print(f"Dummy data created successfully in database '{db_name_for_dummy}'.")
        except Exception as e:
            print(f"Could not create dummy data. Please ensure MongoDB is running. Error: {e}")
            return # Exit if dummy data setup fails

    generator = None
    try:
        generator = MongoTypedDictGenerator(mongo_uri=args.uri, db_name=args.db)
        generated_code = generator.generate_models(sample_size=args.sample_size)

        if generated_code:
            with open(args.output_file, "w", encoding="utf-8") as f:
                f.write(generated_code)
            print(f"\n✅ All models have been successfully generated and saved to '{args.output_file}'")
        else:
            print("\n⚠️ Model generation finished with no output.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if generator and generator.client:
            generator.close_connection()

if __name__ == "__main__":
    main()