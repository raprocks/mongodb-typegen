import pytest
import sys
from bson.objectid import ObjectId
from datetime import datetime
from mongodb_typegen.cli import to_pascal_case, MongoTypedDictGenerator

@pytest.fixture
def generator(mocker):
    """Provides a MongoTypedDictGenerator instance with a mocked MongoDB client."""
    mock_client = mocker.patch('pymongo.MongoClient')
    mock_client.return_value.server_info.return_value = True  # Mock server_info call
    mock_db = mock_client.return_value.__getitem__.return_value
    mock_db.list_collection_names.return_value = ['users', 'products', 'empty_collection']

    user_docs = [
        {
            '_id': ObjectId(),
            'full name': 'Alice',
            'email': 'alice@example.com',
            'age': 30,
            'is_active': True,
            'profile': {'bio': 'Developer', 'website': 'https://a.com'},
            'tags': ['python', 'mongodb']
        },
        {
            '_id': ObjectId(),
            'full name': 'Bob',
            'email': 'bob@example.com',
            'age': None,
            'is_active': False,
            'profile': {'bio': 'Scientist'},
            'tags': ['pandas'],
            'company_id': 1
        }
    ]

    product_docs = [
        {
            'product name': 'Laptop',
            'price': 1200.50,
            'in_stock': True,
            'specs': {'ram': 16, 'storage': 512}
        },
        {
            'product name': 'Mouse',
            'price': 25,
            'in_stock': False,
            'specs': {'dpi': 1600}
        }
    ]

    mock_users_collection = mocker.MagicMock()
    mock_users_collection.aggregate.return_value = user_docs

    mock_products_collection = mocker.MagicMock()
    mock_products_collection.aggregate.return_value = product_docs

    mock_empty_collection = mocker.MagicMock()
    mock_empty_collection.aggregate.return_value = []

    def getitem_side_effect(name):
        if name == 'users':
            return mock_users_collection
        if name == 'products':
            return mock_products_collection
        if name == 'empty_collection':
            return mock_empty_collection
        return mocker.MagicMock()

    mock_db.__getitem__.side_effect = getitem_side_effect

    gen = MongoTypedDictGenerator(mongo_uri='mongodb://mock', db_name='mock_db')
    gen.client = mock_client
    gen.db = mock_db
    
    # Attach docs to the generator instance for easy access in tests
    gen.user_docs = user_docs
    gen.product_docs = product_docs
    
    return gen


# --- Test Core Functions ---

def test_to_pascal_case():
    """Tests the to_pascal_case function."""
    assert to_pascal_case('snake_case') == 'SnakeCase'
    assert to_pascal_case('kebab-case') == 'KebabCase'
    assert to_pascal_case('alreadyPascal') == 'Alreadypascal'
    assert to_pascal_case('with spaces') == 'WithSpaces'


# --- Test Schema Inference and Type Mapping ---

def test_map_value_to_type_str(generator):
    """Tests the _map_value_to_type_str method."""
    assert generator._map_value_to_type_str('hello', 'f', 'c') == 'str'
    assert generator._map_value_to_type_str(123, 'f', 'c') == 'int'
    assert generator._map_value_to_type_str(123.45, 'f', 'c') == 'float'
    assert generator._map_value_to_type_str(True, 'f', 'c') == 'bool'
    assert generator._map_value_to_type_str(datetime.now(), 'f', 'c') == 'datetime'
    assert generator._map_value_to_type_str(ObjectId(), 'f', 'c') == 'ObjectId'
    assert generator._map_value_to_type_str(None, 'f', 'c') == 'Any'
    assert generator._map_value_to_type_str([], 'f', 'c') == 'List[Any]'
    assert generator._map_value_to_type_str([1, 2], 'f', 'c') == 'List[int]'
    assert generator._map_value_to_type_str([1, 'a'], 'f', 'c') == 'List[Union[int, str]]'


def test_infer_schema_from_docs(generator):
    """Tests the _infer_schema_from_docs method."""
    docs = generator.user_docs
    schema = generator._infer_schema_from_docs(docs, 'users')

    assert 'full name' in schema
    assert schema['full name']['types'] == {'str'}
    assert not schema['full name']['is_optional']

    assert 'age' in schema
    assert schema['age']['types'] == {'int', 'Any'}  # From 30 and None
    assert not schema['age']['is_optional'] # Present in all docs

    assert 'company_id' in schema
    assert schema['company_id']['types'] == {'int'}
    assert schema['company_id']['is_optional'] # Missing from one doc


# --- Test TypedDict String Generation ---

def test_create_typeddict_str(generator):
    """Tests the _create_typeddict_str method."""
    schema = {
        'name': {'types': {'str'}, 'is_optional': False},
        'age': {'types': {'int'}, 'is_optional': True},
        'tags': {'types': {'List[str]'}, 'is_optional': False}
    }
    class_str = generator._create_typeddict_str('TestClass', schema)

    # Fields should be sorted alphabetically
    expected_str = (
        'class TestClass(TypedDict):\n'
        '    age: NotRequired[int]\n'
        '    name: str\n'
        '    tags: List[str]'
    )


# --- Test CLI Commands ---
from click.testing import CliRunner
from mongodb_typegen.cli import cli

def test_generate_command_dry_run(mocker):
    """Tests the generate command with --dry-run."""
    runner = CliRunner()
    
    # Mock the generator and its methods
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.connect.return_value = None
    mock_generator.disconnect.return_value = None
    mock_generator.list_collections.return_value = ['users', 'products']
    mock_generator.generate_models_for_collections.return_value = "class Users(TypedDict):\n    ..."
    mock_generator.generated_classes = {'Users': 'class Users...'}

    result = runner.invoke(cli, [
        'generate',
        '--db', 'testdb',
        '--dry-run'
    ])

    assert result.exit_code == 0, result.output
    assert "--- Generated Code (Dry Run) ---" in result.output
    assert "class Users(TypedDict):" in result.output


def test_generate_command_connection_error(mocker):
    """Tests the generate command with a connection error."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.connect.side_effect = ConnectionError("Mocked connection error")

    result = runner.invoke(cli, [
        'generate',
        '--db', 'testdb'
    ])

    assert result.exit_code == 1
    assert "Error: Mocked connection error" in result.output


def test_generate_command_no_collections(mocker):
    """Tests the generate command with no collections found."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.list_collections.return_value = []

    result = runner.invoke(cli, [
        'generate',
        '--db', 'testdb'
    ])

    assert result.exit_code == 1
    assert "No collections to process." in result.output


def test_generate_command_exclude_collections(mocker):
    """Tests the generate command with excluded collections."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.list_collections.return_value = ['users', 'products', 'orders']
    mock_generator.generate_models_for_collections.return_value = "..."
    mock_generator.generated_classes = {'Users': 'class Users...'}

    result = runner.invoke(cli, [
        'generate',
        '--db', 'testdb',
        '--exclude', 'products,orders',
        '--dry-run'
    ])

    assert result.exit_code == 0
    mock_generator.generate_models_for_collections.assert_called_once_with(['users'], 100)


def test_generate_command_file_output(mocker):
    """Tests the generate command writing to a file."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.connect.return_value = None
    mock_generator.disconnect.return_value = None
    mock_generator.list_collections.return_value = ['users']
    mock_generator.generate_models_for_collections.return_value = "class Users(TypedDict):\n    ..."
    mock_generator.generated_classes = {'Users': 'class Users...'}

    mock_open = mocker.patch('builtins.open', mocker.mock_open())
    mocker.patch('os.path.exists', return_value=False)
    mocker.patch('pathlib.Path.mkdir')

    result = runner.invoke(cli, [
        'generate',
        '--db', 'testdb',
        '--out', 'models.py'
    ])

    assert result.exit_code == 0, result.output
    assert "Models successfully generated and saved to 'models.py'" in result.output
    mock_open.assert_called_with('models.py', 'w', encoding='utf-8')
    mock_open().write.assert_called_once_with("class Users(TypedDict):\n    ...")


def test_list_collections_command(mocker):
    """Tests the list-collections command."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.connect.return_value = None
    mock_generator.disconnect.return_value = None
    mock_generator.list_collections.return_value = ['users', 'products']

    result = runner.invoke(cli, [
        'list-collections',
        '--db', 'testdb'
    ])

    assert result.exit_code == 0
    assert "Collections in database 'testdb':" in result.output
    assert "1. users" in result.output
    assert "2. products" in result.output


def test_preview_command(mocker):
    """Tests the preview command."""
    runner = CliRunner()
    
    mock_generator_init = mocker.patch('mongodb_typegen.cli.MongoTypedDictGenerator')
    mock_generator = mock_generator_init.return_value
    mock_generator.connect.return_value = None
    mock_generator.disconnect.return_value = None
    mock_generator.list_collections.return_value = ['users']
    mock_generator.generate_preview_for_collection.return_value = {
        "schema": {"name": {"types": {"str"}, "is_optional": False}},
        "typed_dict": "class Users(TypedDict):\n    name: str",
        "docs_sampled": 5
    }

    result = runner.invoke(cli, [
        'preview',
        '--db', 'testdb',
        'users'
    ])

    assert result.exit_code == 0
    assert "Schema preview for 'users'" in result.output
    assert "Generated TypedDict:" in result.output
    assert "class Users(TypedDict):" in result.output