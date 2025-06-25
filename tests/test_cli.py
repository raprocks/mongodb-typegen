import pytest
from bson.objectid import ObjectId
from datetime import datetime
from mongodb_typegen.cli import to_pascal_case, MongoTypedDictGenerator

@pytest.fixture
def generator(mocker):
    """Provides a MongoTypedDictGenerator instance with a mocked MongoDB client."""
    # Mock the MongoDB client and its methods
    mock_client = mocker.patch('pymongo.MongoClient')
    mock_db = mock_client.return_value.__getitem__.return_value
    mock_db.list_collection_names.return_value = ['users', 'products']

    # Mock the 'find' method for each collection
    mock_users_collection = mocker.MagicMock()
    mock_users_collection.find.return_value = [
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

    mock_products_collection = mocker.MagicMock()
    mock_products_collection.find.return_value = [
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

    # Configure the mock database to return the correct mock collection
    def getitem_side_effect(name):
        if name == 'users':
            return mock_users_collection
        elif name == 'products':
            return mock_products_collection
        return mocker.MagicMock()

    mock_db.__getitem__.side_effect = getitem_side_effect

    # Initialize the generator with the mocked client
    gen = MongoTypedDictGenerator(mongo_uri='mongodb://mock', db_name='mock_db')
    gen.client = mock_client
    gen.db = mock_db
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
    docs = generator.db['users'].find()
    schema = generator._infer_schema_from_docs(docs, 'users')

    assert 'full name' in schema
    assert schema['full name']['types'] == {'str'}
    assert not schema['full name']['is_optional']

    assert 'age' in schema
    assert schema['age']['types'] == {'int', 'Any'}
    assert not schema['age']['is_optional']

    assert 'company_id' in schema
    assert schema['company_id']['types'] == {'int'}
    assert schema['company_id']['is_optional']


# --- Test TypedDict String Generation ---

def test_create_typeddict_str(generator):
    """Tests the _create_typeddict_str method."""
    schema = {
        'name': {'types': {'str'}, 'is_optional': False},
        'age': {'types': {'int'}, 'is_optional': True},
        'tags': {'types': {'List[str]'}, 'is_optional': False}
    }
    class_str = generator._create_typeddict_str('TestClass', schema)

    expected_str = (
        'TestClass = TypedDict("TestClass", {\n'
        '    \'name\': str,\n'
        '    \'age\': Optional[int],\n'
        '    \'tags\': List[str]\n'
        '})'
    )
    assert class_str == expected_str


# --- Test Main Generation Logic ---

def test_generate_models(generator):
    """Tests the main generate_models method."""
    generated_code = generator.generate_models()

    # Check that the main classes are generated
    assert 'Users = TypedDict("Users", {' in generated_code
    assert 'Products = TypedDict("Products", {' in generated_code

    # Check for a nested class
    assert 'UsersProfile = TypedDict("UsersProfile", {' in generated_code

    # Check for correct field definitions
    assert "'full name': str" in generated_code
    assert "'age': Union[Any, int]" in generated_code
    assert "'company_id': Optional[int]" in generated_code
    assert "'profile': UsersProfile" in generated_code
    assert "'product name': str" in generated_code
