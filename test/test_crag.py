import pandas as pd
import pytest
import pandas.api.types as ptypes


@pytest.fixture
def extracted_df():
    return pd.read_parquet('data/processed/extracted_df.parquet')

@pytest.fixture
def transformed_df():
    return pd.read_parquet('data/processed/transformed_df.parquet')

@pytest.fixture
def weather_df():
    return pd.read_parquet('data/processed/weather_df.parquet')
 
def test_transform_output_shape_and_type(crag_df, transformed_df):
    assert isinstance(crag_df, pd.DataFrame)
    assert len(crag_df.columns) == 12
    assert len(crag_df.columns) < len(transformed_df.columns)

def test_transform_country_column(crag_df):
    assert all(crag_df['country'] == 'England')

def test_clean_column_types_crag(crag_df):
    string_columns = ['sector_name', 'crag_name', 'county', 'country', 'route_name', 'difficulty_grade', 'safety_grade']
    for col in string_columns:
        assert ptypes.is_string_dtype(crag_df[col])

    assert ptypes.is_integer_dtype(crag_df['routes_count'])
    assert isinstance(crag_df['type'].dtype, pd.CategoricalDtype)
    assert ptypes.is_float_dtype(crag_df['latitude'])
    assert ptypes.is_float_dtype(crag_df['longitude'])

def test_clean_integrity(crag_df, transformed_df, extracted_df):
    assert isinstance(crag_df, pd.DataFrame)
    assert 'grade' not in crag_df.columns
    assert len(transformed_df.columns) > len(crag_df.columns)
    assert crag_df['routes_count'].max() == extracted_df['routes_count'].max()
    assert all(crag_df['country'] == 'England')

def test_no_nulls_crag(crag_df):
    columns = ['longitude', 'latitude']
    for col in columns:
        assert crag_df[col].notnull().all()

def test_crag_name_survived(crag_df):
    assert 'Clints Crag (Wainwrights summit)' in crag_df['crag_name'].values

def test_route_name_survived(crag_df):
    assert 'The Nose' in crag_df['route_name'].values

def test_rocktype_category(crag_df):
    expected_categories = (
            'Gritstone',
            'Limestone',
            'Sandstone (hard)',
            'Granite',
            'Grit (quarried)',
            'Sandstone (soft)',
            'Rhyolite',
            'UNKNOWN',
            'Artificial',
            'Culm',
            'Slate',
            'Greenstone',
            'Volcanic tuff',
            'Dolerite',
            'Andesite',
            'Gabbro',
            'Killas slate',
            'Mica schist',
            'Shale',
            'Pillow lava',
            'Conglomerate',
            'Chalk',
            'Schist',
            'Amphibiolite & S',
            'Welded Tuff',
            'Quartzite',
            'Crumbly rubbish',
            'Hornstone',
            'Basalt',
            'Diorites',
            'Welsh igneous',
            'Ice',
            'Serpentine',
            'Iron Rock',
            'Ignimbrite',
            'Microgranite',
            'Psammite',
            'UNKNOWN'
            )
    actual_categories = set(crag_df['rocktype'].cat.categories)
    assert actual_categories == set(expected_categories), f"Expected categories: {expected_categories}, but got: {actual_categories}"

def test_type_category(crag_df):
    expected_categories = (
            'Bouldering',
            'Trad',
            'Sport',
            'Top Rope',
            'Winter',
            'DWS',
            'Scrambling',
            'Mixed',
            'Boulder Circuit',
            'Aid',
            'Ice',
            'Alpine',
            'Via Ferrata',
            )
    actual_categories = set(crag_df['type'].cat.categories)
    assert actual_categories == set(expected_categories), f"Expected categories: {expected_categories}, but got: {actual_categories}"

def test_crag_column_present(crag_df):
    expected_columns = [
        'sector_name', 'crag_name', 'county', 'country', 'rocktype',
        'latitude', 'longitude', 'routes_count', 'route_name', 'type',
        'difficulty_grade', 'safety_grade'
    ]
    for col in expected_columns:
        assert col in crag_df.columns, f"Column {col} is missing from crag_df"

def test_route_count_reasonable(crag_df):
    assert crag_df['routes_count'].min() >= 0 
    assert crag_df['routes_count'].max() <= 1500

def test_crag_df_row_count(crag_df):
    assert len(crag_df) == 138754, f"Expected 138754 rows, but got {len(crag_df)}"



