import great_expectations as gx
import pandas as pd
import json


# Importing the data into Great Expectations
cleaned_weather_df = pd.read_csv('Working_Code/cleaned_weather_df.csv', index_col=[0], parse_dates=['date'])
crag_df = pd.read_csv('Working_Code/crag_df.csv', index_col=[0])

# Setting data types for crag DataFrame
crag_df['sector_name'] = crag_df['sector_name'].astype('string')
crag_df['crag_name'] = crag_df['crag_name'].astype('string')
crag_df['county'] = crag_df['county'].astype('string')
crag_df['country'] = crag_df['country'].astype('string')
crag_df['rocktype'] = crag_df['rocktype'].astype('category')
crag_df['latitude'] = crag_df['latitude'].astype('float64')
crag_df['longitude'] = crag_df['longitude'].astype('float64')
crag_df['routes_count'] = crag_df['routes_count'].astype('int64')
crag_df['route_name'] = crag_df['route_name'].astype('string')
crag_df['type'] = crag_df['type'].astype('category')
crag_df['difficulty_grade'] = crag_df['difficulty_grade'].astype('string')
crag_df['safety_grade'] = crag_df['safety_grade'].astype('category')

# Creating data context
context = gx.get_context()
# Connecting context to relevant Pd.DataFrame
data_source_weather = context.data_sources.add_pandas(name='cleaned_weather_df')
data_source_crag = context.data_sources.add_pandas(name='crag_df')

# Creating data asset
data_asset_weather = data_source_weather.add_dataframe_asset(
    name='data_asset_weather'
)

data_asset_crag = data_source_crag.add_dataframe_asset(
    name='data_asset_crag'
)

# Creating weather & crag batch definition
batch_definition_weather = data_asset_weather.add_batch_definition_whole_dataframe(name="my_batch_definition_weather")

batch_definition_crag = data_asset_crag.add_batch_definition_whole_dataframe(name="my_batch_definition_crag")

# Creating a batch for both weather and crag
batch_weather = batch_definition_weather.get_batch(batch_parameters={"dataframe": cleaned_weather_df})
batch_crag = batch_definition_crag.get_batch(batch_parameters={"dataframe": crag_df})

# Creating expectations for row count and column count

column_count_expectations_weather = gx.expectations.ExpectTableColumnCountToEqual(
    value=6
)

column_count_expectations_crag = gx.expectations.ExpectTableColumnCountToEqual(
    value=12
)

row_count_expectations_weather = gx.expectations.ExpectTableRowCountToEqual(
    value=8400
)

row_count_expectations_crag = gx.expectations.ExpectTableRowCountToEqual(
    value=138754
)

column_country_crag = gx.expectations.ExpectColumnDistinctValuesToEqualSet(
    column='country', value_set={'England'}
)


column_routes_notnull_crag = gx.expectations.ExpectColumnValuesToNotBeNull(
    column='routes_count'
)

column_crag_notnull_crag = gx.expectations.ExpectColumnValuesToNotBeNull(
    column='crag_name'
)

column_longitude_notnull_crag = gx.expectations.ExpectColumnValuesToNotBeNull(
    column='longitude'
)

column_latitude_notnull_crag = gx.expectations.ExpectColumnValuesToNotBeNull(
    column='latitude'
)

expected_cols_weather = ['date', 'temperature_c', 'relative_humidity_percentage', 'precipitation_percentage', 'longitude', 'latitude']

expected_cols_crag = ['sector_name', 'crag_name', 'county', 'country', 'rocktype', 'latitude', 'longitude', 'routes_count', 'route_name', 'type', 'difficulty_grade', 'safety_grade']

cols_name_expectations_weather = gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected_cols_weather)

cols_name_expectations_crag = gx.expectations.ExpectTableColumnsToMatchSet(column_set=expected_cols_crag)

type_expectations_weather_date = gx.expectations.ExpectColumnValuesToBeOfType(column='date', type_="Timestamp") 

type_expectations_weather_temperature = gx.expectations.ExpectColumnValuesToBeOfType(column='temperature_c', type_="float64") 

type_expectations_weather_humidity = gx.expectations.ExpectColumnValuesToBeOfType(column='relative_humidity_percentage', type_="float64") 

type_expectations_weather_precipitation = gx.expectations.ExpectColumnValuesToBeOfType(column='precipitation_percentage', type_="float64") 

type_expectations_weather_longitude = gx.expectations.ExpectColumnValuesToBeOfType(column='longitude', type_="float64") 

type_expectations_weather_latitude = gx.expectations.ExpectColumnValuesToBeOfType(column='latitude', type_="float64") 

crag_cols_str = ['sector_name', 'crag_name', 'county', 'country', 'route_name', 'difficulty_grade']

crag_cols_category = ['rocktype', 'safety_grade']

crags_cols_float = ['longitude', 'latitude']

type_expectations_crag_int = gx.expectations.ExpectColumnValuesToBeOfType(column='routes_count', type_="int64")

type_expectations_crag_str = [gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="str") for col in crag_cols_str]

type_expectations_crag_category = [gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="CategoricalDtypeType") for col in crag_cols_category]

type_expectations_crag_float = [gx.expectations.ExpectColumnValuesToBeOfType(column=col, type_="float") for col in crags_cols_float]

weather_suite = gx.ExpectationSuite(
    name='weather_suite',
    expectations=[
        column_count_expectations_weather,
        row_count_expectations_weather,
        cols_name_expectations_weather,
        type_expectations_weather_date,
        type_expectations_weather_temperature,
        type_expectations_weather_humidity,
        type_expectations_weather_precipitation,
        type_expectations_weather_longitude,
        type_expectations_weather_latitude
    ]
)

crag_suite = gx.ExpectationSuite(
    name='crag_suite',
    expectations=[
        column_count_expectations_crag,
        row_count_expectations_crag,
        column_country_crag,
        column_routes_notnull_crag,
        column_crag_notnull_crag,
        column_longitude_notnull_crag,
        column_latitude_notnull_crag,
        cols_name_expectations_crag,
        type_expectations_crag_int,
        *type_expectations_crag_str,  
        *type_expectations_crag_category, 
        *type_expectations_crag_float  
    ]
)

weather_validation = gx.ValidationDefinition(
    name = 'weather_validation',
    data = batch_definition_weather,
    suite = weather_suite
)

crag_validation = gx.ValidationDefinition(
    name = 'crag_validation',
    data = batch_definition_crag,
    suite = crag_suite
)

crag_suite = context.suites.add(
    suite=crag_suite
)

weather_suite = context.suites.add(
    suite=weather_suite
)

weather_validation_results = weather_validation.run(
    batch_parameters={"dataframe":cleaned_weather_df}
)

crag_validation_results = crag_validation.run(
    batch_parameters={"dataframe":crag_df}
)



