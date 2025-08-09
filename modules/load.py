import duckdb

def load (crag_df, cleaned_weather_df):
    """
    Loads both dataframes into DuckDB and creates a simple OLAP Database.

    Args:

    crag_df (pd.DataFrame) = crag dataframe
    cleaned_weather_database (pd.Dataframe) = weather dataframe

    Returns: SQL database
    """
    try:
        con = duckdb.connect("file.db")
        con.register('crag_df', crag_df)
        con.register('cleaned_weather_df', cleaned_weather_df)
        con.sql('''
            DROP TABLE IF EXISTS fact_hourlyrouteweather;
            DROP TABLE IF EXISTS dimRoutes;
            DROP TABLE IF EXISTS dimHourlyWeatherInfo;
            DROP TYPE IF EXISTS rocktype;
            DROP TYPE IF EXISTS type;

            CREATE TYPE type AS ENUM (
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
            'Via Ferrata'
            );
                
        
            CREATE TYPE rocktype AS ENUM (
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
            'Psammite'
            );
            
            
            CREATE TABLE dimHourlyWeatherInfo (
                weather_id int,
                date TIMESTAMP,
                precipitation_percentage int,
                temperature_c FLOAT,
                longitude FLOAT,
                latitude FLOAT,
                relative_humidity_percentage int,
                PRIMARY KEY (weather_id)
            );
        
            INSERT INTO dimHourlyWeatherInfo (weather_id, date, precipitation_percentage, temperature_c, longitude, latitude, relative_humidity_percentage)
            SELECT 
                ROW_NUMBER() OVER ()::int, 
                date, 
                precipitation_percentage, 
                temperature_c, 
                longitude, 
                latitude, 
                relative_humidity_percentage
            FROM cleaned_weather_df;

            CREATE TABLE dimRoutes (
                route_id int,
                route_name VARCHAR,
                climbing_type type,
                safety_grade VARCHAR,
                climbing_grade VARCHAR,
                sector_name VARCHAR,
                rocktype rocktype,
                longitude FLOAT,
                latitude FLOAT,
                route_count int,
                country VARCHAR,
                county VARCHAR,
                PRIMARY KEY (route_id)
            );
        
            INSERT INTO dimRoutes (
                route_id,
                route_name,
                climbing_type,
                safety_grade,
                climbing_grade,
                sector_name,
                rocktype,
                longitude,
                latitude,
                route_count,
                country,
                county
            )
            SELECT 
                ROW_NUMBER() OVER ()::int, 
                route_name, 
                type, 
                safety_grade, 
                difficulty_grade, 
                sector_name, 
                rocktype, 
                longitude, 
                latitude, 
                routes_count, 
                country, 
                county
            FROM crag_df;
        
            CREATE TABLE fact_hourlyrouteweather (
                route_id INTEGER REFERENCES dimRoutes (route_id),
                weather_id INTEGER REFERENCES dimHourlyWeatherInfo (weather_id),
                date TIMESTAMP,
                relative_humidity_percentage INTEGER,
                temperature_c FLOAT,
                precipitation_percentage INTEGER 
            );
        
            INSERT INTO fact_hourlyrouteweather (
                route_id,
                weather_id,
                date,
                relative_humidity_percentage,
                temperature_c,
                precipitation_percentage
            )
            SELECT 
                dimRoutes.route_id,
                dimHourlyWeatherInfo.weather_id,
                dimHourlyWeatherInfo.date,
                dimHourlyWeatherInfo.relative_humidity_percentage,
                dimHourlyWeatherInfo.temperature_c,
                dimHourlyWeatherInfo.precipitation_percentage
            FROM dimHourlyWeatherInfo 
            JOIN dimRoutes 
            ON ROUND(dimHourlyWeatherInfo.latitude,4) = ROUND(dimRoutes.latitude,4) AND ROUND(dimHourlyWeatherInfo.longitude,4) = ROUND(dimRoutes.longitude,4);
        ''')
        result = con.sql("SELECT * FROM fact_hourlyrouteweather LIMIT 5").fetchdf()

        print("\n Tables in the DuckDB database:")
        print(con.sql("SHOW TABLES").fetchdf())

        
        print("\n Schema of fact_hourlyrouteweather:")
        print(con.sql("DESCRIBE fact_hourlyrouteweather").fetchdf())
        
        con.close()
        print ("Data sucessfully loaded to DuckDB")
        return result
    
    except Exception as e:
        con.close()
        print (f"Something went wrong with the loading, error:{e}")
        return None
    
    finally:
        con.close()
        print("Connection to DuckDB closed.")   
