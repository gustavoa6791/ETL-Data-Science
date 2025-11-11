def load_data(df, table_name, engine, if_exists='append', schema='dbo'):
    """
    Carga un DataFrame en una tabla específica en la base de datos de destino.

    Argumentos:
        df (pd.DataFrame): El DataFrame a cargar.
        table_name (str): El nombre de la tabla de destino.
        engine (sqlalchemy.engine.Engine): El motor SQLAlchemy para la base de datos de destino.
        if_exists (str): Cómo comportarse si la tabla ya existe.
                         'fail', 'replace', o 'append'. Por defecto es 'append'.
        schema (str): El esquema de la base de datos donde se cargarán los datos. Por defecto es 'dbo'.
    """
    try:
        print(f"Cargando datos en la tabla: {schema}.{table_name}...")
        df.to_sql(
            name=table_name, 
            con=engine, 
            schema=schema,
            if_exists=if_exists, 
            index=False, 
            chunksize=1000
        )
        print(f"Se cargaron exitosamente {len(df)} filas en {schema}.{table_name}.")
    except Exception as e:
        print(f"Error al cargar datos en {schema}.{table_name}: {e}")

if __name__ == '__main__':
    print("Este script contiene la función de carga de datos")