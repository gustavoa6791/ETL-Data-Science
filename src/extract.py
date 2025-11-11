import pandas as pd

def _execute_query(query, table_name, engine):
    """Función auxiliar para ejecutar una consulta"""
    try:
        print(f"Extrayendo datos de {table_name}...")
        df = pd.read_sql(query, engine)
        print(f"-> Se extrajeron exitosamente {len(df)} filas de {table_name}.")
        return df
    except Exception as e:
        print(f"-> Error al extraer de {table_name}: {e}")
        return None

#==================================================
# Esquema: Person
#==================================================

def extract_person_business_entity(engine):
    """Extrae datos de tabla Person.BusinessEntity"""
    return _execute_query("SELECT * FROM Person.BusinessEntity", "Person.BusinessEntity", engine)

def extract_person_address_type(engine):
    """Extrae datos de tabla Person.AddressType"""
    return _execute_query("SELECT * FROM Person.AddressType", "Person.AddressType", engine)

def extract_person_country_region(engine):
    """Extrae datos de tabla Person.CountryRegion"""
    return _execute_query("SELECT * FROM Person.CountryRegion", "Person.CountryRegion", engine)

def extract_person_state_province(engine):
    """Extrae datos de tabla Person.StateProvince"""
    return _execute_query("SELECT * FROM Person.StateProvince", "Person.StateProvince", engine)

def extract_person_address(engine):
    """Extrae datos de tabla Person.Address"""
    return _execute_query("SELECT AddressID, AddressLine1, AddressLine2, City, StateProvinceID, PostalCode, rowguid, ModifiedDate FROM Person.Address", "Person.Address", engine)

def extract_person_business_entity_address(engine):
    """Extrae datos de tabla Person.BusinessEntityAddress"""
    return _execute_query("SELECT * FROM Person.BusinessEntityAddress", "Person.BusinessEntityAddress", engine)

def extract_person_person(engine):
    """Extrae datos de tabla Person.Person"""
    return _execute_query("SELECT * FROM Person.Person", "Person.Person", engine)

def extract_person_phone_number_type(engine):
    """Extrae datos de tabla Person.PhoneNumberType"""
    return _execute_query("SELECT * FROM Person.PhoneNumberType", "Person.PhoneNumberType", engine)

def extract_person_person_phone(engine):
    """Extrae datos de tabla Person.PersonPhone"""
    return _execute_query("SELECT * FROM Person.PersonPhone", "Person.PersonPhone", engine)

def extract_person_email_address(engine):
    """Extrae datos de tabla Person.EmailAddress"""
    return _execute_query("SELECT * FROM Person.EmailAddress", "Person.EmailAddress", engine)

#==================================================
# Esquema: Production
#==================================================

def extract_production_product_category(engine):
    """Extrae datos de tabla Production.ProductCategory"""
    return _execute_query("SELECT * FROM Production.ProductCategory", "Production.ProductCategory", engine)

def extract_production_product_subcategory(engine):
    """Extrae datos de tabla Production.ProductSubcategory"""
    return _execute_query("SELECT * FROM Production.ProductSubcategory", "Production.ProductSubcategory", engine)

def extract_production_unit_measure(engine):
    """Extrae datos de tabla Production.UnitMeasure"""
    return _execute_query("SELECT * FROM Production.UnitMeasure", "Production.UnitMeasure", engine)

def extract_production_product_model(engine):
    """Extrae datos de tabla Production.ProductModel"""
    return _execute_query("SELECT * FROM Production.ProductModel", "Production.ProductModel", engine)

def extract_production_product_description(engine):
    """Extrae datos de tabla Production.ProductDescription"""
    return _execute_query("SELECT * FROM Production.ProductDescription", "Production.ProductDescription", engine)

def extract_production_culture(engine):
    """Extrae datos de tabla Production.Culture"""
    return _execute_query("SELECT * FROM Production.Culture", "Production.Culture", engine)

def extract_production_product_model_product_description_culture(engine):
    """Extrae datos de tabla Production.ProductModelProductDescriptionCulture"""
    return _execute_query("SELECT * FROM Production.ProductModelProductDescriptionCulture", "Production.ProductModelProductDescriptionCulture", engine)

def extract_production_product(engine):
    """Extrae datos de tabla Production.Product"""
    return _execute_query("SELECT * FROM Production.Product", "Production.Product", engine)

#==================================================
# Esquema: Sales
#==================================================

def extract_sales_currency(engine):
    """Extrae datos de tabla Sales.Currency"""
    return _execute_query("SELECT * FROM Sales.Currency", "Sales.Currency", engine)

def extract_sales_currency_rate(engine):
    """Extrae datos de tabla Sales.CurrencyRate"""
    return _execute_query("SELECT * FROM Sales.CurrencyRate", "Sales.CurrencyRate", engine)

def extract_sales_sales_territory(engine):
    """Extrae datos de tabla Sales.SalesTerritory"""
    return _execute_query("SELECT * FROM Sales.SalesTerritory", "Sales.SalesTerritory", engine)

def extract_sales_customer(engine):
    """Extrae datos de tabla Sales.Customer"""
    return _execute_query("SELECT * FROM Sales.Customer", "Sales.Customer", engine)

def extract_sales_special_offer(engine):
    """Extrae datos de tabla Sales.SpecialOffer"""
    return _execute_query("SELECT * FROM Sales.SpecialOffer", "Sales.SpecialOffer", engine)

def extract_sales_special_offer_product(engine):
    """Extrae datos de tabla Sales.SpecialOfferProduct"""
    return _execute_query("SELECT * FROM Sales.SpecialOfferProduct", "Sales.SpecialOfferProduct", engine)

def extract_sales_sales_order_header(engine):
    """Extrae datos de tabla Sales.SalesOrderHeader"""
    return _execute_query("SELECT * FROM Sales.SalesOrderHeader", "Sales.SalesOrderHeader", engine)

def extract_sales_sales_order_detail(engine):
    """Extrae datos de tabla Sales.SalesOrderDetail"""
    return _execute_query("SELECT * FROM Sales.SalesOrderDetail", "Sales.SalesOrderDetail", engine)

def extract_data_with_query(query: str, engine):
    """Extrae datos de una base de datos usando una consulta SQL personalizada
    """
    try:
        print(f"Ejecutando consulta personalizada: {query[:100]}...")
        df = pd.read_sql(query, engine)
        print(f"-> Se extrajeron exitosamente {len(df)} filas.")
        return df
    except Exception as e:
        print(f"-> Error al extraer datos con consulta personalizada: {e}")
        return None

if __name__ == '__main__':
    from utils import get_db_engine

    source_engine = get_db_engine('SOURCE_DB')
    if source_engine:
        print("--- Probando funcion de extracción ---")
        
        person_df = extract_person_person(source_engine)
        if person_df is not None:
            print("\nSample from Person.Person:")
            print(person_df.head())