from src.utils import get_db_engine
from src.load import load_data
from src.transform import (
    transform_customer_data, 
    transform_date_data,
    transform_product_category_data,
    transform_product_subcategory_data,
    transform_product_data,
    transform_promotion_data,
    transform_sales_territory_data,
    transform_currency_data,
    transform_fact_internet_sales
)
from src.extract import *

def etl_dim_date(dest_engine):
    print("\n--- Procesando DimDate ---")
    #trasform
    dim_date_df = transform_date_data(start_date='2010-01-01', end_date='2014-12-31')
    #load
    if dim_date_df is not None:
        load_data(dim_date_df, 'DimDate', dest_engine, if_exists='replace')
    print("--- Proceso de DimDate completado. ---")


def etl_dim_product_category(source_engine, dest_engine):
    print("\n--- Procesando DimProductCategory ---")
    #extract
    category_df = extract_production_product_category(source_engine)
    #transform
    dim_category_df = transform_product_category_data(category_df)
    #load
    if dim_category_df is not None:
        load_data(dim_category_df, 'DimProductCategory', dest_engine, if_exists='replace')
    print("--- Proceso de DimProductCategory completado. ---")


def etl_dim_product_subcategory(source_engine, dest_engine):
    print("\n--- Procesando DimProductSubcategory ---")
    #extract
    subcategory_df = extract_production_product_subcategory(source_engine)
    dim_category_df = extract_data_with_query("SELECT ProductCategoryKey, ProductCategoryAlternateKey FROM dbo.DimProductCategory", dest_engine)
    #transform
    dim_subcategory_df = transform_product_subcategory_data(subcategory_df, dim_category_df)
    #load
    if dim_subcategory_df is not None:
        load_data(dim_subcategory_df, 'DimProductSubcategory', dest_engine, if_exists='replace')
    print("--- Proceso de DimProductSubcategory completado. ---")


def etl_dim_product(source_engine, dest_engine):
    print("\n--- Procesando DimProduct ---")
    #extract
    product_df = extract_production_product(source_engine)
    model_df = extract_production_product_model(source_engine)
    pmpdc_df = extract_production_product_model_product_description_culture(source_engine)
    desc_df = extract_production_product_description(source_engine)
    subcategory_dim_df = extract_data_with_query("SELECT ProductSubcategoryKey, ProductSubcategoryAlternateKey FROM dbo.DimProductSubcategory", dest_engine)
    #transform
    dim_product_df = transform_product_data(product_df, model_df, pmpdc_df, desc_df, subcategory_dim_df)
    #load
    if dim_product_df is not None:
        load_data(dim_product_df, 'DimProduct', dest_engine, if_exists='replace')
    print("--- Proceso de DimProduct completado. ---")


def etl_dim_customer(source_engine, dest_engine):
    print("\n--- Procesando DimCustomer ---")
    #extract
    person_df = extract_person_person(source_engine)
    customer_df = extract_sales_customer(source_engine)
    address_df = extract_person_address(source_engine)
    email_df = extract_person_email_address(source_engine)
    if any(df is None for df in [person_df, customer_df, address_df, email_df]):
        print("Fallo en la extracción para DimCustomer")
        return
    #transform
    dim_customer_df = transform_customer_data(person_df, customer_df, address_df, email_df)
    #load
    load_data(dim_customer_df, 'DimCustomer', dest_engine, if_exists='replace')
    print("--- Proceso de DimCustomer completado. ---")


def etl_dim_promotion(source_engine, dest_engine):
    """Ejecuta el ETL para la dimensión de Promoción."""
    print("\n--- Procesando DimPromotion ---")
    #extract
    special_offer_df = extract_sales_special_offer(source_engine)
    #transform
    dim_promotion_df = transform_promotion_data(special_offer_df)
    #load
    if dim_promotion_df is not None:
        load_data(dim_promotion_df, 'DimPromotion', dest_engine, if_exists='replace')
    print("--- Proceso de DimPromotion completado. ---")


def etl_dim_sales_territory(source_engine, dest_engine):
    """Ejecuta el ETL para la dimensión de Territorio de Ventas."""
    print("\n--- Procesando DimSalesTerritory ---")
    #extract
    territory_df = extract_sales_sales_territory(source_engine)
    #transform
    dim_territory_df = transform_sales_territory_data(territory_df)

    if dim_territory_df is not None:
        load_data(dim_territory_df, 'DimSalesTerritory', dest_engine, if_exists='replace')
    print("--- Proceso de DimSalesTerritory completado. ---")


def etl_dim_currency(source_engine, dest_engine):
    """Ejecuta el ETL para la dimensión de Moneda."""
    print("\n--- Procesando DimCurrency ---")
    #extract
    currency_df = extract_sales_currency(source_engine)
    #transform
    dim_currency_df = transform_currency_data(currency_df)
    #load
    if dim_currency_df is not None:
        load_data(dim_currency_df, 'DimCurrency', dest_engine, if_exists='replace')
    print("--- Proceso de DimCurrency completado. ---")


def etl_fact_internet_sales(source_engine, dest_engine):
    """Ejecuta el ETL para la tabla de hechos de Ventas por Internet."""
    print("\n--- Procesando FactInternetSales ---")
    # 1. Extraer datos transaccionales del origen
    order_detail_df = extract_sales_sales_order_detail(source_engine)
    order_header_df = extract_sales_sales_order_header(source_engine)

    # 2. Extraer todas las dimensiones desde la bodega de datos
    dim_product = extract_data_with_query("SELECT ProductKey, ProductAlternateKey, ProductID_Origen FROM dbo.DimProduct", dest_engine)
    dim_customer = extract_data_with_query("SELECT CustomerKey, CustomerAlternateKey, CustomerID FROM dbo.DimCustomer", dest_engine)
    dim_date = extract_data_with_query("SELECT DateKey, FullDateAlternateKey FROM dbo.DimDate", dest_engine)
    dim_promotion = extract_data_with_query("SELECT PromotionKey, PromotionAlternateKey FROM dbo.DimPromotion", dest_engine)
    dim_territory = extract_data_with_query("SELECT SalesTerritoryKey, SalesTerritoryAlternateKey FROM dbo.DimSalesTerritory", dest_engine)
    dim_currency = extract_data_with_query("SELECT CurrencyKey, CurrencyAlternateKey FROM dbo.DimCurrency", dest_engine)

    # 3. Transformar
    fact_table_df = transform_fact_internet_sales(order_detail_df, order_header_df, dim_product, dim_customer, dim_date, dim_promotion, dim_territory, dim_currency)

    # 4. Cargar
    if fact_table_df is not None:
        load_data(fact_table_df, 'FactInternetSales', dest_engine, if_exists='replace') # Usar 'append' para las tablas de hechos
    print("--- Proceso de FactInternetSales completado. ---")


def main():
    print("Iniciando proceso ETL...")

    source_engine = get_db_engine('SOURCE_DB')
    dest_engine = get_db_engine('DESTINATION_DB')
    if not source_engine or not dest_engine:
        print("Error en la conexión a la base de datos.")
        return
    
    #Crear tabla de dimensiones
    etl_dim_date(dest_engine)
    etl_dim_product_category(source_engine, dest_engine)
    etl_dim_product_subcategory(source_engine, dest_engine)
    etl_dim_product(source_engine, dest_engine)
    etl_dim_customer(source_engine, dest_engine)
    etl_dim_promotion(source_engine, dest_engine)
    etl_dim_sales_territory(source_engine, dest_engine)
    etl_dim_currency(source_engine, dest_engine)
    
    # Crear tabla de hechos
    etl_fact_internet_sales(source_engine, dest_engine)

    print("\n\n¡Proceso ETL completado exitosamente!")

if __name__ == '__main__':
    main()