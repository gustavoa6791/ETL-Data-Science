import pandas as pd

def transform_customer_data(person_df, customer_df, address_df, email_df):
    """transformación para DimCustomer"""
    print("Transformando datos de clientes...")
    
    # Combinar datos de persona y cliente
    merged_df = pd.merge(customer_df, person_df, left_on='PersonID', right_on='BusinessEntityID', how='inner')
    
    # Combinar con email
    merged_df = pd.merge(merged_df, email_df, on='BusinessEntityID', how='left')
    
    # Añadir clave
    merged_df.reset_index(inplace=True)
    merged_df = merged_df.rename(columns={'index': 'CustomerKey'})
    merged_df['CustomerKey'] = merged_df['CustomerKey'] + 1
    
    dim_customer = pd.DataFrame({
        'CustomerKey': merged_df['CustomerKey'],
        'CustomerID': merged_df['CustomerID'],
        'CustomerAlternateKey': merged_df['AccountNumber'],
        'FirstName': merged_df['FirstName'],
        'LastName': merged_df['LastName'],
        'EmailAddress': merged_df['EmailAddress']
    })
    
    print("Transformación de datos de clientes completada.")
    return dim_customer

def transform_date_data(start_date='2010-01-01', end_date='2014-12-31'):
    """Tabla de dimensión de Fecha (DimDate)"""
    print("Generando datos para DimDate...")
    
    df = pd.DataFrame({"FullDateAlternateKey": pd.to_datetime(pd.date_range(start_date, end_date))})
    
    # Mapeos para nombres internacionales
    day_names_es = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
        'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    day_names_fr = {
        'Monday': 'Lundi', 'Tuesday': 'Mardi', 'Wednesday': 'Mercredi',
        'Thursday': 'Jeudi', 'Friday': 'Vendredi', 'Saturday': 'Samedi', 'Sunday': 'Dimanche'
    }
    month_names_es = {
        'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo', 'April': 'Abril',
        'May': 'Mayo', 'June': 'Junio', 'July': 'Julio', 'August': 'Agosto',
        'September': 'Septiembre', 'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
    }
    month_names_fr = {
        'January': 'Janvier', 'February': 'Février', 'March': 'Mars', 'April': 'Avril',
        'May': 'Mai', 'June': 'Juin', 'July': 'Juillet', 'August': 'Août',
        'September': 'Septembre', 'October': 'Octobre', 'November': 'Novembre', 'December': 'Décembre'
    }

    # Atributos básicos de fecha
    df['DateKey'] = df['FullDateAlternateKey'].dt.strftime('%Y%m%d').astype(int)
    df['DayNumberOfWeek'] = df['FullDateAlternateKey'].dt.dayofweek + 1 # Lunes=1, Domingo=7
    df['EnglishDayNameOfWeek'] = df['FullDateAlternateKey'].dt.day_name()
    df['SpanishDayNameOfWeek'] = df['EnglishDayNameOfWeek'].map(day_names_es)
    df['FrenchDayNameOfWeek'] = df['EnglishDayNameOfWeek'].map(day_names_fr)
    df['DayNumberOfMonth'] = df['FullDateAlternateKey'].dt.day
    df['DayNumberOfYear'] = df['FullDateAlternateKey'].dt.dayofyear
    df['WeekNumberOfYear'] = df['FullDateAlternateKey'].dt.isocalendar().week.astype(int)
    df['EnglishMonthName'] = df['FullDateAlternateKey'].dt.month_name()
    df['SpanishMonthName'] = df['EnglishMonthName'].map(month_names_es)
    df['FrenchMonthName'] = df['EnglishMonthName'].map(month_names_fr)
    df['MonthNumberOfYear'] = df['FullDateAlternateKey'].dt.month
    
    # Atributos de calendario
    df['CalendarQuarter'] = df['FullDateAlternateKey'].dt.quarter
    df['CalendarYear'] = df['FullDateAlternateKey'].dt.year
    df['CalendarSemester'] = (df['CalendarQuarter'] + 1) // 2
    
    # Atributos fiscales
    df['FiscalQuarter'] = ((df['CalendarQuarter'] + 2) % 4) + 1
    df['FiscalYear'] = df['CalendarYear'] + (df['MonthNumberOfYear'] >= 7).astype(int)
    df['FiscalSemester'] = (df['FiscalQuarter'] + 1) // 2

    print(f"Se generaron exitosamente {len(df)} fechas para DimDate.")
    
    column_order = [
        'DateKey', 'FullDateAlternateKey', 'DayNumberOfWeek', 'EnglishDayNameOfWeek',
        'SpanishDayNameOfWeek', 'FrenchDayNameOfWeek', 'DayNumberOfMonth', 'DayNumberOfYear',
        'WeekNumberOfYear', 'EnglishMonthName', 'SpanishMonthName', 'FrenchMonthName',
        'MonthNumberOfYear', 'CalendarQuarter', 'CalendarYear', 'CalendarSemester',
        'FiscalQuarter', 'FiscalYear', 'FiscalSemester'
    ]
    
    return df[column_order]

def transform_product_category_data(df):
    """Transforma los datos de categoría de productos para DimProductCategory."""
    print("Transformando datos de categoría de productos...")
    if df is None:
        return None
    
    renamed_df = df.rename(columns={
        'ProductCategoryID': 'ProductCategoryAlternateKey',
        'Name': 'EnglishProductCategoryName'
    })
    
    # Añadir clave
    renamed_df.reset_index(inplace=True)
    renamed_df = renamed_df.rename(columns={'index': 'ProductCategoryKey'})
    renamed_df['ProductCategoryKey'] = renamed_df['ProductCategoryKey'] + 1
    
    # Reordenar
    dim_df = renamed_df[['ProductCategoryKey', 'ProductCategoryAlternateKey', 'EnglishProductCategoryName']]
    
    print("Transformación de categoría de productos completada.")
    return dim_df

def transform_product_subcategory_data(subcategory_df, category_df_from_dw):
    """ Transforma los datos de subcategoría de productos para DimProductSubcategory """
    print("Transformando datos de subcategoría de productos...")
    if subcategory_df is None or category_df_from_dw is None:
        print("Faltan datos para la transformación de subcategoría.")
        return None

    # Combinar dimensión de categoría para obtener la clave
    merged_df = pd.merge(
        subcategory_df,
        category_df_from_dw,
        left_on='ProductCategoryID',
        right_on='ProductCategoryAlternateKey',
        how='inner'
    )

    renamed_df = merged_df.rename(columns={
        'ProductSubcategoryID': 'ProductSubcategoryAlternateKey',
        'Name': 'EnglishProductSubcategoryName'
    })

    # Añadir clave
    renamed_df.reset_index(inplace=True)
    renamed_df = renamed_df.rename(columns={'index': 'ProductSubcategoryKey'})
    renamed_df['ProductSubcategoryKey'] = renamed_df['ProductSubcategoryKey'] + 1

    # Reordenar
    dim_df = renamed_df[['ProductSubcategoryKey', 'ProductSubcategoryAlternateKey', 'EnglishProductSubcategoryName', 'ProductCategoryKey']]
    
    print("Transformación de subcategoría de productos completada.")
    return dim_df

def transform_product_data(product_df, model_df, pmpdc_df, desc_df, subcategory_dim_df):
    """Transforma datos para construir la dimensión DimProduct."""
    print("Transformando datos de productos...")

    # Descripciones en inglés
    desc_joined = pd.merge(pmpdc_df, desc_df, on='ProductDescriptionID')
    eng_desc = desc_joined[desc_joined['CultureID'].str.strip() == 'en']
    eng_desc = eng_desc[['ProductModelID', 'Description']].rename(columns={'Description': 'EnglishDescription'})

    # Combinaciones principales
    df = product_df.copy()
    df = pd.merge(df, subcategory_dim_df, left_on='ProductSubcategoryID', right_on='ProductSubcategoryAlternateKey', how='left')
    df = pd.merge(df, model_df[['ProductModelID', 'Name']].rename(columns={'Name': 'ModelName'}), on='ProductModelID', how='left')
    df = pd.merge(df, eng_desc, on='ProductModelID', how='left')

    # Añadir clave
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'ProductKey'})
    df['ProductKey'] = df['ProductKey'] + 1

    # Mapeo y creación de columnas
    df = df.rename(columns={'ProductNumber': 'ProductAlternateKey', 'Name': 'EnglishProductName', 'ProductID': 'ProductID_Origen'})
    df['SpanishProductName'] = df['EnglishProductName']
    df['FrenchProductName'] = df['EnglishProductName']
    df['FrenchDescription'] = df['EnglishDescription']
    for lang in ['Chinese', 'Arabic', 'Hebrew', 'Thai', 'German', 'Japanese', 'Turkish']:
        df[f'{lang}Description'] = None
    df['StartDate'] = pd.to_datetime(df['SellStartDate'])
    df['EndDate'] = pd.to_datetime(df['SellEndDate'])
    df['Status'] = 'Current'
    df.loc[df['EndDate'].notna() & (df['EndDate'] < pd.to_datetime('today')), 'Status'] = 'Discontinued'
    df.loc[df['StartDate'] > pd.to_datetime('today'), 'Status'] = 'Future'

    # Seleccionar columnas finales
    final_columns = [
        'ProductKey', 'ProductAlternateKey', 'ProductID_Origen', 'ProductSubcategoryKey', 'WeightUnitMeasureCode', 'SizeUnitMeasureCode', 
        'EnglishProductName', 'SpanishProductName', 'FrenchProductName', 'StandardCost', 'FinishedGoodsFlag', 
        'Color', 'SafetyStockLevel', 'ReorderPoint', 'ListPrice', 'Size', 'SizeRange', 'Weight', 
        'DaysToManufacture', 'ProductLine', 'DealerPrice', 'Class', 'Style', 'ModelName', 'LargePhoto',
        'EnglishDescription', 'FrenchDescription', 'ChineseDescription', 'ArabicDescription', 'HebrewDescription', 
        'ThaiDescription', 'GermanDescription', 'JapaneseDescription', 'TurkishDescription', 'StartDate', 'EndDate', 'Status'
    ]
    for col in final_columns:
        if col not in df.columns:
            df[col] = None
    
    # Rellenar valores NaN de las combinaciones izquierdas
    df['Color'] = df['Color'].fillna('N/A')
    df['ModelName'] = df['ModelName'].fillna('N/A')
    df['EnglishDescription'] = df['EnglishDescription'].fillna('N/A')
    df['FrenchDescription'] = df['FrenchDescription'].fillna('N/A')

    print("Transformación de productos completada.")
    return df[final_columns]

def transform_promotion_data(df):
    """Transforma los datos de promoción para DimPromotion."""
    print("Transformando datos de promoción...")
    if df is None: return None
    
    dim_df = df.rename(columns={
        'SpecialOfferID': 'PromotionAlternateKey',
        'Description': 'EnglishPromotionName',
        'Type': 'EnglishPromotionType',
        'Category': 'EnglishPromotionCategory'
    })
    
    # Añadir clave
    dim_df.reset_index(inplace=True)
    dim_df = dim_df.rename(columns={'index': 'PromotionKey'})
    dim_df['PromotionKey'] = dim_df['PromotionKey'] + 1
    
    dim_df['SpanishPromotionName'] = dim_df['EnglishPromotionName']
    dim_df['FrenchPromotionName'] = dim_df['EnglishPromotionName']
    dim_df['SpanishPromotionType'] = dim_df['EnglishPromotionType']
    dim_df['FrenchPromotionType'] = dim_df['EnglishPromotionType']
    dim_df['SpanishPromotionCategory'] = dim_df['EnglishPromotionCategory']
    dim_df['FrenchPromotionCategory'] = dim_df['EnglishPromotionCategory']
    
    return dim_df[['PromotionKey', 'PromotionAlternateKey', 'EnglishPromotionName', 'SpanishPromotionName', 'FrenchPromotionName', 
                   'DiscountPct', 'EnglishPromotionType', 'SpanishPromotionType', 'FrenchPromotionType', 
                   'EnglishPromotionCategory', 'SpanishPromotionCategory', 'FrenchPromotionCategory', 
                   'StartDate', 'EndDate', 'MinQty', 'MaxQty']]

def transform_sales_territory_data(df):
    """Transforma los datos de territorio de ventas para DimSalesTerritory."""
    print("Transformando datos de territorio de ventas...")
    if df is None: return None
    
    # Añadir clave
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'SalesTerritoryKey'})
    df['SalesTerritoryKey'] = df['SalesTerritoryKey'] + 1
 
    # Renombrar y seleccionar columnas
    dim_df = df.rename(columns={
        'TerritoryID': 'SalesTerritoryAlternateKey',
        'Name': 'SalesTerritoryRegion',
        'CountryRegionCode': 'SalesTerritoryCountry',
        'Group': 'SalesTerritoryGroup'
    })
    return dim_df[['SalesTerritoryKey', 'SalesTerritoryAlternateKey', 'SalesTerritoryRegion', 'SalesTerritoryCountry', 'SalesTerritoryGroup']]

def transform_currency_data(df):
    """Transforma los datos de moneda para DimCurrency."""
    print("Transformando datos de moneda...")
    if df is None: return None
    
    # Añadir clave
    df.reset_index(inplace=True)
    df = df.rename(columns={'index': 'CurrencyKey'})
    df['CurrencyKey'] = df['CurrencyKey'] + 1
    
    dim_df = df.rename(columns={
        'CurrencyCode': 'CurrencyAlternateKey',
        'Name': 'CurrencyName'
    })
    return dim_df[['CurrencyKey', 'CurrencyAlternateKey', 'CurrencyName']]

def transform_fact_internet_sales(order_detail_df, order_header_df, dim_product, dim_customer, dim_date, dim_promotion, dim_territory, dim_currency):
    """Construye la tabla FactInternetSales combinando orígenes con dimensiones."""
    print("Transformando datos para FactInternetSales...")
    
    # Filtrar solo para ventas por internet
    if 'OnlineOrderFlag' in order_header_df.columns:
        order_header_df = order_header_df[order_header_df['OnlineOrderFlag'] == 1].copy()

    # Combinar orígenes transaccionales
    facts_df = pd.merge(order_detail_df, order_header_df, on='SalesOrderID', how='inner')

    # Buscar claves de dimensión
    # Producto
    facts_df = pd.merge(facts_df, dim_product[['ProductKey', 'ProductID_Origen']], left_on='ProductID', right_on='ProductID_Origen', how='left')
    # Cliente
    facts_df = pd.merge(facts_df, dim_customer[['CustomerKey', 'CustomerID']], left_on='CustomerID', right_on='CustomerID', how='left')
    # Promoción
    facts_df = pd.merge(facts_df, dim_promotion[['PromotionKey', 'PromotionAlternateKey']], left_on='SpecialOfferID', right_on='PromotionAlternateKey', how='left')
    # Territorio
    facts_df = pd.merge(facts_df, dim_territory[['SalesTerritoryKey', 'SalesTerritoryAlternateKey']], left_on='TerritoryID', right_on='SalesTerritoryAlternateKey', how='left')
    # Moneda
    facts_df['CurrencyKey'] = 1 # USD

    # Claves de fecha
    dim_date_keys = dim_date[['DateKey', 'FullDateAlternateKey']]
    facts_df['OrderDate_Date'] = facts_df['OrderDate'].dt.normalize()
    facts_df['DueDate_Date'] = facts_df['DueDate'].dt.normalize()
    facts_df['ShipDate_Date'] = facts_df['ShipDate'].dt.normalize()

    facts_df = pd.merge(facts_df, dim_date_keys.rename(columns={'DateKey': 'OrderDateKey'}), left_on='OrderDate_Date', right_on='FullDateAlternateKey', how='left')
    facts_df = pd.merge(facts_df, dim_date_keys.rename(columns={'DateKey': 'DueDateKey'}), left_on='DueDate_Date', right_on='FullDateAlternateKey', how='left')
    facts_df = pd.merge(facts_df, dim_date_keys.rename(columns={'DateKey': 'ShipDateKey'}), left_on='ShipDate_Date', right_on='FullDateAlternateKey', how='left')

    # 3. Renombrar y seleccionar columnas finales
    facts_df = facts_df.rename(columns={
        'SalesOrderDetailID': 'SalesOrderLineNumber',
        'PurchaseOrderNumber': 'CustomerPONumber',
        'RevisionNumber': 'RevisionNumber',
        'OrderQty': 'OrderQuantity',
        'UnitPrice': 'UnitPrice',
        'UnitPriceDiscount': 'UnitPriceDiscountPct',
        'LineTotal': 'ExtendedAmount',
        'TaxAmt': 'TaxAmt',
        'Freight': 'Freight'
    })
    

    facts_df['DiscountAmount'] = facts_df['ExtendedAmount'] * facts_df['UnitPriceDiscountPct']
    facts_df['ProductStandardCost'] = 0 # Marcador de posición
    facts_df['TotalProductCost'] = 0 # Marcador de posición
    facts_df['SalesAmount'] = facts_df['ExtendedAmount'] # Simplificación

    # Eliminar filas con fallos
    key_cols = ['ProductKey', 'OrderDateKey', 'DueDateKey', 'ShipDateKey', 'CustomerKey', 'PromotionKey', 'SalesTerritoryKey', 'CurrencyKey']
    facts_df.dropna(subset=key_cols, inplace=True)
    
    # Convertir claves a entero
    for col in key_cols:
        facts_df[col] = facts_df[col].astype(int)

    final_columns = [
        'ProductKey', 'OrderDateKey', 'DueDateKey', 'ShipDateKey', 'CustomerKey', 'PromotionKey', 'CurrencyKey', 'SalesTerritoryKey',
        'SalesOrderNumber', 'SalesOrderLineNumber', 'RevisionNumber', 'OrderQuantity', 'UnitPrice', 'ExtendedAmount',
        'UnitPriceDiscountPct', 'DiscountAmount', 'ProductStandardCost', 'TotalProductCost', 'SalesAmount', 'TaxAmt', 'Freight',
        'CarrierTrackingNumber', 'CustomerPONumber', 'OrderDate', 'DueDate', 'ShipDate'
    ]
    
    return facts_df[final_columns]

if __name__ == '__main__':
    print("Este script contiene funciones de transformación")