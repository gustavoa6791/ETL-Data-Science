# Proyecto ETL - AdventureWorks

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para migrar datos desde la base de datos transaccional AdventureWorks2022_TRANS hacia una bodega de datos (BodegaDeDatos).

## Requisitos Previos

- Python 3.8 o superior
- SQL Server con ODBC Driver 17
- Bases de datos:
  - `AdventureWorks2022_TRANS` (origen)
  - `BodegaDeDatos` (destino)

## Configuración del Entorno Virtual

### 1. Crear el entorno virtual

En la raíz del proyecto, ejecuta:

```bash
# En Windows
python -m venv venv

# En macOS/Linux
python3 -m venv venv
```

### 2. Activar el entorno virtual

```bash
# En Windows
venv\Scripts\activate

# En macOS/Linux
source venv/bin/activate
```

Una vez activado, verás `(venv)` al inicio de tu línea de comandos.

### 3. Instalar dependencias

Con el entorno virtual activado, instala las dependencias necesarias:

```bash
pip install -r requirements.txt
```

### 4. Desactivar el entorno virtual

Cuando termines de trabajar:

```bash
deactivate
```

## Configuración de Variables de Conexión a Bases de Datos

### Archivo de Configuración

El proyecto utiliza el archivo `config/config.ini` para almacenar las credenciales y configuraciones de conexión.

### Estructura del archivo config.ini

```ini
[SOURCE_DB]
server = localhost
database = AdventureWorks2022_TRANS
driver = ODBC Driver 17 for SQL Server
username = SA
password = TuPasswordAqui

[DESTINATION_DB]
server = localhost
database = BodegaDeDatos
driver = ODBC Driver 17 for SQL Server
username = SA
password = TuPasswordAqui
```

### Parámetros de Configuración

#### [SOURCE_DB] - Base de Datos Origen

- **server**: Servidor SQL Server (ejemplo: `localhost`, `192.168.1.100`, `servidor.dominio.com`)
- **database**: Nombre de la base de datos de origen (`AdventureWorks2022_TRANS`)
- **driver**: Driver ODBC instalado (comúnmente `ODBC Driver 17 for SQL Server` o `ODBC Driver 18 for SQL Server`)
- **username**: Usuario de SQL Server (ejemplo: `SA`, `admin`)
- **password**: Contraseña del usuario

#### [DESTINATION_DB] - Base de Datos Destino

- **server**: Servidor SQL Server de destino
- **database**: Nombre de la bodega de datos (`BodegaDeDatos`)
- **driver**: Driver ODBC instalado
- **username**: Usuario de SQL Server
- **password**: Contraseña del usuario

### Pasos para Configurar

1. Navega a la carpeta `config/`
2. Abre el archivo `config.ini`
3. Modifica los valores según tu configuración:
   - Reemplaza `localhost` con la dirección de tu servidor
   - Actualiza el nombre de usuario y contraseña
   - Verifica el nombre del driver ODBC instalado en tu sistema

### Verificar Driver ODBC Instalado

```bash
# En Windows (PowerShell)
Get-OdbcDriver | Where-Object {$_.Name -like "*SQL Server*"}

# En macOS/Linux
odbcinst -q -d
```

## Estructura del Proyecto

```
ETL/
├── config/
│   └── config.ini          # Configuración de conexiones
├── src/
│   ├── extract.py          # Funciones de extracción
│   ├── transform.py        # Funciones de transformación
│   ├── load.py             # Funciones de carga
│   └── utils.py            # Utilidades (conexión DB)
├── main.py                 # Script principal ETL
├── requirements.txt        # Dependencias del proyecto
└── README.md              # Este archivo
```

## Ejecución del Proceso ETL

### Verificar conexión

Antes de ejecutar el ETL completo, verifica que las conexiones funcionan correctamente.

### Ejecutar el proceso ETL

Con el entorno virtual activado y las configuraciones correctas:

```bash
python main.py
```

### Flujo del Proceso

El proceso ETL carga los datos en el siguiente orden:

**Dimensiones:**
1. DimDate
2. DimProductCategory
3. DimProductSubcategory
4. DimProduct
5. DimCustomer
6. DimPromotion
7. DimSalesTerritory
8. DimCurrency
9. DimReseller
10. DimEmployee

**Tablas de Hechos:**
1. FactInternetSales
2. FactResellerSales


