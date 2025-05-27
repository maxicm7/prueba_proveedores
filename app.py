import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import sqlite3 # Import the sqlite3 library

# --- Configuración Inicial ---
st.set_page_config(layout="wide", page_title="Gestión de Equipos y Obras (Minería)")

# --- Archivos de Datos (Ahora usaremos una base de datos SQLite) ---
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DATABASE_FILE = os.path.join(DATA_DIR, "app_data.db")

# Define table names for SQLite
TABLE_EQUIPOS = "equipos"
TABLE_CONSUMO = "consumo"
TABLE_COSTOS_SALARIAL = "costos_salarial"
TABLE_GASTOS_FIJOS = "gastos_fijos"
TABLE_GASTOS_MANTENIMIENTO = "gastos_mantenimiento"
TABLE_PRECIOS_COMBUSTIBLE = "precios_combustible"
TABLE_PROYECTOS = "proyectos"
TABLE_PRESUPUESTO_MATERIALES = "presupuesto_materiales"
TABLE_COMPRAS_MATERIALES = "compras_materiales"
TABLE_ASIGNACION_MATERIALES = "asignacion_materiales"


# --- Helper function to get SQLite connection ---
def get_db_conn():
    """Establece y retorna una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(DATABASE_FILE)
    return conn

# --- Define expected columns and their pandas dtypes for each table ---
# This is used to create empty dataframes if tables don't exist and for date handling
# Use 'object' for dates initially, then convert to datetime/date after loading
TABLE_COLUMNS = {
    TABLE_EQUIPOS: {'Interno': 'object', 'Patente': 'object'},
    TABLE_CONSUMO: {'Interno': 'object', 'Fecha': 'object', 'Consumo_Litros': 'float64', 'Horas_Trabajadas': 'float64', 'Kilometros_Recorridos': 'float64'},
    TABLE_COSTOS_SALARIAL: {'Interno': 'object', 'Fecha': 'object', 'Monto_Salarial': 'float64'},
    TABLE_GASTOS_FIJOS: {'Interno': 'object', 'Fecha': 'object', 'Tipo_Gasto_Fijo': 'object', 'Monto_Gasto_Fijo': 'float64', 'Descripcion': 'object'},
    TABLE_GASTOS_MANTENIMIENTO: {'Interno': 'object', 'Fecha': 'object', 'Tipo_Mantenimiento': 'object', 'Monto_Mantenimiento': 'float64', 'Descripcion': 'object'},
    TABLE_PRECIOS_COMBUSTIBLE: {'Fecha': 'object', 'Precio_Litro': 'float64'},
    TABLE_PROYECTOS: {'ID_Obra': 'object', 'Nombre_Obra': 'object', 'Responsable': 'object'},
    TABLE_PRESUPUESTO_MATERIALES: {'ID_Obra': 'object', 'Material': 'object', 'Cantidad_Presupuestada': 'float64', 'Precio_Unitario_Presupuestado': 'float64', 'Costo_Presupuestado': 'float64'},
    TABLE_COMPRAS_MATERIALES: {'ID_Compra': 'object', 'Fecha_Compra': 'object', 'Material': 'object', 'Cantidad_Comprada': 'float64', 'Precio_Unitario_Comprado': 'float64', 'Costo_Compra': 'float64'},
    TABLE_ASIGNACION_MATERIALES: {'ID_Asignacion': 'object', 'Fecha_Asignacion': 'object', 'ID_Obra': 'object', 'Material': 'object', 'Cantidad_Asignada': 'float64', 'Precio_Unitario_Asignado': 'float64', 'Costo_Asignado': 'float64'},
}

# Define which columns are dates and should be converted
DATE_COLUMNS = {
    TABLE_CONSUMO: ['Fecha'],
    TABLE_COSTOS_SALARIAL: ['Fecha'],
    TABLE_GASTOS_FIJOS: ['Fecha'],
    TABLE_GASTOS_MANTENIMIENTO: ['Fecha'],
    TABLE_PRECIOS_COMBUSTIBLE: ['Fecha'],
    TABLE_COMPRAS_MATERIALES: ['Fecha_Compra'],
    TABLE_ASIGNACION_MATERIALES: ['Fecha_Asignacion'],
    # Proyectos and Equipos tables have no date columns to convert in this list
}


# --- Funciones para Cargar/Guardar Datos usando SQLite ---
def load_table(db_file, table_name):
    """Carga datos de una tabla SQLite en un DataFrame."""
    conn = None # Initialize connection to None
    try:
        conn = get_db_conn()
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn) # Quote table name just in case
            # Convert date columns
            if table_name in DATE_COLUMNS:
                for col in DATE_COLUMNS[table_name]:
                    if col in df.columns:
                        # Convert potential strings/objects to datetime, then to date objects
                        # Use errors='coerce' to turn invalid dates into NaT
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                        # Drop rows where date conversion failed (optional, but keeps data clean)
                        df.dropna(subset=[col], inplace=True)
            return df
        else:
            st.warning(f"La tabla '{table_name}' no existe en la base de datos. Creando DataFrame vacío con columnas esperadas.")
            # Return empty DataFrame with expected columns and dtypes
            expected_cols_types = TABLE_COLUMNS.get(table_name, {})
            df = pd.DataFrame(columns=expected_cols_types.keys())
            # Ensure correct dtypes for empty dataframe
            for col, dtype in expected_cols_types.items():
                 if col in df.columns:
                     df[col] = df[col].astype(dtype)

            # If it's a date column in an empty df, the dtype might be 'object'.
            # When data is added, pandas will infer. We handle conversion on load.
            return df

    except sqlite3.Error as e:
        st.error(f"Error al leer la tabla {table_name} de la base de datos: {e}")
        # Return empty DataFrame with expected columns on error
        expected_cols_types = TABLE_COLUMNS.get(table_name, {})
        df = pd.DataFrame(columns=expected_cols_types.keys())
        for col, dtype in expected_cols_types.items():
             if col in df.columns:
                 df[col] = df[col].astype(dtype)
        return df
    finally:
        if conn:
            conn.close()

def save_table(df, db_file, table_name):
    """Guarda un DataFrame en una tabla SQLite (reemplazando la tabla si existe)."""
    conn = None # Initialize connection to None
    try:
        conn = get_db_conn()
        # Convert date objects to string format 'YYYY-MM-DD' before saving
        if table_name in DATE_COLUMNS:
            for col in DATE_COLUMNS[table_name]:
                 if col in df.columns:
                      # Convert to datetime first to handle potential mixed types, then format as string
                     df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('') # Handle NaT by filling with empty string or other marker

        # Use if_exists='replace' to overwrite the table with the current DataFrame content
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit() # Commit changes
        # st.success(f"Datos guardados en la tabla {table_name}") # Too verbose, comment

    except sqlite3.Error as e:
        st.error(f"Error al guardar en la tabla {table_name} en la base de datos: {e}")
    except Exception as e:
         st.error(f"Error general al guardar en la tabla {table_name}: {e}")
    finally:
        if conn:
            conn.close()


# --- Cargar todos los DataFrames al inicio (si no están en session_state) ---
# Usamos st.session_state para mantener los datos a través de las interacciones del usuario

if 'df_equipos' not in st.session_state:
    st.session_state.df_equipos = load_table(DATABASE_FILE, TABLE_EQUIPOS)

if 'df_consumo' not in st.session_state:
    st.session_state.df_consumo = load_table(DATABASE_FILE, TABLE_CONSUMO)

if 'df_costos_salarial' not in st.session_state:
    st.session_state.df_costos_salarial = load_table(DATABASE_FILE, TABLE_COSTOS_SALARIAL)

if 'df_gastos_fijos' not in st.session_state:
    st.session_state.df_gastos_fijos = load_table(DATABASE_FILE, TABLE_GASTOS_FIJOS)

if 'df_gastos_mantenimiento' not in st.session_state:
    st.session_state.df_gastos_mantenimiento = load_table(DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO)

if 'df_precios_combustible' not in st.session_state:
    st.session_state.df_precios_combustible = load_table(DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE)

if 'df_proyectos' not in st.session_state:
    st.session_state.df_proyectos = load_table(DATABASE_FILE, TABLE_PROYECTOS)
    # Ensure ID column exists even if empty
    if st.session_state.df_proyectos.empty or 'ID_Obra' not in st.session_state.df_proyectos.columns:
         st.session_state.df_proyectos = pd.DataFrame(columns=TABLE_COLUMNS[TABLE_PROYECTOS].keys()) # Re-create with expected columns

if 'df_presupuesto_materiales' not in st.session_state:
    st.session_state.df_presupuesto_materiales = load_table(DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES)
    # Ensure calculated column exists
    if 'Costo_Presupuestado' not in st.session_state.df_presupuesto_materiales.columns:
         st.session_state.df_presupuesto_materiales['Costo_Presupuestado'] = 0.0

if 'df_compras_materiales' not in st.session_state:
    st.session_state.df_compras_materiales = load_table(DATABASE_FILE, TABLE_COMPRAS_MATERIALES)
    # Ensure calculated column and ID column exist/are unique if needed
    if 'Costo_Compra' not in st.session_state.df_compras_materiales.columns:
         st.session_state.df_compras_materiales['Costo_Compra'] = 0.0
    # Simple check/re-generation of IDs if loaded data doesn't have them or they aren't unique
    # This might be too aggressive; unique IDs should ideally be generated *on creation*
    # Let's keep the ID generation only on form submission for new rows.
    # Ensure the column exists though:
    if 'ID_Compra' not in st.session_state.df_compras_materiales.columns:
        st.session_state.df_compras_materiales['ID_Compra'] = [f"COMPRA_OLD_{i}" for i in range(len(st.session_state.df_compras_materiales))]


if 'df_asignacion_materiales' not in st.session_state:
    st.session_state.df_asignacion_materiales = load_table(DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)
    # Ensure calculated column and ID column exist
    if 'Costo_Asignado' not in st.session_state.df_asignacion_materiales.columns:
         st.session_state.df_asignacion_materiales['Costo_Asignado'] = 0.0
    # Ensure the column exists though:
    if 'ID_Asignacion' not in st.session_state.df_asignacion_materiales.columns:
        st.session_state.df_asignacion_materiales['ID_Asignacion'] = [f"ASIG_OLD_{i}" for i in range(len(st.session_state.df_asignacion_materiales))]


# --- Helper para calcular costos ---
# Se asegura de no fallar si alguna columna falta temporalmente
def calcular_costo_presupuestado(df):
    """Calcula el costo total presupuestado por fila."""
    # Ensure columns are numeric, coerce errors to handle potential non-numeric input from editor
    df['Cantidad_Presupuestada'] = pd.to_numeric(df.get('Cantidad_Presupuestada', 0), errors='coerce').fillna(0)
    df['Precio_Unitario_Presupuestado'] = pd.to_numeric(df.get('Precio_Unitario_Presupuestado', 0), errors='coerce').fillna(0)
    df['Costo_Presupuestado'] = df['Cantidad_Presupuestada'] * df['Precio_Unitario_Presupuestado']
    return df

def calcular_costo_compra(df):
    """Calcula el costo total de compra por fila."""
    df['Cantidad_Comprada'] = pd.to_numeric(df.get('Cantidad_Comprada', 0), errors='coerce').fillna(0)
    df['Precio_Unitario_Comprado'] = pd.to_numeric(df.get('Precio_Unitario_Comprado', 0), errors='coerce').fillna(0)
    df['Costo_Compra'] = df['Cantidad_Comprada'] * df['Precio_Unitario_Comprado']
    return df

def calcular_costo_asignado(df):
    """Calcula el costo total asignado por fila."""
    df['Cantidad_Asignada'] = pd.to_numeric(df.get('Cantidad_Asignada', 0), errors='coerce').fillna(0)
    df['Precio_Unitario_Asignado'] = pd.to_numeric(df.get('Precio_Unitario_Asignado', 0), errors='coerce').fillna(0)
    df['Costo_Asignado'] = df['Cantidad_Asignada'] * df['Precio_Unitario_Asignado']
    return df


# Aplicar cálculos iniciales si los DataFrames no estaban vacíos
if not st.session_state.df_presupuesto_materiales.empty:
    st.session_state.df_presupuesto_materiales = calcular_costo_presupuestado(st.session_state.df_presupuesto_materiales)
if not st.session_state.df_compras_materiales.empty:
    st.session_state.df_compras_materiales = calcular_costo_compra(st.session_state.df_compras_materiales)
if not st.session_state.df_asignacion_materiales.empty:
    st.session_state.df_asignacion_materiales = calcular_costo_asignado(st.session_state.df_asignacion_materiales)


# --- Authentication Removed ---
# The authentication setup and login/logout calls are removed.
# The sidebar and page content logic will now run unconditionally.


# --- Funciones para cada "Página" ---

def page_equipos():
    st.title("Gestión de Equipos de Mina")
    st.write("Aquí puedes añadir y ver la lista de equipos.")

    st.subheader("Añadir Nuevo Equipo")
    with st.form("form_add_equipo", clear_on_submit=True):
        interno = st.text_input("Interno del Equipo").strip()
        patente = st.text_input("Patente").strip()
        submitted = st.form_submit_button("Añadir Equipo")
        if submitted:
            if interno and patente:
                if interno in st.session_state.df_equipos['Interno'].values:
                    st.warning(f"Ya existe un equipo con Interno {interno}")
                else:
                    new_equipo = pd.DataFrame([{'Interno': interno, 'Patente': patente}])
                    st.session_state.df_equipos = pd.concat([st.session_state.df_equipos, new_equipo], ignore_index=True)
                    save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS) # Save to DB
                    st.success(f"Equipo {interno} ({patente}) añadido.")
            else:
                st.warning("Por favor, complete Interno y Patente.")

    st.subheader("Lista de Equipos")
    # Usar data_editor para permitir edición directa
    df_equipos_editable = st.session_state.df_equipos.copy() # Trabajar en una copia para el editor
    df_equipos_edited = st.data_editor(
        df_equipos_editable,
        key="data_editor_equipos",
        num_rows="dynamic",
        column_config={
             "Interno": st.column_config.TextColumn("Interno", required=True),
             "Patente": st.column_config.TextColumn("Patente", required=True)
        }
    )

    # Lógica para guardar cambios del data_editor
    # Comparar con el original para ver si hay cambios significativos (evitar guardar innecesariamente)
    if not df_equipos_edited.equals(st.session_state.df_equipos):
         st.session_state.df_equipos = df_equipos_edited
         # Auto-save on change detected (can be slow for large data), or require a button click.
         # Button click is safer for validation and user control.
         if st.button("Guardar Cambios en Lista de Equipos"):
              # Validar antes de guardar (ej. Internos únicos, no vacíos)
              if st.session_state.df_equipos['Interno'].duplicated().any():
                  st.error("Error: Hay Internos de Equipo duplicados en la lista. Por favor, corrija los duplicados antes de guardar.")
              elif st.session_state.df_equipos['Interno'].isnull().any() or st.session_state.df_equipos['Patente'].isnull().any():
                  st.error("Error: Hay campos 'Interno' o 'Patente' vacíos. Por favor, complete la información faltante.")
              else:
                  save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS) # Save to DB
                  st.success("Cambios en la lista de equipos guardados.")
         else:
             st.info("Hay cambios sin guardar en la lista de equipos.") # Feedback al usuario


def page_consumibles():
    st.title("Registro de Consumibles por Equipo")
    st.write("Aquí puedes registrar el consumo de combustible, horas y kilómetros por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].tolist()

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero.")
        return

    st.subheader("Añadir Registro de Consumo")
    with st.form("form_add_consumo", clear_on_submit=True):
        interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles)
        fecha = st.date_input("Fecha")
        consumo_litros = st.number_input("Consumo en Litros de Combustible", min_value=0.0, format="%.2f")
        horas_trabajadas = st.number_input("Cantidad de Horas Trabajadas", min_value=0.0, format="%.2f")
        kilometros_recorridos = st.number_input("Cantidad de Kilómetros Recorridos", min_value=0.0, format="%.2f")

        submitted = st.form_submit_button("Registrar Consumo")
        if submitted:
            if interno_seleccionado and fecha and (consumo_litros > 0 or horas_trabajadas > 0 or kilometros_recorridos > 0):
                 new_consumo = pd.DataFrame([{
                    'Interno': interno_seleccionado,
                    'Fecha': fecha, # Will be converted to string by save_table
                    'Consumo_Litros': consumo_litros,
                    'Horas_Trabajadas': horas_trabajadas,
                    'Kilometros_Recorridos': kilometros_recorridos
                 }])
                 # Date conversion handled by save_table before saving, load_table after loading

                 st.session_state.df_consumo = pd.concat([st.session_state.df_consumo, new_consumo], ignore_index=True)
                 save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO) # Save to DB
                 st.success("Registro de consumo añadido.")
            else:
                st.warning("Por favor, complete todos los campos y añada al menos un valor (Litros, Horas o Kilómetros).")

    st.subheader("Registros de Consumo Existente")
    df_consumo_editable = st.session_state.df_consumo.copy()
    # Ensure Fecha is datetime for data_editor if it was loaded as date objects
    if 'Fecha' in df_consumo_editable.columns:
        df_consumo_editable['Fecha'] = pd.to_datetime(df_consumo_editable['Fecha'])

    df_consumo_edited = st.data_editor(
         df_consumo_editable,
         key="data_editor_consumo",
         num_rows="dynamic",
         column_config={
              "Fecha": st.column_config.DateColumn("Fecha", required=True),
              "Interno": st.column_config.TextColumn("Interno", required=True),
              "Consumo_Litros": st.column_config.NumberColumn("Consumo Litros", min_value=0.0, format="%.2f", required=True),
              "Horas_Trabajadas": st.column_config.NumberColumn("Horas Trabajadas", min_value=0.0, format="%.2f", required=True),
              "Kilometros_Recorridos": st.column_config.NumberColumn("Kilómetros Recorridos", min_value=0.0, format="%.2f", required=True),
         }
     )
    if not df_consumo_edited.equals(df_consumo_editable): # Compare with the version before editor
         st.session_state.df_consumo = df_consumo_edited.copy()
         # Convert date column back to date objects for internal consistency in session state
         if 'Fecha' in st.session_state.df_consumo.columns:
             st.session_state.df_consumo['Fecha'] = pd.to_datetime(st.session_state.df_consumo['Fecha'], errors='coerce').dt.date.dropna()

         if st.button("Guardar Cambios en Registros de Consumo"):
              # Basic validation
              if st.session_state.df_consumo['Interno'].isnull().any() or st.session_state.df_consumo['Fecha'].isnull().any():
                   st.error("Error: Campos obligatorios (Interno, Fecha) no pueden estar vacíos.")
              else:
                   save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO) # Save to DB
                   st.success("Cambios en registros de consumo guardados.")
         else:
             st.info("Hay cambios sin guardar en registros de consumo.")


def page_costos_equipos():
    st.title("Registro de Costos por Equipo")
    st.write("Aquí puedes registrar costos salariales, fijos y de mantenimiento por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].tolist()

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero.")
        return

    tab1, tab2, tab3 = st.tabs(["Costos Salariales", "Gastos Fijos", "Gastos Mantenimiento"])

    with tab1:
        st.subheader("Registro de Costos Salariales")
        with st.form("form_add_salarial", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="sal_int")
            fecha = st.date_input("Fecha", key="sal_fecha")
            monto_salarial = st.number_input("Monto Salarial", min_value=0.0, format="%.2f", key="sal_monto")
            submitted = st.form_submit_button("Registrar Costo Salarial")
            if submitted:
                if interno_seleccionado and fecha and monto_salarial > 0:
                    new_costo = pd.DataFrame([{
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Monto_Salarial': monto_salarial
                    }])
                    st.session_state.df_costos_salarial = pd.concat([st.session_state.df_costos_salarial, new_costo], ignore_index=True)
                    save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL) # Save to DB
                    st.success("Costo salarial registrado.")
                else:
                    st.warning("Por favor, complete todos los campos.")
        st.subheader("Registros Salariales Existente")
        df_salarial_editable = st.session_state.df_costos_salarial.copy()
        if 'Fecha' in df_salarial_editable.columns:
             df_salarial_editable['Fecha'] = pd.to_datetime(df_salarial_editable['Fecha'])
        df_salarial_edited = st.data_editor(
            df_salarial_editable,
            key="data_editor_salarial",
            num_rows="dynamic",
             column_config={
                 "Fecha": st.column_config.DateColumn("Fecha", required=True),
                 "Interno": st.column_config.TextColumn("Interno", required=True),
                 "Monto_Salarial": st.column_config.NumberColumn("Monto Salarial", min_value=0.0, format="%.2f", required=True),
             }
        )
        if not df_salarial_edited.equals(df_salarial_editable):
             st.session_state.df_costos_salarial = df_salarial_edited.copy()
             if 'Fecha' in st.session_state.df_costos_salarial.columns:
                 st.session_state.df_costos_salarial['Fecha'] = pd.to_datetime(st.session_state.df_costos_salarial['Fecha'], errors='coerce').dt.date.dropna()
             if st.button("Guardar Cambios en Registros Salariales"):
                  if st.session_state.df_costos_salarial['Interno'].isnull().any() or st.session_state.df_costos_salarial['Fecha'].isnull().any() or st.session_state.df_costos_salarial['Monto_Salarial'].isnull().any():
                       st.error("Error: Campos obligatorios no pueden estar vacíos.")
                  else:
                       save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL) # Save to DB
                       st.success("Cambios en registros salariales guardados.")
             else:
                 st.info("Hay cambios sin guardar en registros salariales.")


    with tab2:
        st.subheader("Registro de Gastos Fijos")
        with st.form("form_add_fijos", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="fij_int")
            fecha = st.date_input("Fecha", key="fij_fecha")
            tipo_gasto = st.text_input("Tipo de Gasto Fijo", key="fij_tipo").strip()
            monto_gasto = st.number_input("Monto del Gasto Fijo", min_value=0.0, format="%.2f", key="fij_monto")
            descripcion = st.text_area("Descripción (Opcional)", key="fij_desc").strip()
            submitted = st.form_submit_button("Registrar Gasto Fijo")
            if submitted:
                if interno_seleccionado and fecha and monto_gasto > 0 and tipo_gasto:
                    new_gasto = pd.DataFrame([{
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Tipo_Gasto_Fijo': tipo_gasto,
                       'Monto_Gasto_Fijo': monto_gasto,
                       'Descripcion': descripcion
                    }])
                    st.session_state.df_gastos_fijos = pd.concat([st.session_state.df_gastos_fijos, new_gasto], ignore_index=True)
                    save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS) # Save to DB
                    st.success("Gasto fijo registrado.")
                else:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto).")
        st.subheader("Registros de Gastos Fijos Existente")
        df_fijos_editable = st.session_state.df_gastos_fijos.copy()
        if 'Fecha' in df_fijos_editable.columns:
             df_fijos_editable['Fecha'] = pd.to_datetime(df_fijos_editable['Fecha'])
        df_fijos_edited = st.data_editor(
            df_fijos_editable,
            key="data_editor_fijos",
            num_rows="dynamic",
            column_config={
                 "Fecha": st.column_config.DateColumn("Fecha", required=True),
                 "Interno": st.column_config.TextColumn("Interno", required=True),
                 "Tipo_Gasto_Fijo": st.column_config.TextColumn("Tipo Gasto Fijo", required=True),
                 "Monto_Gasto_Fijo": st.column_config.NumberColumn("Monto Gasto Fijo", min_value=0.0, format="%.2f", required=True),
                 "Descripcion": st.column_config.TextColumn("Descripción", required=False),
             }
        )
        if not df_fijos_edited.equals(df_fijos_editable):
             st.session_state.df_gastos_fijos = df_fijos_edited.copy()
             if 'Fecha' in st.session_state.df_gastos_fijos.columns:
                  st.session_state.df_gastos_fijos['Fecha'] = pd.to_datetime(st.session_state.df_gastos_fijos['Fecha'], errors='coerce').dt.date.dropna()
             if st.button("Guardar Cambios en Registros de Gastos Fijos"):
                  if st.session_state.df_gastos_fijos['Interno'].isnull().any() or st.session_state.df_gastos_fijos['Fecha'].isnull().any() or st.session_state.df_gastos_fijos['Tipo_Gasto_Fijo'].isnull().any() or st.session_state.df_gastos_fijos['Monto_Gasto_Fijo'].isnull().any():
                       st.error("Error: Campos obligatorios no pueden estar vacíos.")
                  else:
                       save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS) # Save to DB
                       st.success("Cambios en registros de gastos fijos guardados.")
             else:
                 st.info("Hay cambios sin guardar en registros de gastos fijos.")


    with tab3:
        st.subheader("Registro de Gastos de Mantenimiento")
        with st.form("form_add_mantenimiento", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="mant_int")
            fecha = st.date_input("Fecha", key="mant_fecha")
            tipo_mantenimiento = st.text_input("Tipo de Mantenimiento", key="mant_tipo").strip()
            monto_mantenimiento = st.number_input("Monto del Mantenimiento", min_value=0.0, format="%.2f", key="mant_monto")
            descripcion = st.text_area("Descripción (Opcional)", key="mant_desc").strip()
            submitted = st.form_submit_button("Registrar Gasto Mantenimiento")
            if submitted:
                if interno_seleccionado and fecha and monto_mantenimiento > 0 and tipo_mantenimiento:
                    new_gasto = pd.DataFrame([{
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Tipo_Mantenimiento': tipo_mantenimiento,
                       'Monto_Mantenimiento': monto_mantenimiento,
                       'Descripcion': descripcion
                    }])
                    st.session_state.df_gastos_mantenimiento = pd.concat([st.session_state.df_gastos_mantenimiento, new_gasto], ignore_index=True)
                    save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO) # Save to DB
                    st.success("Gasto de mantenimiento registrado.")
                else:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto).")
        st.subheader("Registros de Gastos de Mantenimiento Existente")
        df_mantenimiento_editable = st.session_state.df_gastos_mantenimiento.copy()
        if 'Fecha' in df_mantenimiento_editable.columns:
             df_mantenimiento_editable['Fecha'] = pd.to_datetime(df_mantenimiento_editable['Fecha'])
        df_mantenimiento_edited = st.data_editor(
            df_mantenimiento_editable,
            key="data_editor_mantenimiento",
            num_rows="dynamic",
            column_config={
                 "Fecha": st.column_config.DateColumn("Fecha", required=True),
                 "Interno": st.column_config.TextColumn("Interno", required=True),
                 "Tipo_Mantenimiento": st.column_config.TextColumn("Tipo Mantenimiento", required=True),
                 "Monto_Mantenimiento": st.column_config.NumberColumn("Monto Mantenimiento", min_value=0.0, format="%.2f", required=True),
                 "Descripcion": st.column_config.TextColumn("Descripción", required=False),
             }
        )
        if not df_mantenimiento_edited.equals(df_mantenimiento_editable):
             st.session_state.df_gastos_mantenimiento = df_mantenimiento_edited.copy()
             if 'Fecha' in st.session_state.df_gastos_mantenimiento.columns:
                  st.session_state.df_gastos_mantenimiento['Fecha'] = pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha'], errors='coerce').dt.date.dropna()
             if st.button("Guardar Cambios en Registros de Mantenimiento"):
                  if st.session_state.df_gastos_mantenimiento['Interno'].isnull().any() or st.session_state.df_gastos_mantenimiento['Fecha'].isnull().any() or st.session_state.df_gastos_mantenimiento['Tipo_Mantenimiento'].isnull().any() or st.session_state.df_gastos_mantenimiento['Monto_Mantenimiento'].isnull().any():
                       st.error("Error: Campos obligatorios no pueden estar vacíos.")
                  else:
                       save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO) # Save to DB
                       st.success("Cambios en registros de mantenimiento guardados.")
             else:
                 st.info("Hay cambios sin guardar en registros de mantenimiento.")


def page_reportes_mina():
    st.title("Reportes de Mina por Fecha")
    st.write("Genera reportes de consumo y costos por equipo en un rango de fechas.")

    st.subheader("Registrar Precio del Combustible")
    with st.form("form_add_precio_combustible", clear_on_submit=True):
        fecha_precio = st.date_input("Fecha del Precio")
        precio_litro = st.number_input("Precio por Litro", min_value=0.01, format="%.2f")
        submitted = st.form_submit_button("Registrar Precio")
        if submitted:
            if fecha_precio and precio_litro > 0:
                new_precio = pd.DataFrame([{'Fecha': fecha_precio, 'Precio_Litro': precio_litro}]) # Handled by save_table
                # Reemplazar si la fecha ya existe, de lo contrario añadir
                # Need to convert session_state dates to comparable format if they are date objects
                df_precios_temp = st.session_state.df_precios_combustible.copy()
                if 'Fecha' in df_precios_temp.columns:
                     # Convert both sides to string or comparable type for accurate comparison
                     df_precios_temp['Fecha_str'] = pd.to_datetime(df_precios_temp['Fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
                     fecha_precio_str = pd.to_datetime(fecha_precio, errors='coerce').strftime('%Y-%m-%d')

                     st.session_state.df_precios_combustible = df_precios_temp[
                         df_precios_temp['Fecha_str'] != fecha_precio_str
                     ].drop(columns=['Fecha_str']) # Drop temp column

                st.session_state.df_precios_combustible = pd.concat([st.session_state.df_precios_combustible, new_precio], ignore_index=True)
                save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE) # Save to DB
                st.success("Precio del combustible registrado/actualizado.")
            else:
                st.warning("Por favor, complete la fecha y el precio.")
    st.subheader("Precios del Combustible Existente")
    df_precios_editable = st.session_state.df_precios_combustible.copy()
    if 'Fecha' in df_precios_editable.columns:
         df_precios_editable['Fecha'] = pd.to_datetime(df_precios_editable['Fecha'])
    df_precios_edited = st.data_editor(
        df_precios_editable,
        key="data_editor_precios",
        num_rows="dynamic",
        column_config={
            "Fecha": st.column_config.DateColumn("Fecha", required=True),
            "Precio_Litro": st.column_config.NumberColumn("Precio por Litro", min_value=0.01, format="%.2f", required=True),
        }
    )
    if not df_precios_edited.equals(df_precios_editable):
         st.session_state.df_precios_combustible = df_precios_edited.copy()
         if 'Fecha' in st.session_state.df_precios_combustible.columns:
             st.session_state.df_precios_combustible['Fecha'] = pd.to_datetime(st.session_state.df_precios_combustible['Fecha'], errors='coerce').dt.date.dropna()

         if st.button("Guardar Cambios en Precios de Combustible"):
              # Opcional: Validar fechas únicas si cada fecha debe tener un solo precio
              # Need to check duplicates based on date object or string representation
              df_temp_check = st.session_state.df_precios_combustible.copy()
              if 'Fecha' in df_temp_check.columns:
                   df_temp_check['Fecha_str'] = pd.to_datetime(df_temp_check['Fecha'], errors='coerce').dt.strftime('%Y-%m-%d')
                   if df_temp_check['Fecha_str'].duplicated().any():
                       st.error("Error: Hay fechas duplicadas en los precios de combustible. Por favor, corrija los duplicados antes de guardar.")
                       return # Stop saving if duplicates found
              save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE) # Save to DB
              st.success("Cambios en precios de combustible guardados.")
         else:
             st.info("Hay cambios sin guardar en precios de combustible.")


    st.subheader("Reporte por Rango de Fechas")
    col1, col2 = st.columns(2)

    # Ensure dates in session state DFs are datetime or date objects for min/max
    min_date_cons = pd.to_datetime(st.session_state.df_consumo['Fecha'], errors='coerce').dropna().min() if not st.session_state.df_consumo.empty and 'Fecha' in st.session_state.df_consumo.columns else pd.Timestamp.now().date()
    min_date_sal = pd.to_datetime(st.session_state.df_costos_salarial['Fecha'], errors='coerce').dropna().min() if not st.session_state.df_costos_salarial.empty and 'Fecha' in st.session_state.df_costos_salarial.columns else pd.Timestamp.now().date()
    min_date_fij = pd.to_datetime(st.session_state.df_gastos_fijos['Fecha'], errors='coerce').dropna().min() if not st.session_state.df_gastos_fijos.empty and 'Fecha' in st.session_state.df_gastos_fijos.columns else pd.Timestamp.now().date()
    min_date_mant = pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha'], errors='coerce').dropna().min() if not st.session_state.df_gastos_mantenimiento.empty and 'Fecha' in st.session_state.df_gastos_mantenimiento.columns else pd.Timestamp.now().date()
    min_date_pre = pd.to_datetime(st.session_state.df_precios_combustible['Fecha'], errors='coerce').dropna().min() if not st.session_state.df_precios_combustible.empty and 'Fecha' in st.session_state.df_precios_combustible.columns else pd.Timestamp.now().date()

    max_date_cons = pd.to_datetime(st.session_state.df_consumo['Fecha'], errors='coerce').dropna().max() if not st.session_state.df_consumo.empty and 'Fecha' in st.session_state.df_consumo.columns else pd.Timestamp.now().date()
    max_date_sal = pd.to_datetime(st.session_state.df_costos_salarial['Fecha'], errors='coerce').dropna().max() if not st.session_state.df_costos_salarial.empty and 'Fecha' in st.session_state.df_costos_salarial.columns else pd.Timestamp.now().date()
    max_date_fij = pd.to_datetime(st.session_state.df_gastos_fijos['Fecha'], errors='coerce').dropna().max() if not st.session_state.df_gastos_fijos.empty and 'Fecha' in st.session_state.df_gastos_fijos.columns else pd.Timestamp.now().date()
    max_date_mant = pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha'], errors='coerce').dropna().max() if not st.session_state.df_gastos_mantenimiento.empty and 'Fecha' in st.session_state.df_gastos_mantenimiento.columns else pd.Timestamp.now().date()
    max_date_pre = pd.to_datetime(st.session_state.df_precios_combustible['Fecha'], errors='coerce').dropna().max() if not st.session_state.df_precios_combustible.empty and 'Fecha' in st.session_state.df_precios_combustible.columns else pd.Timestamp.now().date()

    # Determine the overall min/max date across relevant dataframes
    all_relevant_dates = pd.to_datetime([]).date # Start with empty Series of date objects
    if not st.session_state.df_consumo.empty and 'Fecha' in st.session_state.df_consumo.columns:
         all_relevant_dates = all_relevant_dates.union(pd.to_datetime(st.session_state.df_consumo['Fecha'], errors='coerce').dropna().dt.date)
    if not st.session_state.df_costos_salarial.empty and 'Fecha' in st.session_state.df_costos_salarial.columns:
         all_relevant_dates = all_relevant_dates.union(pd.to_datetime(st.session_state.df_costos_salarial['Fecha'], errors='coerce').dropna().dt.date)
    if not st.session_state.df_gastos_fijos.empty and 'Fecha' in st.session_state.df_gastos_fijos.columns:
         all_relevant_dates = all_relevant_dates.union(pd.to_datetime(st.session_state.df_gastos_fijos['Fecha'], errors='coerce').dropna().dt.date)
    if not st.session_state.df_gastos_mantenimiento.empty and 'Fecha' in st.session_state.df_gastos_mantenimiento.columns:
         all_relevant_dates = all_relevant_dates.union(pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha'], errors='coerce').dropna().dt.date)
    if not st.session_state.df_precios_combustible.empty and 'Fecha' in st.session_state.df_precios_combustible.columns:
         all_relevant_dates = all_relevant_dates.union(pd.to_datetime(st.session_state.df_precios_combustible['Fecha'], errors='coerce').dropna().dt.date)

    min_app_date = min(all_relevant_dates) if not all_relevant_dates.empty else pd.Timestamp.now().date() - pd.Timedelta(days=365) # Default to a year ago
    max_app_date = max(all_relevant_dates) if not all_relevant_dates.empty else pd.Timestamp.now().date() # Default to today

    # Ensure date inputs have valid default values within possible range
    default_start = max_app_date - pd.Timedelta(days=30) if max_app_date - pd.Timedelta(days=30) >= min_app_date else min_app_date
    default_end = max_app_date

    with col1:
        fecha_inicio = st.date_input("Fecha de Inicio del Reporte", default_start, min_value=min_app_date, max_value=max_app_date)
    with col2:
        fecha_fin = st.date_input("Fecha de Fin del Reporte", default_end, min_value=min_app_date, max_value=max_app_date)

    if st.button("Generar Reporte"):
        if fecha_inicio > fecha_fin:
            st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
            return

        # Ensure date columns in the DFs being filtered are datetime objects for robust comparison
        # Filter needs pd.Timestamp for comparison, not date objects directly sometimes
        df_consumo_temp = st.session_state.df_consumo.copy()
        if 'Fecha' in df_consumo_temp.columns:
             df_consumo_temp['Fecha'] = pd.to_datetime(df_consumo_temp['Fecha'], errors='coerce').dropna()

        df_precios_temp = st.session_state.df_precios_combustible.copy()
        if 'Fecha' in df_precios_temp.columns:
             df_precios_temp['Fecha'] = pd.to_datetime(df_precios_temp['Fecha'], errors='coerce').dropna()

        df_salarial_temp = st.session_state.df_costos_salarial.copy()
        if 'Fecha' in df_salarial_temp.columns:
             df_salarial_temp['Fecha'] = pd.to_datetime(df_salarial_temp['Fecha'], errors='coerce').dropna()

        df_fijos_temp = st.session_state.df_gastos_fijos.copy()
        if 'Fecha' in df_fijos_temp.columns:
             df_fijos_temp['Fecha'] = pd.to_datetime(df_fijos_temp['Fecha'], errors='coerce').dropna()

        df_mantenimiento_temp = st.session_state.df_gastos_mantenimiento.copy()
        if 'Fecha' in df_mantenimiento_temp.columns:
             df_mantenimiento_temp['Fecha'] = pd.to_datetime(df_mantenimiento_temp['Fecha'], errors='coerce').dropna()


        # Filtrar por fecha (using pd.Timestamp for comparisons)
        start_ts = pd.to_datetime(fecha_inicio)
        end_ts = pd.to_datetime(fecha_fin)

        df_consumo_filtrado = df_consumo_temp[(df_consumo_temp['Fecha'] >= start_ts) & (df_consumo_temp['Fecha'] <= end_ts)].copy()
        precios_filtrado = df_precios_temp[(df_precios_temp['Fecha'] >= start_ts) & (df_precios_temp['Fecha'] <= end_ts)].copy()
        salarial_filtrado = df_salarial_temp[(df_salarial_temp['Fecha'] >= start_ts) & (df_salarial_temp['Fecha'] <= end_ts)].copy()
        fijos_filtrado = df_fijos_temp[(df_fijos_temp['Fecha'] >= start_ts) & (df_fijos_temp['Fecha'] <= end_ts)].copy()
        mantenimiento_filtrado = df_mantenimiento_temp[(df_mantenimiento_temp['Fecha'] >= start_ts) & (df_mantenimiento_temp['Fecha'] <= end_ts)].copy()


        if df_consumo_filtrado.empty:
            st.info("No hay datos de consumo en el rango de fechas seleccionado.")
            # We continue to show other costs if they exist
            reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Costo_Total_Combustible']) # Empty DF for later merge
        else:
             # Calcular métricas por equipo y fecha en el periodo
             df_consumo_filtrado['Consumo_L_H'] = df_consumo_filtrado.apply(
                 lambda row: row['Consumo_Litros'] / row['Horas_Trabajadas'] if row['Horas_Trabajadas'] > 0 else 0, axis=1
             )
             df_consumo_filtrado['Consumo_L_KM'] = df_consumo_filtrado.apply(
                  lambda row: row['Consumo_Litros'] / row['Kilometros_Recorridos'] if row['Kilometros_Recorridos'] > 0 else 0, axis=1
             )

             # Unir con precios de combustible (usando el precio más reciente antes o en la fecha de consumo)
             # Ensure dates are sorted datetime for merge_asof
             consumo_for_merge = df_consumo_filtrado.sort_values('Fecha')
             precios_for_merge = precios_filtrado.sort_values('Fecha')

             # merge_asof requires 'Fecha' to be datetime, which df_..._temp are
             reporte_consumo = pd.merge_asof(
                 consumo_for_merge,
                 precios_for_merge,
                 on='Fecha',
                 by='Interno' if 'Interno' in precios_for_merge.columns else None, # Intenta unir por Interno si es posible (precio por equipo)
                 direction='backward'
             )
             # Calcular costo del combustible
             reporte_consumo['Costo_Combustible'] = reporte_consumo['Consumo_Litros'] * reporte_consumo['Precio_Litro'].fillna(0) # If no price, cost is 0

             # Resumen de Consumo y Costo Combustible por Equipo en el período
             reporte_resumen_consumo = reporte_consumo.groupby('Interno').agg(
                 Total_Consumo_Litros=('Consumo_Litros', 'sum'),
                 Total_Horas=('Horas_Trabajadas', 'sum'),
                 Total_Kilometros=('Kilometros_Recorridos', 'sum'),
                 Costo_Total_Combustible=('Costo_Combustible', 'sum')
             ).reset_index()

             # Calcular L/H y L/KM promedio en el período
             reporte_resumen_consumo['Avg_Consumo_L_H'] = reporte_resumen_consumo.apply(
                  lambda row: row['Total_Consumo_Litros'] / row['Total_Horas'] if row['Total_Horas'] > 0 else 0, axis=1
             )
             reporte_resumen_consumo['Avg_Consumo_L_KM'] = reporte_resumen_consumo.apply(
                  lambda row: row['Total_Consumo_Litros'] / row['Total_Kilometros'] if row['Kilometros_Recorridos'] > 0 else 0, axis=1
             )

             # Unir con información de equipos (Patente)
             reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_equipos[['Interno', 'Patente']], on='Interno', how='left')

             st.subheader(f"Reporte Consumo y Costo Combustible ({fecha_inicio} a {fecha_fin})")
             st.dataframe(reporte_resumen_consumo[[
                 'Interno', 'Patente', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros',
                 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'
             ]].round(2))


        # --- Sumar otros costos (Salarial, Fijos, Mantenimiento) en el periodo ---
        # Agrupar por Interno
        salarial_agg = salarial_filtrado.groupby('Interno')['Monto_Salarial'].sum().reset_index(name='Total_Salarial')
        fijos_agg = fijos_filtrado.groupby('Interno')['Monto_Gasto_Fijo'].sum().reset_index(name='Total_Gastos_Fijos')
        mantenimiento_agg = mantenimiento_filtrado.groupby('Interno')['Monto_Mantenimiento'].sum().reset_index(name='Total_Gastos_Mantenimiento')

        # Unir todos los costos
        # Empezar con la lista única de equipos que tuvieron ALGÚN costo o consumo en el periodo
        all_internos_in_period = pd.concat([
            df_consumo_filtrado['Interno'] if not df_consumo_filtrado.empty else pd.Series(dtype='object'),
            salarial_filtrado['Interno'] if not salarial_filtrado.empty else pd.Series(dtype='object'),
            fijos_filtrado['Interno'] if not fijos_filtrado.empty else pd.Series(dtype='object'),
            mantenimiento_filtrado['Interno'] if not mantenimiento_filtrado.empty else pd.Series(dtype='object')
        ]).unique()
        df_all_internos = pd.DataFrame(all_internos_in_period, columns=['Interno'])

        reporte_costo_total = df_all_internos.merge(reporte_resumen_consumo[['Interno', 'Costo_Total_Combustible']], on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(salarial_agg, on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(fijos_agg, on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(mantenimiento_agg, on='Interno', how='left').fillna(0)

        # Añadir Patente
        reporte_costo_total = reporte_costo_total.merge(st.session_state.df_equipos[['Interno', 'Patente']], on='Interno', how='left')


        reporte_costo_total['Costo_Total_Equipo'] = reporte_costo_total['Costo_Total_Combustible'] + reporte_costo_total['Total_Salarial'] + reporte_costo_total['Total_Gastos_Fijos'] + reporte_costo_total['Total_Gastos_Mantenimiento']

        st.subheader(f"Reporte Costo Total por Equipo ({fecha_inicio} a {fecha_fin})")
        if reporte_costo_total.empty:
             st.info("No hay datos de costos (Combustible, Salarial, Fijos, Mantenimiento) en el rango de fechas seleccionado para ningún equipo.")
        else:
             st.dataframe(reporte_costo_total[[
                 'Interno', 'Patente', 'Costo_Total_Combustible', 'Total_Salarial',
                 'Total_Gastos_Fijos', 'Total_Gastos_Mantenimiento', 'Costo_Total_Equipo'
             ]].round(2))


def page_variacion_costos_flota():
    st.title("Variación de Costos de Flota (Gráfico de Cascada)")
    st.write("Compara los costos totales de la flota entre dos períodos para visualizar la variación.")

    st.subheader("Seleccione Períodos a Comparar")
    col1, col2, col3, col4 = st.columns(4)
    # Intentar establecer fechas por defecto basadas en datos si existen
    all_cost_dates = pd.to_datetime([]).date # Empty date series
    df_list_for_dates = [st.session_state.df_consumo, st.session_state.df_costos_salarial, st.session_state.df_gastos_fijos, st.session_state.df_gastos_mantenimiento, st.session_state.df_precios_combustible]
    date_cols_list = [['Fecha'], ['Fecha'], ['Fecha'], ['Fecha'], ['Fecha']]

    for df, cols in zip(df_list_for_dates, date_cols_list):
        if not df.empty and cols and cols[0] in df.columns: # Check if df not empty and date column exists
            valid_dates = pd.to_datetime(df[cols[0]], errors='coerce').dropna()
            if not valid_dates.empty:
                 all_cost_dates = all_cost_dates.union(valid_dates.dt.date)

    if not all_cost_dates.empty:
        min_app_date = min(all_cost_dates)
        max_app_date = max(all_cost_dates)
        # Suggest recent months
        default_end_p2 = max_app_date
        default_start_p2 = default_end_p2 - pd.Timedelta(days=30)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = default_end_p1 - pd.Timedelta(days=30)

        # Ensure default dates are within the min/max range
        default_start_p1 = max(default_start_p1, min_app_date)
        default_end_p1 = max(default_end_p1, min_app_date) # Ensure end is not before min
        default_start_p2 = max(default_start_p2, min_app_date)
        default_end_p2 = max(default_end_p2, min_app_date)


    else:
        # Fallback if no data dates exist
        today = pd.Timestamp.now().date()
        min_app_date = today - pd.Timedelta(days=365)
        max_app_date = today
        default_end_p2 = max_app_date
        default_start_p2 = default_end_p2 - pd.Timedelta(days=30)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = default_end_p1 - pd.Timedelta(days=30)


    with col1:
        fecha_inicio_p1 = st.date_input("Inicio Período 1", default_start_p1, min_value=min_app_date, max_value=max_app_date, key="fecha_inicio_p1")
    with col2:
        fecha_fin_p1 = st.date_input("Fin Período 1", default_end_p1, min_value=min_app_date, max_value=max_app_date, key="fecha_fin_p1")
    with col3:
        fecha_inicio_p2 = st.date_input("Inicio Período 2", default_start_p2, min_value=min_app_date, max_value=max_app_date, key="fecha_inicio_p2")
    with col4:
        fecha_fin_p2 = st.date_input("Fin Período 2", default_end_p2, min_value=min_app_date, max_value=max_app_date, key="fecha_fin_p2")


    if st.button("Generar Gráfico de Cascada"):
        # Basic date validation
        if fecha_inicio_p1 > fecha_fin_p1 or fecha_inicio_p2 > fecha_fin_p2:
             st.error("Las fechas dentro de cada período no son válidas.")
             return
        # Optional: Warning for overlapping periods
        if not (fecha_fin_p1 < fecha_inicio_p2 or fecha_fin_p2 < fecha_inicio_p1):
             st.warning("Los períodos seleccionados se solapan. Para una comparación clara de la variación, es recomendable usar períodos que no se solapen.")


        # --- Calcular Costos por Período y Categoría ---
        # Helper function to aggregate costs for a given date range
        def aggregate_costs(df, date_col, start_date, end_date):
            # Ensure date column is datetime and handle errors for filtering
            if date_col not in df.columns:
                 return pd.DataFrame() # Return empty if date col doesn't exist

            df_temp = df.copy()
            df_temp[date_col] = pd.to_datetime(df_temp[date_col], errors='coerce').dropna() # Remove rows with invalid dates
            # Filter
            df_filtered = df_temp[(df_temp[date_col] >= pd.to_datetime(start_date)) & (df_temp[date_col] <= pd.to_datetime(end_date))]
            return df_filtered

        # Costs for Period 1
        consumo_p1 = aggregate_costs(st.session_state.df_consumo, 'Fecha', fecha_inicio_p1, fecha_fin_p1)
        precios_p1 = aggregate_costs(st.session_state.df_precios_combustible, 'Fecha', fecha_inicio_p1, fecha_fin_p1)
        salarial_p1 = aggregate_costs(st.session_state.df_costos_salarial, 'Fecha', fecha_inicio_p1, fecha_fin_p1)
        fijos_p1 = aggregate_costs(st.session_state.df_gastos_fijos, 'Fecha', fecha_inicio_p1, fecha_fin_p1)
        mantenimiento_p1 = aggregate_costs(st.session_state.df_gastos_mantenimiento, 'Fecha', fecha_inicio_p1, fecha_fin_p1)

        # Calculate fuel cost for Period 1
        costo_combustible_p1 = 0
        if not consumo_p1.empty and not precios_p1.empty:
             # Need to ensure dates are sorted datetime for merge_asof
             consumo_p1_sorted = consumo_p1.sort_values('Fecha')
             precios_p1_sorted = precios_p1.sort_values('Fecha')
             # Merge asof - using date only is likely sufficient for fleet total
             consumo_p1_merged = pd.merge_asof(consumo_p1_sorted, precios_p1_sorted, on='Fecha', direction='backward')
             costo_combustible_p1 = (consumo_p1_merged['Consumo_Litros'] * consumo_p1_merged['Precio_Litro'].fillna(0)).sum()


        costo_salarial_p1 = salarial_p1['Monto_Salarial'].sum() if not salarial_p1.empty and 'Monto_Salarial' in salarial_p1.columns else 0
        costo_fijos_p1 = fijos_p1['Monto_Gasto_Fijo'].sum() if not fijos_p1.empty and 'Monto_Gasto_Fijo' in fijos_p1.columns else 0
        costo_mantenimiento_p1 = mantenimiento_p1['Monto_Mantenimiento'].sum() if not mantenimiento_p1.empty and 'Monto_Mantenimiento' in mantenimiento_p1.columns else 0

        total_costo_p1 = costo_combustible_p1 + costo_salarial_p1 + costo_fijos_p1 + costo_mantenimiento_p1

        # Costs for Period 2
        consumo_p2 = aggregate_costs(st.session_state.df_consumo, 'Fecha', fecha_inicio_p2, fecha_fin_p2)
        precios_p2 = aggregate_costs(st.session_state.df_precios_combustible, 'Fecha', fecha_inicio_p2, fecha_fin_p2)
        salarial_p2 = aggregate_costs(st.session_state.df_costos_salarial, 'Fecha', fecha_inicio_p2, fecha_fin_p2)
        fijos_p2 = aggregate_costs(st.session_state.df_gastos_fijos, 'Fecha', fecha_inicio_p2, fecha_fin_p2)
        mantenimiento_p2 = aggregate_costs(st.session_state.df_gastos_mantenimiento, 'Fecha', fecha_inicio_p2, fecha_fin_p2)

        # Calculate fuel cost for Period 2
        costo_combustible_p2 = 0
        if not consumo_p2.empty and not precios_p2.empty:
             consumo_p2_sorted = consumo_p2.sort_values('Fecha')
             precios_p2_sorted = precios_p2.sort_values('Fecha')
             consumo_p2_merged = pd.merge_asof(consumo_p2_sorted, precios_p2_sorted, on='Fecha', direction='backward')
             costo_combustible_p2 = (consumo_p2_merged['Consumo_Litros'] * consumo_p2_merged['Precio_Litro'].fillna(0)).sum()

        costo_salarial_p2 = salarial_p2['Monto_Salarial'].sum() if not salarial_p2.empty and 'Monto_Salarial' in salarial_p2.columns else 0
        costo_fijos_p2 = fijos_p2['Monto_Gasto_Fijo'].sum() if not fijos_p2.empty and 'Monto_Gasto_Fijo' in fijos_p2.columns else 0
        costo_mantenimiento_p2 = mantenimiento_p2['Monto_Mantenimiento'].sum() if not mantenimiento_p2.empty and 'Monto_Mantenimiento' in mantenimiento_p2.columns else 0

        total_costo_p2 = costo_combustible_p2 + costo_salarial_p2 + costo_fijos_p2 + costo_mantenimiento_p2

        # --- Preparar Datos para el Gráfico de Cascada ---
        labels = [
            f'Total Costo<br>Periodo 1<br>({fecha_inicio_p1} a {fecha_fin_p1})'
        ]
        measures = ['absolute']
        values = [total_costo_p1]
        texts = [f"${total_costo_p1:,.2f}"] # Formato de texto

        # Variaciones
        variacion_combustible = costo_combustible_p2 - costo_combustible_p1
        variacion_salarial = costo_salarial_p2 - costo_salarial_p1
        variacion_fijos = costo_fijos_p2 - costo_fijos_p1
        variacion_mantenimiento = costo_mantenimiento_p2 - costo_mantenimiento_p1

        # Añadir variaciones (solo si hay alguna diferencia significativa)
        # Use a slightly larger threshold for display purposes maybe? 10 or 100?
        threshold = 0.01 # Keep small threshold for calculation accuracy

        if abs(variacion_combustible) > threshold:
            labels.append('Variación Combustible')
            measures.append('relative')
            values.append(variacion_combustible)
            texts.append(f"${variacion_combustible:,.2f}")

        if abs(variacion_salarial) > threshold:
            labels.append('Variación Salarial')
            measures.append('relative')
            values.append(variacion_salarial)
            texts.append(f"${variacion_salarial:,.2f}")

        if abs(variacion_fijos) > threshold:
            labels.append('Variación Fijos')
            measures.append('relative')
            values.append(variacion_fijos)
            texts.append(f"${variacion_fijos:,.2f}")

        if abs(variacion_mantenimiento) > threshold:
            labels.append('Variación Mantenimiento')
            measures.append('relative')
            values.append(variacion_mantenimiento)
            texts.append(f"${variacion_mantenimiento:,.2f}")

        # Añadir total Periodo 2
        labels.append(f'Total Costo<br>Periodo 2<br>({fecha_inicio_p2} a {fecha_fin_p2})')
        measures.append('total')
        values.append(total_costo_p2)
        texts.append(f"${total_costo_p2:,.2f}")


        # --- Crear Gráfico de Cascada ---
        if len(labels) <= 1: # Only the starting point (Total Period 1) means no variation or Period 2 exists
             st.info("No hay datos de costos o la variación entre los períodos es insignificante para mostrar el gráfico de cascada.")
        else:
             fig = go.Figure(go.Waterfall(
                 name = "Variación de Costos",
                 orientation = "v",
                 measure = measures,
                 x = labels,
                 textposition = "outside",
                 text = texts,
                 y = values,
                 connector = {"line":{"color":"rgb(63, 63, 63)"}},
             ))

             fig.update_layout(
                 title = f'Variación de Costos de Flota: {fecha_inicio_p1} a {fecha_fin_p1} vs {fecha_inicio_p2} a {fecha_fin_p2}',
                 showlegend = False,
                 yaxis_title="Monto ($)",
                 margin=dict(l=20, r=20, t=100, b=20), # Ajustar márgenes para el título largo
                 height=500
             )

             st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detalle de Costos por Período")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Periodo 1: {fecha_inicio_p1} a {fecha_fin_p1}**")
            st.write(f"- Combustible: ${costo_combustible_p1:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p1:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p1:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p1:,.2f}")
            st.write(f"**Total Periodo 1: ${total_costo_p1:,.2f}**")
        with col2:
            st.write(f"**Periodo 2: {fecha_inicio_p2} a {fecha_fin_p2}**")
            st.write(f"- Combustible: ${costo_combustible_p2:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p2:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p2:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p2:,.2f}")
            st.write(f"**Total Periodo 2: ${total_costo_p2:,.2f}**")

        if abs(total_costo_p1 - total_costo_p2) > threshold:
            st.subheader("Variaciones Absolutas")
            st.write(f"- Combustible: ${variacion_combustible:,.2f}")
            st.write(f"- Salarial: ${variacion_salarial:,.2f}")
            st.write(f"- Fijos: ${variacion_fijos:,.2f}")
            st.write(f"- Mantenimiento: ${variacion_mantenimiento:,.2f}")
            st.write(f"**Variación Total: ${total_costo_p2 - total_costo_p1:,.2f}**")
        else:
             st.info("Los costos totales entre los dos períodos son iguales o la variación es insignificante.")


def page_gestion_obras():
    st.title("Gestión de Obras")
    st.write("Aquí puedes crear y gestionar proyectos de obra.")

    st.subheader("Crear Nueva Obra")
    with st.form("form_add_obra", clear_on_submit=True):
        nombre_obra = st.text_input("Nombre de la Obra").strip()
        responsable = st.text_input("Responsable de Seguimiento").strip()
        submitted = st.form_submit_button("Crear Obra")
        if submitted:
            if nombre_obra and responsable:
                # Generar ID único simple (usando timestamp + contador para evitar colisiones si se crean rápido)
                # Use nanoseconds for higher chance of uniqueness if many created quickly
                id_obra = f"OBRA_{int(pd.Timestamp.now().timestamp() * 1e9)}_{len(st.session_state.df_proyectos)}"
                new_obra = pd.DataFrame([{'ID_Obra': id_obra, 'Nombre_Obra': nombre_obra, 'Responsable': responsable}])
                st.session_state.df_proyectos = pd.concat([st.session_state.df_proyectos, new_obra], ignore_index=True)
                save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS) # Save to DB
                st.success(f"Obra '{nombre_obra}' creada con ID: {id_obra}")
            else:
                st.warning("Por favor, complete el Nombre de la Obra y el Responsable.")

    st.subheader("Lista de Obras")
    if st.session_state.df_proyectos.empty:
        st.info("No hay obras creadas aún.")
    else:
        # Mostrar lista de obras y permitir edición básica
        df_proyectos_editable = st.session_state.df_proyectos.copy()
        df_proyectos_edited = st.data_editor(
             df_proyectos_editable,
             key="data_editor_proyectos",
             num_rows="dynamic",
             column_config={
                  "ID_Obra": st.column_config.TextColumn("ID Obra", disabled=True), # No permitir editar ID
                  "Nombre_Obra": st.column_config.TextColumn("Nombre Obra", required=True),
                  "Responsable": st.column_config.TextColumn("Responsable", required=True)
             }
        )
        # Check if any row was deleted or added, or if content changed
        if not df_proyectos_edited.equals(st.session_state.df_proyectos):
             st.session_state.df_proyectos = df_proyectos_edited
             if st.button("Guardar Cambios en Lista de Obras"):
                 # Simple validation
                 if st.session_state.df_proyectos['Nombre_Obra'].isnull().any() or st.session_state.df_proyectos['Responsable'].isnull().any():
                      st.error("Error: Los campos 'Nombre Obra' y 'Responsable' no pueden estar vacíos.")
                 else:
                      save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS) # Save to DB
                      st.success("Cambios en la lista de obras guardados.")
                      st.experimental_rerun() # Recargar para actualizar selectbox y otros elementos dependientes
             else:
                 st.info("Hay cambios sin guardar en la lista de obras.")

        # Ensure obras_disponibles is populated after potential saves/deletions
        obras_disponibles = st.session_state.df_proyectos['ID_Obra'].tolist()
        if not obras_disponibles:
             st.warning("No hay obras disponibles para gestionar presupuesto.")
             return # Exit function if no works exist after potential edits


        st.markdown("---")
        st.subheader("Gestionar Presupuesto por Obra")

        obra_seleccionada_id = st.selectbox(
            "Seleccione una Obra:",
            obras_disponibles,
            format_func=lambda x: f"{st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x]['Nombre_Obra'].iloc[0]} (ID: {x})" if not st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x].empty else x,
            key="select_obra_gestion"
        )

        if obra_seleccionada_id:
            # Ensure the selected ID still exists in the updated df_proyectos
            if obra_seleccionada_id not in st.session_state.df_proyectos['ID_Obra'].values:
                 st.warning(f"La obra seleccionada (ID: {obra_seleccionada_id}) ya no existe. Por favor, seleccione otra.")
                 # Consider clearing the selectbox or state related to the selected work
                 # This can happen if the selected work was just deleted via data_editor
                 st.session_state.select_obra_gestion = None # Reset selectbox
                 st.experimental_rerun() # Rerun to clear the state
                 return # Exit function

            obra_nombre = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_seleccionada_id]['Nombre_Obra'].iloc[0]
            st.markdown(f"#### Presupuesto de Materiales para '{obra_nombre}'")

            # Filtrar presupuesto para la obra seleccionada
            df_presupuesto_obra = st.session_state.df_presupuesto_materiales[
                st.session_state.df_presupuesto_materiales['ID_Obra'] == obra_seleccionada_id
            ].copy()

            st.write("Añadir o editar el presupuesto de materiales.")

            # Añadir nuevo material al presupuesto (Formulario)
            with st.form(f"form_add_presupuesto_{obra_seleccionada_id}", clear_on_submit=True):
                material = st.text_input("Nombre del Material").strip()
                cantidad_presupuestada = st.number_input("Cantidad Presupuestada", min_value=0.0, format="%.2f")
                precio_unitario_presupuestado = st.number_input("Precio Unitario Presupuestado", min_value=0.0, format="%.2f")
                submitted = st.form_submit_button("Añadir Material al Presupuesto")
                if submitted:
                    if material and cantidad_presupuestada >= 0 and precio_unitario_presupuestado >= 0:
                         new_item = pd.DataFrame([{
                             'ID_Obra': obra_seleccionada_id,
                             'Material': material,
                             'Cantidad_Presupuestada': cantidad_presupuestada,
                             'Precio_Unitario_Presupuestado': precio_unitario_presupuestado
                         }])
                         new_item = calcular_costo_presupuestado(new_item)
                         st.session_state.df_presupuesto_materiales = pd.concat([st.session_state.df_presupuesto_materiales, new_item], ignore_index=True)
                         save_table(st.session_state.df_presupuesto_materiales, DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES) # Save to DB
                         st.success(f"Material '{material}' añadido al presupuesto de la obra.")
                         st.experimental_rerun() # Rerun to update the data_editor and report
                    else:
                        st.warning("Por favor, complete todos los campos para añadir material.")

            # Mostrar y editar presupuesto existente (data_editor)
            st.write("Editar presupuesto existente:")
            # Only display columns the user should edit
            df_presupuesto_obra_display = df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']].copy()
            # data_editor returns a copy, work with that
            df_presupuesto_obra_edited = st.data_editor(
                df_presupuesto_obra_display,
                key=f"data_editor_presupuesto_{obra_seleccionada_id}",
                num_rows="dynamic",
                column_config={
                    "Material": st.column_config.TextColumn("Material", required=True),
                    "Cantidad_Presupuestada": st.column_config.NumberColumn("Cantidad Presupuestada", min_value=0.0, format="%.2f", required=True),
                    "Precio_Unitario_Presupuestado": st.column_config.NumberColumn("Precio Unitario Presupuestado", min_value=0.0, format="%.2f", required=True)
                }
            )

            # Lógica para guardar cambios del data_editor
            # Compare the edited version (which only has editable columns) with a sliced version of the original
            if not df_presupuesto_obra_edited.equals(df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']]):
                 if st.button(f"Guardar Cambios en Presupuesto de {obra_nombre}"):
                     # Remove the old rows for this work from the main DataFrame
                     df_rest_presupuesto = st.session_state.df_presupuesto_materiales[
                         st.session_state.df_presupuesto_materiales['ID_Obra'] != obra_seleccionada_id
                     ].copy()

                     # Add the edited rows (which might include new dynamic rows from data_editor)
                     # Ensure the ID_Obra column is present for these rows
                     df_presupuesto_obra_edited['ID_Obra'] = obra_seleccionada_id
                     # Recalculate cost based on edited values
                     df_presupuesto_obra_edited = calcular_costo_presupuestado(df_presupuesto_obra_edited)

                     # Combine the rest of the data with the updated/edited data for this work
                     st.session_state.df_presupuesto_materiales = pd.concat([df_rest_presupuesto, df_presupuesto_obra_edited], ignore_index=True)

                     # Basic validation before saving
                     if st.session_state.df_presupuesto_materiales[st.session_state.df_presupuesto_materiales['ID_Obra'] == obra_seleccionada_id]['Material'].isnull().any():
                          st.error("Error: El nombre del material no puede estar vacío en el presupuesto.")
                     else:
                          save_table(st.session_state.df_presupuesto_materiales, DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES) # Save to DB
                          st.success("Presupuesto de la obra guardado.")
                          st.experimental_rerun() # Recargar para actualizar la vista del editor y el reporte
                 else:
                     st.info("Hay cambios sin guardar en el presupuesto de la obra.")

            # Reporte dentro de la misma página de gestión de obra
            st.markdown(f"#### Reporte de Presupuesto para '{obra_nombre}'")
            if df_presupuesto_obra.empty:
                st.info("No hay presupuesto de materiales registrado para esta obra.")
            else:
                st.subheader("Detalle del Presupuesto")
                # Ensure calculated cost column is present even if not explicitly added in editor section
                df_presupuesto_obra_with_cost = calcular_costo_presupuestado(df_presupuesto_obra.copy())
                st.dataframe(df_presupuesto_obra_with_cost[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']].round(2))

                total_cantidad_presupuestada = df_presupuesto_obra_with_cost['Cantidad_Presupuestada'].sum()
                total_costo_presupuestado = df_presupuesto_obra_with_cost['Costo_Presupuestado'].sum()

                st.subheader("Resumen del Presupuesto")
                st.write(f"**Cantidad Total Presupuestada:** {total_cantidad_presupuestada:,.2f}")
                st.write(f"**Costo Total Presupuestado:** ${total_costo_presupuestado:,.2f}")

            # Reporte de Variación dentro de la misma página de gestión de obra
            st.markdown(f"#### Variación Materiales para '{obra_nombre}' (Presupuesto vs Asignado)")

            df_asignacion_obra = st.session_state.df_asignacion_materiales[
                 st.session_state.df_asignacion_materiales['ID_Obra'] == obra_seleccionada_id
            ].copy()

            if df_presupuesto_obra.empty and df_asignacion_obra.empty:
                st.info("No hay presupuesto ni materiales asignados para esta obra.")
            else:
               # Agrupar presupuesto por material
               # Use the dataframe that includes the calculated cost
               presupuesto_agg = df_presupuesto_obra_with_cost.groupby('Material').agg(
                   Cantidad_Presupuestada=('Cantidad_Presupuestada', 'sum'),
                   Costo_Presupuestado=('Costo_Presupuestado', 'sum')
               ).reset_index()

               # Agrupar asignaciones por material
               # Ensure calculated cost is present in the assignment df
               df_asignacion_obra_with_cost = calcular_costo_asignado(df_asignacion_obra.copy())
               asignacion_agg = df_asignacion_obra_with_cost.groupby('Material').agg(
                   Cantidad_Asignada=('Cantidad_Asignada', 'sum'),
                   Costo_Asignado=('Costo_Asignado', 'sum')
               ).reset_index()

               # Unir presupuesto y asignación
               variacion_obra = pd.merge(presupuesto_agg, asignacion_agg, on='Material', how='outer').fillna(0)

               # Calcular variaciones
               variacion_obra['Cantidad_Variacion'] = variacion_obra['Cantidad_Asignada'] - variacion_obra['Cantidad_Presupuestada']
               variacion_obra['Costo_Variacion'] = variacion_obra['Costo_Asignado'] - variacion_obra['Costo_Presupuestado']

               st.subheader("Reporte de Variación por Material")
               if variacion_obra.empty:
                    st.info("No hay datos de variación de materiales para esta obra.")
               else:
                   st.dataframe(variacion_obra[[
                       'Material',
                       'Cantidad_Presupuestada', 'Cantidad_Asignada', 'Cantidad_Variacion',
                       'Costo_Presupuestado', 'Costo_Asignado', 'Costo_Variacion'
                   ]].round(2))

                   total_costo_presupuestado_obra = variacion_obra['Costo_Presupuestado'].sum()
                   total_costo_asignado_obra = variacion_obra['Costo_Asignado'].sum()
                   total_variacion_costo_obra = total_costo_asignado_obra - total_costo_presupuestado_obra

                   st.subheader("Resumen de Variación de Costo Total")
                   st.write(f"Costo Presupuestado Total: ${total_costo_presupuestado_obra:,.2f}")
                   st.write(f"Costo Asignado (Real) Total: ${total_costo_asignado_obra:,.2f}")
                   st.write(f"Variación Total: ${total_variacion_costo_obra:,.2f}")

                   # Opcional: Gráfico de Cascada para la obra (presupuesto vs asignado)
                   if abs(total_variacion_costo_obra) > 0.01 or total_costo_presupuestado_obra > 0 or total_costo_asignado_obra > 0: # Only show if there's data or variation
                        st.subheader("Gráfico de Variación de Costo por Obra")
                        fig_obra_variacion = go.Figure(go.Waterfall(
                           name = "Variación Obra",
                           orientation = "v",
                           measure = ['absolute', 'relative', 'total'],
                           x = [f'Presupuesto<br>{obra_nombre}', 'Variación Total', f'Asignado<br>{obra_nombre}'],
                           textposition = "outside",
                           text = [f"${total_costo_presupuestado_obra:,.2f}", f"${total_variacion_costo_obra:,.2f}", f"${total_costo_asignado_obra:,.2f}"],
                           y = [total_costo_presupuestado_obra, total_variacion_costo_obra, total_costo_asignado_obra],
                           connector = {"line":{"color":"rgb(63, 63, 63)"}},
                        ))

                        fig_obra_variacion.update_layout(
                            title = f'Variación Costo Total Obra: {obra_nombre}',
                            showlegend = False,
                            yaxis_title="Monto ($)",
                            margin=dict(l=20, r=20, t=60, b=20),
                            height=400
                        )
                        st.plotly_chart(fig_obra_variacion, use_container_width=True)
                   else:
                       st.info("El costo presupuestado y asignado para esta obra son iguales o ambos son cero.")


def page_reporte_presupuesto_total_obras():
    st.title("Reporte de Presupuesto Total por Obras")
    st.write("Suma el presupuesto total de materiales de todas las obras.")

    if st.session_state.df_presupuesto_materiales.empty:
        st.info("No hay presupuesto de materiales registrado para ninguna obra.")
        return

    # Asegurar que la columna calculada existe y los datos son numéricos
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated

    # Agrupar por obra
    reporte_por_obra = df_presupuesto.groupby('ID_Obra').agg(
        Cantidad_Total_Presupuestada=('Cantidad_Presupuestada', 'sum'),
        Costo_Total_Presupuestado=('Costo_Presupuestado', 'sum')
    ).reset_index()

    # Unir con nombres de obras
    reporte_por_obra = reporte_por_obra.merge(st.session_state.df_proyectos[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')

    st.subheader("Presupuesto Total por Obra")
    if reporte_por_obra.empty:
         st.info("No hay presupuesto total calculado (posiblemente datos de presupuesto no válidos).")
    else:
         st.dataframe(reporte_por_obra[['Nombre_Obra', 'ID_Obra', 'Cantidad_Total_Presupuestada', 'Costo_Total_Presupuestado']].round(2))

         # Total general
         cantidad_gran_total = reporte_por_obra['Cantidad_Total_Presupuestada'].sum()
         costo_gran_total = reporte_por_obra['Costo_Total_Presupuestado'].sum()

         st.subheader("Gran Total Presupuestado (Todas las Obras)")
         st.write(f"**Cantidad Gran Total Presupuestada:** {cantidad_gran_total:,.2f}")
         st.write(f"**Costo Gran Total Presupuestado:** ${costo_gran_total:,.2f}")


def page_compras_asignacion():
    st.title("Gestión de Compras y Asignación de Materiales")
    st.write("Registra las compras y asigna materiales a las obras.")

    st.subheader("Registrar Compra de Materiales")
    with st.form("form_add_compra", clear_on_submit=True):
        fecha_compra = st.date_input("Fecha de Compra")
        material_compra = st.text_input("Nombre del Material Comprado").strip()
        cantidad_comprada = st.number_input("Cantidad Comprada", min_value=0.0, format="%.2f")
        precio_unitario_comprado = st.number_input("Precio Unitario de Compra", min_value=0.0, format="%.2f")
        submitted = st.form_submit_button("Registrar Compra")
        if submitted:
            if fecha_compra and material_compra and cantidad_comprada >= 0 and precio_unitario_comprado >= 0:
                 # Generar ID único simple
                id_compra = f"COMPRA_{int(pd.Timestamp.now().timestamp() * 1000)}_{len(st.session_state.df_compras_materiales)}"
                new_compra = pd.DataFrame([{
                    'ID_Compra': id_compra,
                    'Fecha_Compra': fecha_compra, # Handled by save_table
                    'Material': material_compra,
                    'Cantidad_Comprada': cantidad_comprada,
                    'Precio_Unitario_Comprado': precio_unitario_comprado
                }])
                new_compra = calcular_costo_compra(new_compra)
                st.session_state.df_compras_materiales = pd.concat([st.session_state.df_compras_materiales, new_compra], ignore_index=True)
                save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES) # Save to DB
                st.success(f"Compra de '{material_compra}' registrada con ID: {id_compra}")
                st.experimental_rerun() # Rerun to update the history view and assignment options
            else:
                st.warning("Por favor, complete todos los campos de la compra.")

    st.subheader("Historial de Compras")
    if st.session_state.df_compras_materiales.empty:
        st.info("No hay compras registradas aún.")
    else:
         # Usar data_editor para permitir edición
         df_compras_editable = st.session_state.df_compras_materiales.copy()
         # Ensure date is datetime for data_editor
         if 'Fecha_Compra' in df_compras_editable.columns:
              df_compras_editable['Fecha_Compra'] = pd.to_datetime(df_compras_editable['Fecha_Compra'])
         # Recalculate cost for display in editor if needed, or rely on save logic
         df_compras_editable = calcular_costo_compra(df_compras_editable) # Ensure cost is up-to-date in editor display


         df_compras_edited = st.data_editor(
             df_compras_editable[['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']], # Select columns to display/edit
             key="data_editor_compras",
             num_rows="dynamic",
             column_config={
                 "ID_Compra": st.column_config.TextColumn("ID Compra", disabled=True),
                 "Fecha_Compra": st.column_config.DateColumn("Fecha Compra", required=True),
                 "Material": st.column_config.TextColumn("Material", required=True),
                 "Cantidad_Comprada": st.column_config.NumberColumn("Cantidad Comprada", min_value=0.0, format="%.2f", required=True),
                 "Precio_Unitario_Comprado": st.column_config.NumberColumn("Precio Unitario Compra", min_value=0.0, format="%.2f", required=True),
                 "Costo_Compra": st.column_config.NumberColumn("Costo Compra", disabled=True, format="%.2f") # Calculated, not editable
             }
         )
         # Lógica de guardado para el editor
         # Compare the edited version (which only has displayed columns) with a sliced version of the original
         if not df_compras_edited.equals(st.session_state.df_compras_materiales[['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']]):
             # Update the session state DataFrame with the edited data
             # Need to handle potential new rows from 'dynamic' editor (they won't have ID_Compra initially)
             # And deleted rows
             # The simplest approach with 'replace' saving is to just make the edited df the new session state df
             # Need to re-calculate cost and ensure IDs for new rows if any
             edited_with_all_cols = df_compras_edited.copy()

             # Re-calculate cost for edited/new rows
             edited_with_all_cols = calcular_costo_compra(edited_with_all_cols)

             # Handle new rows added via the editor (they won't have an ID_Compra)
             # Check for rows where ID_Compra is null or empty after editing
             if edited_with_all_cols['ID_Compra'].isnull().any() or (edited_with_all_cols['ID_Compra'].astype(str) == '').any():
                  st.warning("Detectadas nuevas filas en el historial de compras sin ID. Se generarán IDs al guardar.")
                  # Generate IDs only for rows that don't have one
                  new_row_mask = edited_with_all_cols['ID_Compra'].isnull() | (edited_with_all_cols['ID_Compra'].astype(str) == '')
                  # Simple ID generation - improve this if truly unique IDs across sessions/runs are critical
                  # For simplicity with replace, just ensuring it's not empty is sufficient
                  edited_with_all_cols.loc[new_row_mask, 'ID_Compra'] = [
                       f"COMPRA_EDIT_{int(pd.Timestamp.now().timestamp() * 1000)}_{i}"
                       for i in range(new_row_mask.sum())
                  ]


             st.session_state.df_compras_materiales = edited_with_all_cols

             if st.button("Guardar Cambios en Historial de Compras"):
                 # Validar antes de guardar (ej. campos requeridos, IDs únicos si necessary, though 'replace' handles duplicates by overwriting)
                 if st.session_state.df_compras_materiales['Material'].isnull().any() or st.session_state.df_compras_materiales['Cantidad_Comprada'].isnull().any() or st.session_state.df_compras_materiales['Precio_Unitario_Comprado'].isnull().any():
                      st.error("Error: Hay campos obligatorios vacíos en el historial de compras.")
                 # Optional: Check for duplicate IDs if you ever plan to merge based on ID after replacement
                 # elif st.session_state.df_compras_materiales['ID_Compra'].duplicated().any():
                 #     st.error("Error: Hay IDs de compra duplicados. Por favor, corrija.")
                 else:
                      save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES) # Save to DB
                      st.success("Cambios en historial de compras guardados.")
                      st.experimental_rerun() # Opcional: recargar para mostrar el DF actualizado
             else:
                 st.info("Hay cambios sin guardar en el historial de compras.")


    st.markdown("---")

    st.subheader("Asignar Materiales a Obra")
    if st.session_state.df_proyectos.empty:
        st.warning("No hay obras creadas. No se pueden asignar materiales.")
    else:
        obras_disponibles = st.session_state.df_proyectos['ID_Obra'].tolist()
        # Listar materiales únicos de las compras para el selectbox, o permitir texto libre
        materiales_comprados_unicos = st.session_state.df_compras_materiales['Material'].unique().tolist()


        with st.form("form_asignar_material", clear_on_submit=True):
            fecha_asignacion = st.date_input("Fecha de Asignación")
            obra_destino_id = st.selectbox(
                "Seleccione Obra de Destino:",
                obras_disponibles,
                 format_func=lambda x: f"{st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x]['Nombre_Obra'].iloc[0]} (ID: {x})" if not st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x].empty else x,
                 key="asig_obra"
            )
            # Option 2: Texto libre (permite asignar stock inicial or not tied to specific purchase)
            # If you want to use selectbox from purchased:
            # material_assign_options = materiales_comprados_unicos + ["(Otro / No Comprado)"] # Add option for free text? Or just allow free text.
            # material_asignado_select = st.selectbox("Material a Asignar:", material_assign_options, key="asig_material_select")
            # if material_asignado_select == "(Otro / No Comprado)":
            #      material_asignado = st.text_input("Especificar Material").strip()
            # else:
            #      material_asignado = material_asignado_select
            # Simplest: Just allow free text input for material name
            material_asignado = st.text_input("Nombre del Material a Asignar").strip()


            cantidad_asignada = st.number_input("Cantidad a Asignar", min_value=0.0, format="%.2f", key="asig_cantidad")
            # Precio al que se ASIGNA (puede ser diferente al de compra, ej. costo promedio, or ingreso manual del costo real)
            precio_unitario_asignado = st.number_input("Precio Unitario Asignado (Costo Real)", min_value=0.0, format="%.2f", key="asig_precio")

            submitted = st.form_submit_button("Asignar Material")
            if submitted:
                # Check for required fields and valid numeric inputs
                if fecha_asignacion and obra_destino_id and material_asignado and cantidad_asignada >= 0 and precio_unitario_asignado >= 0:
                     if cantidad_asignada == 0 and precio_unitario_asignado == 0:
                          st.warning("La cantidad y el precio unitario asignado no pueden ser ambos cero si desea registrar una asignación significativa.")
                     elif obra_destino_id not in st.session_state.df_proyectos['ID_Obra'].values:
                          st.error(f"El ID de Obra '{obra_destino_id}' seleccionado no es válido. Por favor, seleccione una obra de la lista existente.")
                     else:
                          id_asignacion = f"ASIG_{int(pd.Timestamp.now().timestamp() * 1000)}_{len(st.session_state.df_asignacion_materiales)}" # Simple ID único
                          new_asignacion = pd.DataFrame([{
                              'ID_Asignacion': id_asignacion,
                              'Fecha_Asignacion': fecha_asignacion, # Handled by save_table
                              'ID_Obra': obra_destino_id,
                              'Material': material_asignado,
                              'Cantidad_Asignada': cantidad_asignada,
                              'Precio_Unitario_Asignado': precio_unitario_asignado
                          }])
                          new_asignacion = calcular_costo_asignado(new_asignacion)
                          st.session_state.df_asignacion_materiales = pd.concat([st.session_state.df_asignacion_materiales, new_asignacion], ignore_index=True)
                          save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB
                          st.success(f"Material '{material_asignado}' ({cantidad_asignada} unidades) asignado a obra '{st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_destino_id]['Nombre_Obra'].iloc[0]}'.")
                          st.experimental_rerun() # Rerun to update history and deshacer list
                else:
                    st.warning("Por favor, complete todos los campos de asignación con valores válidos (Cantidad y Precio >= 0).")

        st.subheader("Historial de Asignaciones")
        if st.session_state.df_asignacion_materiales.empty:
            st.info("No hay materiales asignados aún.")
        else:
            # Usar data_editor para permitir edición si se desea
             df_asignaciones_editable = st.session_state.df_asignacion_materiales.copy()
             # Ensure date is datetime for data_editor
             if 'Fecha_Asignacion' in df_asignaciones_editable.columns:
                  df_asignaciones_editable['Fecha_Asignacion'] = pd.to_datetime(df_asignaciones_editable['Fecha_Asignacion'])
             # Ensure cost is up-to-date in editor display
             df_asignaciones_editable = calcular_costo_asignado(df_asignaciones_editable)


             df_asignaciones_edited = st.data_editor(
                 df_asignaciones_editable[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']],
                 key="data_editor_asignaciones",
                 num_rows="dynamic",
                  column_config={
                      "ID_Asignacion": st.column_config.TextColumn("ID Asignación", disabled=True),
                      "Fecha_Asignacion": st.column_config.DateColumn("Fecha Asignación", required=True),
                      "ID_Obra": st.column_config.TextColumn("ID Obra", required=True),
                      "Material": st.column_config.TextColumn("Material", required=True),
                      "Cantidad_Asignada": st.column_config.NumberColumn("Cantidad Asignada", min_value=0.0, format="%.2f", required=True),
                      "Precio_Unitario_Asignado": st.column_config.NumberColumn("Precio Unitario Asignado", min_value=0.0, format="%.2f", required=True),
                      "Costo_Asignado": st.column_config.NumberColumn("Costo Asignado", disabled=True, format="%.2f") # Calculado
                  }
             )
             # Lógica de guardado para el editor
             # Compare the edited version with a sliced version of the original
             if not df_asignaciones_edited.equals(st.session_state.df_asignacion_materiales[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']]):
                  edited_with_all_cols = df_asignaciones_edited.copy()

                  # Re-calculate cost for edited/new rows
                  edited_with_all_cols = calcular_costo_asignado(edited_with_all_cols)

                  # Handle new rows added via the editor (they won't have an ID_Asignacion)
                  if edited_with_all_cols['ID_Asignacion'].isnull().any() or (edited_with_all_cols['ID_Asignacion'].astype(str) == '').any():
                       st.warning("Detectadas nuevas filas en el historial de asignaciones sin ID. Se generarán IDs al guardar.")
                       new_row_mask = edited_with_all_cols['ID_Asignacion'].isnull() | (edited_with_all_cols['ID_Asignacion'].astype(str) == '')
                       edited_with_all_cols.loc[new_row_mask, 'ID_Asignacion'] = [
                            f"ASIG_EDIT_{int(pd.Timestamp.now().timestamp() * 1000)}_{i}"
                            for i in range(new_row_mask.sum())
                       ]

                  st.session_state.df_asignacion_materiales = edited_with_all_cols

                  if st.button("Guardar Cambios en Historial de Asignaciones"):
                      # Validar antes de guardar (ej. campos requeridos, ID_Obra exista)
                      if st.session_state.df_asignacion_materiales['ID_Obra'].isnull().any() or st.session_state.df_asignacion_materiales['Material'].isnull().any() or st.session_state.df_asignacion_materiales['Cantidad_Asignada'].isnull().any() or st.session_state.df_asignacion_materiales['Precio_Unitario_Asignado'].isnull().any():
                           st.error("Error: Hay campos obligatorios vacíos en el historial de asignaciones.")
                      elif not st.session_state.df_asignacion_materiales['ID_Obra'].isin(st.session_state.df_proyectos['ID_Obra']).all():
                           # Check only IDs that are *not* empty strings or null
                           invalid_ids = st.session_state.df_asignacion_materiales[
                               st.session_state.df_asignacion_materiales['ID_Obra'].notna() & (st.session_state.df_asignacion_materiales['ID_Obra'].astype(str) != '')
                           ]['ID_Obra'].unique()

                           non_existent_ids = [id for id in invalid_ids if id not in st.session_state.df_proyectos['ID_Obra'].values]

                           if non_existent_ids:
                                st.error(f"Error: Una o más asignaciones tienen un 'ID Obra' que no existe en la lista de obras: {non_existent_ids}. Por favor, corrija.")
                                # Do not save if there are invalid work IDs
                                return
                           else:
                                # All non-empty work IDs exist
                                pass # Proceed to save


                      save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB
                      st.success("Cambios en historial de asignaciones guardados.")
                      st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en el historial de asignaciones.")

        st.subheader("Deshacer Asignación (por ID)")
        asignaciones_disponibles = st.session_state.df_asignacion_materiales['ID_Asignacion'].tolist()

        if not asignaciones_disponibles:
            st.info("No hay asignaciones para deshacer.")
        else:
            # Fetch brief info for the selectbox display
            # Need to handle date objects for formatting
            df_asig_info = st.session_state.df_asignacion_materiales[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada']].copy()
            if 'Fecha_Asignacion' in df_asig_info.columns:
                df_asig_info['Fecha_Asignacion'] = pd.to_datetime(df_asig_info['Fecha_Asignacion'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('Fecha Inválida')

            asig_options_dict = df_asig_info.set_index('ID_Asignacion').to_dict('index')

            format_func = lambda asig_id: f"{asig_id} ({asig_options_dict.get(asig_id, {}).get('Fecha_Asignacion', 'N/A')} - {asig_options_dict.get(asig_id, {}).get('ID_Obra', 'N/A')} - {asig_options_dict.get(asig_id, {}).get('Material', 'N/A')} - {asig_options_dict.get(asig_id, {}).get('Cantidad_Asignada', 0.0):.2f})" if asig_id in asig_options_dict else asig_id

            id_asignacion_deshacer = st.selectbox(
                "Seleccione ID de Asignación a deshacer:",
                asignaciones_disponibles,
                format_func=format_func
            )

            if st.button(f"Deshacer Asignación Seleccionada ({id_asignacion_deshacer})"):
                st.session_state.df_asignacion_materiales = st.session_state.df_asignacion_materiales[
                    st.session_state.df_asignacion_materiales['ID_Asignacion'] != id_asignacion_deshacer
                ].copy() # Use .copy() to avoid SettingWithCopyWarning later
                save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB
                st.success(f"Asignación {id_asignacion_deshacer} deshecha.")
                st.experimental_rerun()


def page_reporte_variacion_total_obras():
    st.title("Reporte de Variación Total Obras (Presupuesto vs Real)")
    st.write("Compara el costo total presupuestado vs el costo total real (asignado) para cada obra.")

    if st.session_state.df_presupuesto_materiales.empty and st.session_state.df_asignacion_materiales.empty:
        st.info("No hay datos de presupuesto ni de asignación para generar el reporte.")
        return

    # Calcular totales presupuestados por obra
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    # Ensure numeric and calculate cost
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated
    presupuesto_total_obra = df_presupuesto.groupby('ID_Obra')['Costo_Presupuestado'].sum().reset_index(name='Costo_Presupuestado_Total')
    presupuesto_cantidad_obra = df_presupuesto.groupby('ID_Obra')['Cantidad_Presupuestada'].sum().reset_index(name='Cantidad_Presupuestada_Total')


    # Calcular totales asignados por obra
    df_asignacion = st.session_state.df_asignacion_materiales.copy()
    # Ensure numeric and calculate cost
    df_asignacion = calcular_costo_asignado(df_asignacion) # Ensure cost is calculated/updated
    asignacion_total_obra = df_asignacion.groupby('ID_Obra')['Costo_Asignado'].sum().reset_index(name='Costo_Asignado_Total')
    asignacion_cantidad_obra = df_asignacion.groupby('ID_Obra')['Cantidad_Asignada'].sum().reset_index(name='Cantidad_Asignada_Total')


    # Unir DataFrames de costo y cantidad
    reporte_variacion_obras = pd.merge(
        presupuesto_total_obra,
        asignacion_total_obra,
        on='ID_Obra',
        how='outer' # Usar outer para incluir obras que solo tienen presupuesto o solo asignación
    )
    reporte_variacion_obras = pd.merge(
         reporte_variacion_obras,
         presupuesto_cantidad_obra,
         on='ID_Obra',
         how='outer'
    )
    reporte_variacion_obras = pd.merge(
        reporte_variacion_obras,
        asignacion_cantidad_obra,
        on='ID_Obra',
        how='outer'
    ).fillna(0)


    # Unir con nombres de obras (para mostrar nombres en el reporte)
    reporte_variacion_obras = reporte_variacion_obras.merge(st.session_state.df_proyectos[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')

    # Calculate variation
    reporte_variacion_obras['Variacion_Total_Costo'] = reporte_variacion_obras['Costo_Asignado_Total'] - reporte_variacion_obras['Costo_Presupuestado_Total']
    reporte_variacion_obras['Variacion_Total_Cantidad'] = reporte_variacion_obras['Cantidad_Asignada_Total'] - reporte_variacion_obras['Cantidad_Presupuestada_Total']


    st.subheader("Variación de Costo y Cantidad por Obra (Presupuesto vs Real)")
    if reporte_variacion_obras.empty:
        st.info("No hay datos válidos para generar el reporte de variación por obra.")
    else:
        st.dataframe(reporte_variacion_obras[[
            'Nombre_Obra', 'ID_Obra',
            'Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total', 'Variacion_Total_Cantidad',
            'Costo_Presupuestado_Total', 'Costo_Asignado_Total', 'Variacion_Total_Costo'
        ]].round(2))

        # --- Gráfico de Cascada Total (Costo) ---
        total_presupuestado_general = reporte_variacion_obras['Costo_Presupuestado_Total'].sum()
        total_asignado_general = reporte_variacion_obras['Costo_Asignado_Total'].sum()
        total_variacion_general_costo = total_asignado_general - total_presupuestado_general


        if abs(total_variacion_general_costo) > 0.01 or total_presupuestado_general > 0 or total_asignado_general > 0: # Only show if there's something to show
            st.subheader("Gráfico de Cascada: Presupuesto Total vs Costo Real Total")

            # Preparar datos para la cascada de COSTO
            labels_costo = ['Total Presupuestado']
            values_costo = [total_presupuestado_general]
            measures_costo = ['absolute']
            texts_costo = [f"${total_presupuestado_general:,.2f}"]

            # Añadir variaciones por obra (solo costo, solo si hay variación en la obra)
            # Filter out zero variations for the graph and sort
            reporte_variacion_obras_sorted_costo = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Costo']) > 0.01].sort_values('Variacion_Total_Costo', ascending=False)

            for index, row in reporte_variacion_obras_sorted_costo.iterrows():
                 # Use shorter label for graph if needed
                 obra_label = row['Nombre_Obra']
                 if len(obra_label) > 15: # Arbitrary length limit
                      obra_label = obra_label[:12] + '...'
                 labels_costo.append(f"Var: {obra_label}") # Use Nombre_Obra in label?

                 values_costo.append(row['Variacion_Total_Costo'])
                 measures_costo.append('relative')
                 texts_costo.append(f"${row['Variacion_Total_Costo']:,.2f}")

            # Añadir el total asignado
            labels_costo.append('Total Asignado')
            values_costo.append(total_asignado_general)
            measures_costo.append('total')
            texts_costo.append(f"${total_asignado_general:,.2f}")

            # Check again after adding variation bars if there's more than just the start and end points
            if len(labels_costo) > 2 or (len(labels_costo) == 2 and values_costo[0] != values_costo[1]): # More than just start/end OR start!=end
                fig_total_variacion_costo = go.Figure(go.Waterfall(
                    name = "Variación Total Costo",
                    orientation = "v",
                    measure = measures_costo,
                    x = labels_costo,
                    textposition = "outside",
                    text = texts_costo,
                    y = values_costo,
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                ))

                fig_total_variacion_costo.update_layout(
                    title = 'Variación Total de Costos Materiales (Presupuesto vs Real) - Todas las Obras',
                    showlegend = False,
                    yaxis_title="Monto ($)",
                    margin=dict(l=20, r=20, t=60, b=20),
                    height=600
                )

                st.plotly_chart(fig_total_variacion_costo, use_container_width=True)
            else:
                 st.info("El costo total presupuestado es igual al costo total asignado o la variación total es insignificante. No hay variación de costo para mostrar en el gráfico.")

        else:
             st.info("No hay costo presupuestado ni asignado total para mostrar el gráfico de variación de costo.")

        # --- Gráfico de Cascada Total (Cantidad) ---
        total_cantidad_presupuestada_general = reporte_variacion_obras['Cantidad_Presupuestada_Total'].sum()
        total_cantidad_asignada_general = reporte_variacion_obras['Cantidad_Asignada_Total'].sum()
        total_variacion_general_cantidad = total_cantidad_asignada_general - total_cantidad_presupuestada_general

        if abs(total_variacion_general_cantidad) > 0.01 or total_cantidad_presupuestada_general > 0 or total_cantidad_asignada_general > 0: # Only show if there's something to show
            st.subheader("Gráfico de Cascada: Cantidad Total Presupuestada vs Cantidad Real Total")

            # Preparar datos para la cascada de CANTIDAD
            labels_cantidad = ['Total Presupuestado (Cant.)']
            values_cantidad = [total_cantidad_presupuestada_general]
            measures_cantidad = ['absolute']
            texts_cantidad = [f"{total_cantidad_presupuestada_general:,.2f}"]

            # Añadir variaciones por obra (solo cantidad, solo si hay variación en la obra)
            # Filter out zero variations and sort
            reporte_variacion_obras_sorted_cantidad = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Cantidad']) > 0.01].sort_values('Variacion_Total_Cantidad', ascending=False)

            for index, row in reporte_variacion_obras_sorted_cantidad.iterrows():
                 # Use shorter label for graph
                 obra_label = row['Nombre_Obra']
                 if len(obra_label) > 15: # Arbitrary length limit
                      obra_label = obra_label[:12] + '...'
                 labels_cantidad.append(f"Var Cant: {obra_label}") # Use Nombre_Obra

                 values_cantidad.append(row['Variacion_Total_Cantidad'])
                 measures_cantidad.append('relative')
                 texts_cantidad.append(f"{row['Variacion_Total_Cantidad']:,.2f}")

            # Añadir el total asignado
            labels_cantidad.append('Total Asignado (Cant.)')
            values_cantidad.append(total_cantidad_asignada_general)
            measures_cantidad.append('total')
            texts_cantidad.append(f"{total_cantidad_asignada_general:,.2f}")

            # Check again after adding variation bars if there's more than just the start and end points
            if len(labels_cantidad) > 2 or (len(labels_cantidad) == 2 and values_cantidad[0] != values_cantidad[1]): # More than just start/end OR start!=end
                fig_total_variacion_cantidad = go.Figure(go.Waterfall(
                    name = "Variación Total Cantidad",
                    orientation = "v",
                    measure = measures_cantidad,
                    x = labels_cantidad,
                    textposition = "outside",
                    text = texts_cantidad,
                    y = values_cantidad,
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                ))

                fig_total_variacion_cantidad.update_layout(
                    title = 'Variación Total de Cantidades Materiales (Presupuesto vs Real) - Todas las Obras',
                    showlegend = False,
                    yaxis_title="Cantidad",
                    margin=dict(l=20, r=20, t=60, b=20),
                    height=600
                )

                st.plotly_chart(fig_total_variacion_cantidad, use_container_width=True)
            else:
                 st.info("La cantidad total presupuestada es igual a la cantidad total asignada o la variación total es insignificante. No hay variación de cantidad para mostrar en el gráfico.")
        else:
            st.info("No hay cantidad presupuestada ni asignada total para mostrar el gráfico de variación de cantidad.")


# --- Main App Logic (No Auth) ---

# The authentication block is removed.
# The app now proceeds directly to rendering the sidebar and page content.

# --- Sidebar Navigation ---
with st.sidebar:
    st.title("Menú Principal")

    # Define the pages for navigation
    pages = {
        "Dashboard Principal": "dashboard", # Simple placeholder
        "Gestión de Equipos": "equipos",
        "Registro de Consumibles": "consumibles",
        "Registro de Costos Equipos": "costos_equipos",
        "Reportes Mina (Consumo/Costo)": "reportes_mina",
        "Variación Costos Flota": "variacion_costos_flota",
        "--- Gestión de Obras y Materiales ---": None, # Separador (Not a real page key)
        "Gestión de Obras (Proyectos)": "gestion_obras",
        "Reporte Presupuesto Total Obras": "reporte_presupuesto_total_obras",
        "Gestión Compras y Asignación": "compras_asignacion",
        "Reporte Variación Total Obras": "reporte_variacion_total_obras",
    }

    # Use a stable key for the radio button
    selected_page_key = st.radio("Ir a:", list(pages.keys()), index=0, key="main_navigation_radio")
    selected_page = pages[selected_page_key]


# --- Main Content Area based on selection ---
# This code executes outside the 'with st.sidebar:' block

# Call the function of the selected page
if selected_page == "dashboard":
    st.title("Dashboard Principal")
    st.write(f"Bienvenido al sistema de gestión para la empresa proveedora de minería.")
    st.info("Seleccione una opción del menú lateral para comenzar.")
    st.markdown("---")
    st.subheader("Resumen Rápido")
    # Here you can add quick key metrics
    total_equipos = len(st.session_state.df_equipos)
    total_obras = len(st.session_state.df_proyectos)
    # Ensure cost calculation functions are robust to empty DFs
    total_presupuesto_materiales = calcular_costo_presupuestado(st.session_state.df_presupuesto_materiales.copy())['Costo_Presupuestado'].sum() if not st.session_state.df_presupuesto_materiales.empty else 0
    total_comprado_materiales = calcular_costo_compra(st.session_state.df_compras_materiales.copy())['Costo_Compra'].sum() if not st.session_state.df_compras_materiales.empty else 0

    col_summary1, col_summary2, col_summary3, col_summary4 = st.columns(4)
    with col_summary1:
        st.metric("Total Equipos", total_equipos)
    with col_summary2:
         st.metric("Total Obras", total_obras)
    with col_summary3:
         st.metric("Presupuesto Materiales Total", f"${total_presupuesto_materiales:,.0f}")
    with col_summary4:
         st.metric("Compras Materiales Total", f"${total_comprado_materiales:,.0f}")


elif selected_page == "equipos":
    page_equipos()
elif selected_page == "consumibles":
    page_consumibles()
elif selected_page == "costos_equipos":
    page_costos_equipos()
elif selected_page == "reportes_mina":
    page_reportes_mina()
elif selected_page == "variacion_costos_flota":
    page_variacion_costos_flota()
elif selected_page == "gestion_obras":
    page_gestion_obras()
elif selected_page == "reporte_presupuesto_total_obras":
    page_reporte_presupuesto_total_obras()
elif selected_page == "compras_asignacion":
    page_compras_asignacion()
elif selected_page == "reporte_variacion_total_obras":
    page_reporte_variacion_total_obras()
elif selected_page is None:
    # If selected_page is None (for separators like "---"), render a placeholder or nothing
    st.empty() # Ensure the main area is empty
    # Optional: show a generic message if a separator is "selected" (though not clickable as a page)
    # st.info("Seleccione una página válida del menú lateral.")
