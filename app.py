import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import sqlite3 # Import the sqlite3 library
import time # Used for generating unique IDs
import numpy as np # Import numpy for pd.NA comparisons, etc.
import datetime # Import datetime module

# --- Configuración Inicial ---
st.set_page_config(layout="wide", page_title="Gestión de Equipos y Obras (Minería)")

# --- Archivos de Datos (Usaremos una base de datos SQLite) ---
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DATABASE_FILE = os.path.join(DATA_DIR, "app_data.db")

# Define table names for SQLite
TABLE_FLOTAS = "flotas" # New table for Fleets
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


# --- Define expected columns and their pandas dtypes for each table ---
# This is used to create empty dataframes if tables don't exist and for type hints
TABLE_COLUMNS = {
    TABLE_FLOTAS: {'ID_Flota': 'object', 'Nombre_Flota': 'object'},
    TABLE_EQUIPOS: {'Interno': 'object', 'Patente': 'object', 'ID_Flota': 'object'}, # Added ID_Flota to Equipos
    TABLE_CONSUMO: {'Interno': 'object', 'Fecha': 'object', 'Consumo_Litros': 'float64', 'Horas_Trabajadas': 'float64', 'Kilometros_Recorridos': 'float64'},
    TABLE_COSTOS_SALARIAL: {'Interno': 'object', 'Fecha': 'object', 'Monto_Salarial': 'float64'},
    TABLE_GASTOS_FIJOS: {'Interno': 'object', 'Fecha': 'object', 'Tipo_Gasto_Fijo': 'object', 'Monto_Gasto_Fijo': 'float64', 'Descripcion': 'object'},
    TABLE_GASTOS_MANTENIMIENTO: {'Interno': 'object', 'Fecha': 'object', 'Tipo_Mantenimiento': 'object', 'Monto_Mantenimiento': 'float64', 'Descripcion': 'object'},
    TABLE_PRECIOS_COMBUSTIBLE: {'Fecha': 'object', 'Precio_Litro': 'float64'},
    TABLE_PROYECTOS: {'ID_Obra': 'object', 'Nombre_Obra': 'object', 'Responsable': 'object'},
    TABLE_PRESUPUESTO_MATERIALES: {'ID_Obra': 'object', 'Material': 'object', 'Cantidad_Presupuestada': 'float64', 'Precio_Unitario_Presupuestado': 'float64', 'Costo_Presupuestado': 'float64'}, # Include calculated column in definition
    TABLE_COMPRAS_MATERIALES: {'ID_Compra': 'object', 'Fecha_Compra': 'object', 'Material': 'object', 'Cantidad_Comprada': 'float64', 'Precio_Unitario_Comprado': 'float64', 'Costo_Compra': 'float64'}, # Include calculated column
    TABLE_ASIGNACION_MATERIALES: {'ID_Asignacion': 'object', 'Fecha_Asignacion': 'object', 'ID_Obra': 'object', 'Material': 'object', 'Cantidad_Asignada': 'float64', 'Precio_Unitario_Asignado': 'float64', 'Costo_Asignado': 'float64'}, # Include calculated column
}

# Define which columns should be treated as dates
DATE_COLUMNS = {
    TABLE_CONSUMO: 'Fecha',
    TABLE_COSTOS_SALARIAL: 'Fecha',
    TABLE_GASTOS_FIJOS: 'Fecha',
    TABLE_GASTOS_MANTENIMIENTO: 'Fecha',
    TABLE_PRECIOS_COMBUSTIBLE: 'Fecha',
    TABLE_COMPRAS_MATERIALES: 'Fecha_Compra',
    TABLE_ASIGNACION_MATERIALES: 'Fecha_Asignacion',
}


# --- Helper function to get SQLite connection ---
@st.cache_resource # Cache the database connection to avoid reconnecting on each rerun
def get_db_conn():
    """Establece y retorna una conexión a la base de datos SQLite."""
    # Allows the connection to be used from different threads.
    # Necessary for environments like Streamlit that might use different threads per request.
    # Crucially, also ensures consistency with data editor interactions.
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    # Optional: Configure to use WAL mode for better concurrency if needed (requires app-level lock management for writes).
    # conn.execute('PRAGMA journal_mode=WAL;')
    # Or use a locking mechanism for writes if strictly necessary in a highly concurrent setup.
    # For a single-user Streamlit session (common setup), check_same_thread=False is usually enough.
    return conn

# --- Functions to Load/Save Data using SQLite ---
def load_table(db_file, table_name):
    """Carga datos de una tabla SQLite en un DataFrame."""
    conn = get_db_conn() # Use cached connection
    df = pd.DataFrame() # Initialize empty DataFrame
    try:
        cursor = conn.cursor()
        # Check if table exists robustly, accounting for case sensitivity etc.
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE;", (table_name,))
        table_exists = cursor.fetchone() is not None

        expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
        expected_cols = list(expected_cols_dict.keys())

        if table_exists:
            # Read the actual columns in the database table
            db_cols_info = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            db_cols = [col_info[1] for col_info in db_cols_info]

            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)

            # Ensure df columns match expected columns, adding missing ones
            for col in expected_cols:
                if col not in df.columns:
                    # Add missing column with appropriate default
                    dtype = expected_cols_dict.get(col)
                    if dtype == 'object':
                        df[col] = pd.NA # Use pd.NA for string/object columns
                    elif 'float' in dtype or 'int' in dtype:
                        df[col] = 0.0 # Use 0 for numeric columns
                    else:
                         df[col] = None # Default to None otherwise

            # Ensure only expected columns are kept, and in the expected order (important for equals comparison later)
            df = df[expected_cols]


            # Convert date columns *after* ensuring they exist and df is loaded
            if table_name in DATE_COLUMNS:
                date_col = DATE_COLUMNS[table_name]
                if date_col in df.columns:
                    # Convert potential strings/objects to datetime64[ns]
                    # Use errors='coerce' to turn invalid/missing dates into NaT
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    # Now replace NaT with pandas NA specifically for date columns, which plays better with data_editor
                    df[date_col] = df[date_col].where(pd.notna(df[date_col]), pd.NA) # Use pd.NA for consistency
                    # If you strictly need datetime.date objects *internally* outside data_editor:
                    # df[date_col] = df[date_col].dt.date.where(pd.notna(df[date_col].dt.date)), None # Revert if needed

        else:
            # Table doesn't exist, create an empty DataFrame with expected columns
            # St.warning(f"La tabla '{table_name}' no existe. Creando DataFrame vacío.") # Can be noisy
            df = pd.DataFrame(columns=expected_cols)
            # Set initial types where possible, although pandas handles this often
            for col, dtype in expected_cols_dict.items():
                 if dtype == 'object':
                      df[col] = df[col].astype(pd.StringDtype()) # Use StringDtype for explicit nullable string
                 # No need to explicitly set float/int types on empty DF, they infer later or on append

    except sqlite3.Error as e:
        st.error(f"Error al leer la tabla '{table_name}' de la base de datos: {e}")
        # Return empty DataFrame with expected columns on error
        expected_cols = list(TABLE_COLUMNS.get(table_name, {}).keys())
        df = pd.DataFrame(columns=expected_cols)
    except Exception as e:
         st.error(f"Error general al cargar la tabla '{table_name}': {e}")
         expected_cols = list(TABLE_COLUMNS.get(table_name, {}).keys())
         df = pd.DataFrame(columns=expected_cols)
    finally:
        # No need to close conn if it's cached with @st.cache_resource
        pass

    return df


def save_table(df, db_file, table_name):
    """Guarda un DataFrame en una tabla SQLite (reemplazando la tabla si existe)."""
    conn = get_db_conn() # Use cached connection
    try:
        df_to_save = df.copy() # Work on a copy

        # Ensure all expected columns are present in the DataFrame before saving.
        expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
        for col, dtype in expected_cols_dict.items():
            if col not in df_to_save.columns:
                # Add missing column with default value
                 if dtype == 'object':
                      df_to_save[col] = pd.NA
                 elif 'float' in dtype or 'int' in dtype:
                      df_to_save[col] = 0.0
                 else:
                     df_to_save[col] = None


        # Ensure only expected columns are kept, and in the expected order for schema clarity
        df_to_save = df_to_save[list(expected_cols_dict.keys())]


        # Convert date columns to string format 'YYYY-MM-DD' before saving
        if table_name in DATE_COLUMNS:
            date_col = DATE_COLUMNS[table_name]
            if date_col in df_to_save.columns:
                 # Convert to datetime first (handles date objects, pd.NA, etc.), then format as string
                 # Fill NaT (invalid dates) or pandas NA dates with None, which saves as SQL NULL
                 # .dt.strftime('%Y-%m-%d') returns NaT as NaN, then fillna(None) makes it Python None for SQL NULL
                 df_to_save[date_col] = pd.to_datetime(df_to_save[date_col], errors='coerce').dt.strftime('%Y-%m-%d').fillna(None)


        # Ensure any pd.NA/None values in object columns are saved as None (NULL in SQL)
        # .astype(pd.StringDtype()) often helps here, but explicit replace is also safe
        for col, dtype in expected_cols_dict.items():
             if dtype == 'object' and col in df_to_save.columns:
                  # Replace pandas NA or empty-like strings with None
                  df_to_save[col] = df_to_save[col].replace({np.nan: None, pd.NA: None, '': None})


        # Use if_exists='replace' to overwrite the table
        df_to_save.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit() # Commit changes

    except sqlite3.Error as e:
        st.error(f"Error al guardar la tabla '{table_name}' en la base de datos: {e}")
        if conn:
             try:
                  conn.rollback() # Rollback changes if save failed
             except Exception as rb_e:
                  st.error(f"Error durante el rollback: {rb_e}")
    except Exception as e:
         st.error(f"Error general al guardar la tabla '{table_name}': {e}")
         if conn:
              try:
                   conn.rollback() # Rollback changes if save failed
              except Exception as rb_e:
                   st.error(f"Error durante el rollback: {rb_e}")
    finally:
        pass # Connection is cached


# --- Load all DataFrames on startup (if not in session_state) ---
# Ensure a default empty DataFrame structure if table is missing or empty
def get_initialized_dataframe(table_name):
    """Loads table or creates empty DataFrame with expected columns and dtypes."""
    df = load_table(DATABASE_FILE, table_name)
    expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
    expected_cols = list(expected_cols_dict.keys())

    if df.empty:
        df = pd.DataFrame(columns=expected_cols)
        # Set basic column types on the empty DataFrame for robustness, especially object/string types
        for col, dtype in expected_cols_dict.items():
             if dtype == 'object':
                  df[col] = df[col].astype(pd.StringDtype())
             # For numbers, pandas often infers on append


    # --- Add placeholders for missing calculated columns in old data if needed ---
    # This was previously done implicitly in load_table but better ensure here
    if table_name == TABLE_PRESUPUESTO_MATERIALES and 'Costo_Presupuestado' not in df.columns:
         df['Costo_Presupuestado'] = 0.0
         df = calcular_costo_presupuestado(df) # Calculate based on existing data
    if table_name == TABLE_COMPRAS_MATERIALES and 'Costo_Compra' not in df.columns:
         df['Costo_Compra'] = 0.0
         # Add placeholder IDs if missing for old data structure without IDs
         if 'ID_Compra' not in df.columns or (df['ID_Compra'].isnull() | (df['ID_Compra'].astype(str) == '')).all():
               df['ID_Compra'] = [f"COMPRA_MIG_{i}_{int(time.time()*1000)}" for i in range(len(df))] # Generate IDs for old rows
         df = calcular_costo_compra(df) # Calculate
    if table_name == TABLE_ASIGNACION_MATERIALES and 'Costo_Asignado' not in df.columns:
         df['Costo_Asignado'] = 0.0
         # Add placeholder IDs if missing for old data structure without IDs
         if 'ID_Asignacion' not in df.columns or (df['ID_Asignacion'].isnull() | (df['ID_Asignacion'].astype(str) == '')).all():
              df['ID_Asignacion'] = [f"ASIG_MIG_{i}_{int(time.time()*1000)}" for i in range(len(df))] # Generate IDs for old rows
         df = calcular_costo_asignado(df) # Calculate

    # Ensure ID columns exist even if empty (e.g., Flotas, Equipos, Proyectos)
    if 'ID_Flota' in expected_cols_dict and 'ID_Flota' not in df.columns:
         df['ID_Flota'] = pd.NA # Consistent missing value for nullable ID
    if 'ID_Obra' in expected_cols_dict and 'ID_Obra' not in df.columns:
         df['ID_Obra'] = pd.NA # Consistent missing value for nullable ID


    return df


if 'df_flotas' not in st.session_state:
    st.session_state.df_flotas = get_initialized_dataframe(TABLE_FLOTAS)

if 'df_equipos' not in st.session_state:
    st.session_state.df_equipos = get_initialized_dataframe(TABLE_EQUIPOS)

if 'df_consumo' not in st.session_state:
    st.session_state.df_consumo = get_initialized_dataframe(TABLE_CONSUMO)

if 'df_costos_salarial' not in st.session_state:
    st.session_state.df_costos_salarial = get_initialized_dataframe(TABLE_COSTOS_SALARIAL)

if 'df_gastos_fijos' not in st.session_state:
    st.session_state.df_gastos_fijos = get_initialized_dataframe(TABLE_GASTOS_FIJOS)

if 'df_gastos_mantenimiento' not in st.session_state:
    st.session_state.df_gastos_mantenimiento = get_initialized_dataframe(TABLE_GASTOS_MANTENIMIENTO)

if 'df_precios_combustible' not in st.session_state:
    st.session_state.df_precios_combustible = get_initialized_dataframe(TABLE_PRECIOS_COMBUSTIBLE)

if 'df_proyectos' not in st.session_state:
    st.session_state.df_proyectos = get_initialized_dataframe(TABLE_PROYECTOS)

if 'df_presupuesto_materiales' not in st.session_state:
    st.session_state.df_presupuesto_materiales = get_initialized_dataframe(TABLE_PRESUPUESTO_MATERIALES)

if 'df_compras_materiales' not in st.session_state:
    st.session_state.df_compras_materiales = get_initialized_dataframe(TABLE_COMPRAS_MATERIALES)

if 'df_asignacion_materiales' not in st.session_state:
    st.session_state.df_asignacion_materiales = get_initialized_dataframe(TABLE_ASIGNACION_MATERIALES)


# --- Helper function to calculate costs ---
# Moved these after df loading so they are available.
# Ensure numeric conversion is done inside to handle potential 'object' type before calculation.
def calcular_costo_presupuestado(df):
    """Calcula el costo total presupuestado por fila."""
    # Ensure columns exist and are numeric, default to 0 for calculation if missing or invalid
    cantidad = pd.to_numeric(df.get('Cantidad_Presupuestada', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    precio_unitario = pd.to_numeric(df.get('Precio_Unitario_Presupuestado', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    df['Costo_Presupuestado'] = cantidad * precio_unitario
    return df

def calcular_costo_compra(df):
    """Calcula el costo total de compra por fila."""
    # Ensure columns exist and are numeric, default to 0 for calculation
    cantidad = pd.to_numeric(df.get('Cantidad_Comprada', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    precio_unitario = pd.to_numeric(df.get('Precio_Unitario_Comprado', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    df['Costo_Compra'] = cantidad * precio_unitario
    return df

def calcular_costo_asignado(df):
    """Calcula el costo total asignado por fila."""
    # Ensure columns exist and are numeric, default to 0 for calculation
    cantidad = pd.to_numeric(df.get('Cantidad_Asignada', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    precio_unitario = pd.to_numeric(df.get('Precio_Unitario_Asignado', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0)
    df['Costo_Asignado'] = cantidad * precio_unitario
    return df


# Re-calculate costs after loading, in case data was manually edited or missing the calculated column
# These might be redundant if get_initialized_dataframe already does it, but doesn't hurt for safety
if not st.session_state.df_presupuesto_materiales.empty:
    st.session_state.df_presupuesto_materiales = calcular_costo_presupuestado(st.session_state.df_presupuesto_materiales)
if not st.session_state.df_compras_materiales.empty:
    st.session_state.df_compras_materiales = calcular_costo_compra(st.session_state.df_compras_materiales)
if not st.session_state.df_asignacion_materiales.empty:
    st.session_state.df_asignacion_materiales = calcular_costo_asignado(st.session_state.df_asignacion_materiales)


# --- Functions for each "Page" ---

def page_flotas():
    st.title("Gestión de Flotas")
    st.write("Aquí puedes añadir, editar y eliminar flotas.")

    st.subheader("Añadir Nueva Flota")
    with st.form("form_add_flota", clear_on_submit=True):
        nombre_flota = st.text_input("Nombre de la Flota").strip()
        submitted = st.form_submit_button("Añadir Flota")
        if submitted:
            if not nombre_flota:
                st.warning("Por favor, complete el Nombre de la Flota.")
            elif nombre_flota in st.session_state.df_flotas['Nombre_Flota'].values:
                 st.warning(f"La flota '{nombre_flota}' ya existe.")
            else:
                # Generate simple unique ID (using timestamp + counter for robustness)
                # Using current length as a partial uniqueness helper (within session add)
                current_ids = set(st.session_state.df_flotas['ID_Flota'].astype(str)) # Get existing IDs as strings for comparison
                base_id = f"FLOTA_{int(time.time() * 1000)}"
                id_flota = base_id
                counter = 0
                while id_flota in current_ids:
                    counter += 1
                    id_flota = f"{base_id}_{counter}"

                new_flota = pd.DataFrame([{'ID_Flota': id_flota, 'Nombre_Flota': nombre_flota}])
                # Ensure all expected columns exist in new dataframe before concat
                new_flota = get_initialized_dataframe(TABLE_FLOTAS).append(new_flota, ignore_index=True).tail(1) # Add to a base empty df then take the new row
                new_flota['ID_Flota'] = id_flota # Re-assert ID after potentially losing it
                new_flota['Nombre_Flota'] = nombre_flota # Re-assert name


                st.session_state.df_flotas = pd.concat([st.session_state.df_flotas, new_flota], ignore_index=True)
                save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS) # Save to DB
                st.success(f"Flota '{nombre_flota}' añadida con ID: {id_flota}.")
                st.experimental_rerun() # Rerun to update the list and selectboxes on other pages


    st.subheader("Lista de Flotas")
    if st.session_state.df_flotas.empty:
         st.info("No hay flotas registradas aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar flotas. Use el ícono de papelera para eliminar filas.")
        df_flotas_editable = st.session_state.df_flotas.copy()

        df_flotas_edited = st.data_editor(
            df_flotas_editable,
            key="data_editor_flotas",
            num_rows="dynamic", # Allow adding/deleting rows
            column_config={
                 "ID_Flota": st.column_config.TextColumn("ID Flota", disabled=True), # Prevent editing ID
                 "Nombre_Flota": st.column_config.TextColumn("Nombre Flota", required=True),
            }
        )

        # Logic to save changes from data_editor
        # Use deep comparison including index and order if exact match is needed,
        # otherwise checking length and column values (after sorting/reindexing if needed) is sufficient for 'replace'.
        # Simple comparison is often enough given the 'replace' strategy.
        if not df_flotas_edited.equals(st.session_state.df_flotas):
             st.session_state.df_flotas = df_flotas_edited.copy() # Update session state with the edited data

             if st.button("Guardar Cambios en Lista de Flotas", key="save_flotas_button"):
                  # Validate before saving (e.g., Nombre_Flota not empty, no duplicate names)
                  # Drop rows that might have been added empty by data_editor before validating
                  df_to_save = st.session_state.df_flotas.dropna(subset=['Nombre_Flota'], how='all').copy()
                  df_to_save = df_to_save[df_to_save['Nombre_Flota'].astype(str).str.strip() != '']

                  if df_to_save['Nombre_Flota'].isnull().any() or (df_to_save['Nombre_Flota'].astype(str).str.strip() == '').any():
                      st.error("Error: Los nombres de las flotas no pueden estar vacíos.")
                  elif df_to_save['Nombre_Flota'].duplicated().any():
                       st.error("Error: Hay nombres de flotas duplicados en la lista. Por favor, corrija los duplicados antes de guardar.")
                  else:
                      # Add ID for any potentially new rows added directly in the editor without an ID
                      new_row_mask = df_to_save['ID_Flota'].isnull() | (df_to_save['ID_Flota'].astype(str).str.strip() == '')
                      if new_row_mask.any():
                           existing_ids = set(df_to_save['ID_Flota'].dropna().astype(str))
                           new_ids = []
                           for i in range(new_row_mask.sum()):
                                base_id = f"FLOTA_EDIT_{int(time.time() * 1000)}_{i}"
                                unique_id = base_id
                                counter = 0
                                while unique_id in existing_ids:
                                    counter += 1
                                    unique_id = f"{base_id}_{counter}"
                                new_ids.append(unique_id)
                                existing_ids.add(unique_id) # Add to set for subsequent checks

                           df_to_save.loc[new_row_mask, 'ID_Flota'] = new_ids
                           st.session_state.df_flotas = df_to_save # Update session state with generated IDs

                      save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS) # Save to DB
                      st.success("Cambios en la lista de flotas guardados.")
                      st.experimental_rerun() # Rerun to update selectboxes on other pages
             else:
                 st.info("Hay cambios sin guardar en la lista de flotas.") # Feedback al usuario


def page_equipos():
    st.title("Gestión de Equipos de Mina")
    st.write("Aquí puedes añadir, editar y eliminar equipos.")

    st.subheader("Añadir Nuevo Equipo")
    # Get list of available fleets for the selectbox
    flotas_disponibles = st.session_state.df_flotas[['ID_Flota', 'Nombre_Flota']].copy()
    # Add an option for no fleet assignment using None/pd.NA
    # Create mapping for display (handle None, pd.NA, '' mapping to "Sin Flota")
    flota_id_to_name = st.session_state.df_flotas.set_index('ID_Flota')['Nombre_Flota'].to_dict()
    flota_id_to_name[pd.NA] = "Sin Flota"
    flota_id_to_name[''] = "Sin Flota" # Handle empty strings from user input/editor
    flota_id_to_name[None] = "Sin Flota" # Handle None


    # Create selectbox options: first is "Sin Flota", then others
    flota_select_options = [{"ID": pd.NA, "Label": "Sin Flota"}] + \
                           [{"ID": row['ID_Flota'], "Label": f"{row['Nombre_Flota']} (ID: {row['ID_Flota']})"}
                            for index, row in flotas_disponibles.iterrows()]

    # Check if there are any fleets, if not, the only option is "Sin Flota"
    if not flota_select_options:
        flota_select_options = [{"ID": pd.NA, "Label": "Sin Flota"}] # Ensure at least "Sin Flota" is available

    flota_option_labels = [opt['Label'] for opt in flota_select_options]


    with st.form("form_add_equipo", clear_on_submit=True):
        interno = st.text_input("Interno del Equipo").strip()
        patente = st.text_input("Patente").strip()

        selected_flota_label = st.selectbox(
            "Seleccionar Flota:",
            options=flota_option_labels,
            index=0, # Default to "Sin Flota"
            key="add_equipo_flota_select"
        )

        # Find the corresponding ID_Flota from the selected label
        selected_flota_id = next((opt['ID'] for opt in flota_select_options if opt['Label'] == selected_flota_label), pd.NA) # Default to pd.NA if label not found


        submitted = st.form_submit_button("Añadir Equipo")
        if submitted:
            if not interno or not patente:
                st.warning("Por favor, complete Interno y Patente.")
            elif interno in st.session_state.df_equipos['Interno'].values:
                st.warning(f"Ya existe un equipo con Interno {interno}")
            else:
                new_equipo_data = {'Interno': interno, 'Patente': patente, 'ID_Flota': selected_flota_id}
                new_equipo = pd.DataFrame([new_equipo_data])

                # Ensure all expected columns exist in the new row DF before concatenation
                new_equipo_processed = get_initialized_dataframe(TABLE_EQUIPOS).append(new_equipo, ignore_index=True).tail(1)
                # Restore explicit values as appending might coerce types/lose pd.NA status if original is empty
                new_equipo_processed['Interno'] = interno
                new_equipo_processed['Patente'] = patente
                new_equipo_processed['ID_Flota'] = selected_flota_id # Ensure pd.NA is kept if selected


                st.session_state.df_equipos = pd.concat([st.session_state.df_equipos, new_equipo_processed], ignore_index=True)
                save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS) # Save to DB
                flota_name_display = flota_id_to_name.get(selected_flota_id, "Sin Flota") # Use the robust map
                st.success(f"Equipo {interno} ({patente}) añadido a flota '{flota_name_display}'.")
                st.experimental_rerun() # Rerun to update editor view and selectboxes

    st.subheader("Lista de Equipos")
    st.info("Edite la tabla siguiente para modificar o eliminar equipos. Use el ícono de papelera para eliminar filas.")
    # Usar data_editor para permitir edición directa, incluyendo la Flota
    df_equipos_editable = st.session_state.df_equipos.copy()

    # Prepare options for the Fleet SelectboxColumn in data_editor
    # The options should just be the ID_Flota values PLUS the representation for "Sin Flota" (pd.NA)
    flota_ids_for_editor = st.session_state.df_flotas['ID_Flota'].tolist()
    # Add pd.NA as a valid option for "Sin Flota". Need to be careful with duplicates if pd.NA already exists
    # Let's ensure only unique, valid IDs + pd.NA
    flota_editor_options_unique = [pd.NA] + [id for id in flota_ids_for_editor if pd.notna(id)]
    flota_editor_options = list(dict.fromkeys(flota_editor_options_unique)) # Get unique while preserving order [pd.NA, ID1, ID2...]

    # Re-use the robust flota_id_to_name mapping created earlier
    # Need to update the map if new fleets were just added (experimental_rerun handles this usually)
    flota_id_to_name_editor = st.session_state.df_flotas.set_index('ID_Flota')['Nombre_Flota'].to_dict()
    flota_id_to_name_editor[pd.NA] = "Sin Flota"
    flota_id_to_name_editor[''] = "Sin Flota" # Handle empty strings
    flota_id_to_name_editor[None] = "Sin Flota" # Handle None

    # Safer format_func that handles various null representations explicitly
    def format_flota_for_editor(id_value):
        # pd.isna() is True for np.nan, pd.NA, and None
        if pd.isna(id_value) or str(id_value).strip() == '': # Also treat empty string as "Sin Flota"
             return "Sin Flota"
        return flota_id_to_name_editor.get(id_value, f"ID Desconocido ({id_value})") # Fallback for unknown IDs


    df_equipos_edited = st.data_editor(
        df_equipos_editable,
        key="data_editor_equipos",
        num_rows="dynamic", # Allow adding/deleting rows
        column_config={
             "Interno": st.column_config.TextColumn("Interno", required=True),
             "Patente": st.column_config.TextColumn("Patente", required=True),
             "ID_Flota": st.column_config.SelectboxColumn(
                 "Flota",
                 options=flota_editor_options, # Provide the list of valid IDs + pd.NA
                 required=False, # Fleet assignment is optional
                 # Use the robust mapping for display in the dropdown
                 format_func=format_flota_for_editor
             )
        }
    )

    # Logic to save changes from data_editor
    if not df_equipos_edited.equals(st.session_state.df_equipos):
         st.session_state.df_equipos = df_equipos_edited.copy() # Update session state

         if st.button("Guardar Cambios en Lista de Equipos", key="save_equipos_button"):
              # Validate before saving
              df_to_save = st.session_state.df_equipos.dropna(subset=['Interno', 'Patente'], how='all').copy()
              df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Patente'].astype(str).str.strip() != '')] # Remove rows where essential fields are empty

              if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any() or df_to_save['Patente'].isnull().any() or (df_to_save['Patente'].astype(str).str.strip() == '').any():
                   st.error("Error: Los campos 'Interno' y 'Patente' no pueden estar vacíos.")
              elif df_to_save['Interno'].duplicated().any():
                  st.error("Error: Hay Internos de Equipo duplicados en la lista. Por favor, corrija los duplicados antes de guardar.")
              else:
                  # Validate if entered ID_Flota values actually exist in df_flotas (excluding NA/None/empty)
                  # Note: SelectboxColumn already restricts choices to existing IDs + pd.NA/None
                  # However, direct text input or copy-paste might introduce invalid IDs if not using selectbox
                  # But st.data_editor with column_config applies the selectbox constraint on direct input/paste too for this column type.
                  # So, the validation `if not df_to_save['ID_Flota'].apply(lambda x: pd.isna(x) or x in st.session_state.df_flotas['ID_Flota'].values).all():` might be redundant but kept for safety if data is manipulated externally.

                  save_table(df_to_save, DATABASE_FILE, TABLE_EQUIPOS) # Save the cleaned and validated DF
                  st.success("Cambios en la lista de equipos guardados.")
                  st.experimental_rerun()


         else:
             st.info("Hay cambios sin guardar en la lista de equipos.")


def page_consumibles():
    st.title("Registro de Consumibles por Equipo")
    st.write("Aquí puedes registrar el consumo de combustible, horas y kilómetros por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].unique().tolist()
    # Filter out any None/NaN/empty string internos
    internos_disponibles = [i for i in internos_disponibles if pd.notna(i) and str(i).strip() != '']

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero para registrar consumibles.")
        return

    st.subheader("Añadir Registro de Consumo")
    with st.form("form_add_consumo", clear_on_submit=True):
        interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles)
        fecha = st.date_input("Fecha", value=datetime.date.today()) # Default to today
        consumo_litros = st.number_input("Consumo en Litros de Combustible", min_value=0.0, format="%.2f")
        horas_trabajadas = st.number_input("Cantidad de Horas Trabajadas", min_value=0.0, format="%.2f")
        kilometros_recorridos = st.number_input("Cantidad de Kilómetros Recorridos", min_value=0.0, format="%.2f")

        submitted = st.form_submit_button("Registrar Consumo")
        if submitted:
            # Basic validation
            if not interno_seleccionado or not fecha:
                 st.warning("Por favor, complete Equipo y Fecha.")
            elif consumo_litros <= 0 and horas_trabajadas <= 0 and kilometros_recorridos <= 0:
                 st.warning("Por favor, ingrese al menos un valor de Consumo, Horas o Kilómetros mayor a cero.")
            else: # All validations pass
                 new_consumo_data = {
                    'Interno': interno_seleccionado,
                    'Fecha': fecha, # Will be handled by save_table (date obj to string)
                    'Consumo_Litros': consumo_litros,
                    'Horas_Trabajadas': horas_trabajadas,
                    'Kilometros_Recorridos': kilometros_recorridos
                 }
                 new_consumo = pd.DataFrame([new_consumo_data])
                 # Ensure all expected columns exist
                 new_consumo_processed = get_initialized_dataframe(TABLE_CONSUMO).append(new_consumo, ignore_index=True).tail(1)
                 # Restore specific values after potential append conversions
                 new_consumo_processed.update(new_consumo) # Update non-NaN/NaT values


                 st.session_state.df_consumo = pd.concat([st.session_state.df_consumo, new_consumo_processed], ignore_index=True)
                 save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO) # Save to DB
                 st.success("Registro de consumo añadido.")
                 st.experimental_rerun() # Rerun to update editor view


    st.subheader("Registros de Consumo Existente")
    if st.session_state.df_consumo.empty:
         st.info("No hay registros de consumo aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera para eliminar filas.")
        df_consumo_editable = st.session_state.df_consumo.copy()
        # Ensure date column is datetime64[ns] for data_editor compatibility
        if 'Fecha' in df_consumo_editable.columns:
            df_consumo_editable['Fecha'] = pd.to_datetime(df_consumo_editable['Fecha'], errors='coerce') # NaT handled by data_editor as empty date


        df_consumo_edited = st.data_editor(
             df_consumo_editable,
             key="data_editor_consumo",
             num_rows="dynamic", # Allow adding/deleting rows
             column_config={
                  "Fecha": st.column_config.DateColumn("Fecha", required=True),
                  "Interno": st.column_config.TextColumn("Interno", required=True),
                  "Consumo_Litros": st.column_config.NumberColumn("Consumo Litros", min_value=0.0, format="%.2f", required=True),
                  "Horas_Trabajadas": st.column_config.NumberColumn("Horas Trabajadas", min_value=0.0, format="%.2f", required=True),
                  "Kilometros_Recorridos": st.column_config.NumberColumn("Kilómetros Recorridos", min_value=0.0, format="%.2f", required=True),
             }
         )
        # Compare edited with current session state dataframe for changes
        if not df_consumo_edited.equals(st.session_state.df_consumo):
             st.session_state.df_consumo = df_consumo_edited.copy() # Update session state with edited DF

             if st.button("Guardar Cambios en Registros de Consumo", key="save_consumo_button"):
                  # Basic validation
                  df_to_save = st.session_state.df_consumo.dropna(subset=['Interno', 'Fecha', 'Consumo_Litros', 'Horas_Trabajadas', 'Kilometros_Recorridos'], how='all').copy() # Remove completely empty rows
                  # Further validation for required non-numeric fields
                  df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Fecha'].notna())]


                  if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any(): # Check after filtering
                       st.error("Error: El campo 'Interno' no puede estar vacío.")
                  elif df_to_save['Fecha'].isnull().any(): # Check after filtering (these are NaT from data_editor/coerce)
                       st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                  # Add checks for internal consistency, e.g., Interno should ideally exist in Equipos, but might not if equipment is deleted
                  # Keep required=True in data_editor and trust that it forces input
                  else:
                       save_table(df_to_save, DATABASE_FILE, TABLE_CONSUMO) # Save cleaned/validated DF
                       st.success("Cambios en registros de consumo guardados.")
                       st.experimental_rerun()


             else:
                 st.info("Hay cambios sin guardar en registros de consumo.")


def page_costos_equipos():
    st.title("Registro de Costos por Equipo")
    st.write("Aquí puedes registrar costos salariales, fijos y de mantenimiento por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].unique().tolist()
    # Filter out any None/NaN/empty string internos
    internos_disponibles = [i for i in internos_disponibles if pd.notna(i) and str(i).strip() != '']

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero para registrar costos.")
        return

    tab1, tab2, tab3 = st.tabs(["Costos Salariales", "Gastos Fijos", "Gastos Mantenimiento"])

    with tab1:
        st.subheader("Registro de Costos Salariales")
        with st.form("form_add_salarial", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="sal_int")
            fecha = st.date_input("Fecha", key="sal_fecha", value=datetime.date.today())
            monto_salarial = st.number_input("Monto Salarial", min_value=0.0, format="%.2f", key="sal_monto")
            submitted = st.form_submit_button("Registrar Costo Salarial")
            if submitted:
                if not interno_seleccionado or not fecha or monto_salarial <= 0:
                    st.warning("Por favor, complete Equipo, Fecha y Monto (mayor a cero).")
                else: # All validations pass
                    new_costo_data = {
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Monto_Salarial': monto_salarial
                    }
                    new_costo = pd.DataFrame([new_costo_data])
                    # Ensure all expected columns exist
                    new_costo_processed = get_initialized_dataframe(TABLE_COSTOS_SALARIAL).append(new_costo, ignore_index=True).tail(1)
                    new_costo_processed.update(new_costo) # Restore specific values

                    st.session_state.df_costos_salarial = pd.concat([st.session_state.df_costos_salarial, new_costo_processed], ignore_index=True)
                    save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL) # Save to DB
                    st.success("Costo salarial registrado.")
                    st.experimental_rerun() # Rerun to update editor view

        st.subheader("Registros Salariales Existente")
        if st.session_state.df_costos_salarial.empty:
             st.info("No hay registros salariales aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera para eliminar filas.")
            df_salarial_editable = st.session_state.df_costos_salarial.copy()
            if 'Fecha' in df_salarial_editable.columns:
                 df_salarial_editable['Fecha'] = pd.to_datetime(df_salarial_editable['Fecha'], errors='coerce')
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
            # Compare edited with current session state dataframe for changes
            if not df_salarial_edited.equals(st.session_state.df_costos_salarial):
                 st.session_state.df_costos_salarial = df_salarial_edited.copy() # Update session state

                 if st.button("Guardar Cambios en Registros Salariales", key="save_salarial_button"):
                      # Basic validation
                      df_to_save = st.session_state.df_costos_salarial.dropna(subset=['Interno', 'Fecha', 'Monto_Salarial'], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Fecha'].notna())]

                      if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Interno' no puede estar vacío.")
                      elif df_to_save['Fecha'].isnull().any():
                           st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                      elif df_to_save['Monto_Salarial'].isnull().any(): # Data editor required=True should handle this, but safety check
                            st.error("Error: El campo 'Monto Salarial' no puede estar vacío.")
                      else:
                           save_table(df_to_save, DATABASE_FILE, TABLE_COSTOS_SALARIAL) # Save cleaned/validated DF
                           st.success("Cambios en registros salariales guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros salariales.")


    with tab2:
        st.subheader("Registro de Gastos Fijos")
        with st.form("form_add_fijos", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="fij_int")
            fecha = st.date_input("Fecha", key="fij_fecha", value=datetime.date.today())
            tipo_gasto = st.text_input("Tipo de Gasto Fijo", key="fij_tipo").strip()
            monto_gasto = st.number_input("Monto del Gasto Fijo", min_value=0.0, format="%.2f", key="fij_monto")
            descripcion = st.text_area("Descripción (Opcional)", key="fij_desc").strip()
            submitted = st.form_submit_button("Registrar Gasto Fijo")
            if submitted:
                if not interno_seleccionado or not fecha or monto_gasto <= 0 or not tipo_gasto:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto - mayor a cero).")
                else: # All validations pass
                    new_gasto_data = {
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Tipo_Gasto_Fijo': tipo_gasto,
                       'Monto_Gasto_Fijo': monto_gasto,
                       'Descripcion': descripcion if descripcion else None # Store empty string as None/NULL
                    }
                    new_gasto = pd.DataFrame([new_gasto_data])
                    # Ensure all expected columns exist
                    new_gasto_processed = get_initialized_dataframe(TABLE_GASTOS_FIJOS).append(new_gasto, ignore_index=True).tail(1)
                    new_gasto_processed.update(new_gasto) # Restore specific values

                    st.session_state.df_gastos_fijos = pd.concat([st.session_state.df_gastos_fijos, new_gasto_processed], ignore_index=True)
                    save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS) # Save to DB
                    st.success("Gasto fijo registrado.")
                    st.experimental_rerun() # Rerun to update editor view

        st.subheader("Registros de Gastos Fijos Existente")
        if st.session_state.df_gastos_fijos.empty:
             st.info("No hay registros de gastos fijos aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera para eliminar filas.")
            df_fijos_editable = st.session_state.df_gastos_fijos.copy()
            if 'Fecha' in df_fijos_editable.columns:
                 df_fijos_editable['Fecha'] = pd.to_datetime(df_fijos_editable['Fecha'], errors='coerce')
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
            # Compare edited with current session state dataframe for changes
            if not df_fijos_edited.equals(st.session_state.df_gastos_fijos):
                 st.session_state.df_gastos_fijos = df_fijos_edited.copy() # Update session state

                 if st.button("Guardar Cambios en Registros de Gastos Fijos", key="save_fijos_button"):
                      # Basic validation
                      df_to_save = st.session_state.df_gastos_fijos.dropna(subset=['Interno', 'Fecha', 'Tipo_Gasto_Fijo', 'Monto_Gasto_Fijo'], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Fecha'].notna()) & (df_to_save['Tipo_Gasto_Fijo'].astype(str).str.strip() != '')]

                      if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Interno' no puede estar vacío.")
                      elif df_to_save['Fecha'].isnull().any():
                           st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                      elif df_to_save['Tipo_Gasto_Fijo'].isnull().any() or (df_to_save['Tipo_Gasto_Fijo'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Tipo Gasto Fijo' no puede estar vacío.")
                      elif df_to_save['Monto_Gasto_Fijo'].isnull().any():
                           st.error("Error: El campo 'Monto Gasto Fijo' no puede estar vacío.")
                      else:
                           save_table(df_to_save, DATABASE_FILE, TABLE_GASTOS_FIJOS) # Save cleaned/validated DF
                           st.success("Cambios en registros de gastos fijos guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros de gastos fijos.")


    with tab3:
        st.subheader("Registro de Gastos de Mantenimiento")
        with st.form("form_add_mantenimiento", clear_on_submit=True):
            interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="mant_int")
            fecha = st.date_input("Fecha", key="mant_fecha", value=datetime.date.today())
            tipo_mantenimiento = st.text_input("Tipo de Mantenimiento", key="mant_tipo").strip()
            monto_mantenimiento = st.number_input("Monto del Mantenimiento", min_value=0.0, format="%.2f", key="mant_monto")
            descripcion = st.text_area("Descripción (Opcional)", key="mant_desc").strip()
            submitted = st.form_submit_button("Registrar Gasto Mantenimiento")
            if submitted:
                if not interno_seleccionado or not fecha or monto_mantenimiento <= 0 or not tipo_mantenimiento:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto - mayor a cero).")
                else: # All validations pass
                    new_gasto_data = {
                       'Interno': interno_seleccionado,
                       'Fecha': fecha, # Handled by save_table
                       'Tipo_Mantenimiento': tipo_mantenimiento,
                       'Monto_Mantenimiento': monto_mantenimiento,
                       'Descripcion': descripcion if descripcion else None # Store empty string as None/NULL
                    }
                    new_gasto = pd.DataFrame([new_gasto_data])
                    # Ensure all expected columns exist
                    new_gasto_processed = get_initialized_dataframe(TABLE_GASTOS_MANTENIMIENTO).append(new_gasto, ignore_index=True).tail(1)
                    new_gasto_processed.update(new_gasto) # Restore specific values

                    st.session_state.df_gastos_mantenimiento = pd.concat([st.session_state.df_gastos_mantenimiento, new_gasto_processed], ignore_index=True)
                    save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO) # Save to DB
                    st.success("Gasto de mantenimiento registrado.")
                    st.experimental_rerun() # Rerun to update editor view

        st.subheader("Registros de Gastos de Mantenimiento Existente")
        if st.session_state.df_gastos_mantenimiento.empty:
            st.info("No hay registros de mantenimiento aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera para eliminar filas.")
            df_mantenimiento_editable = st.session_state.df_gastos_mantenimiento.copy()
            if 'Fecha' in df_mantenimiento_editable.columns:
                 df_mantenimiento_editable['Fecha'] = pd.to_datetime(df_mantenimiento_editable['Fecha'], errors='coerce')
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
            # Compare edited with current session state dataframe for changes
            if not df_mantenimiento_edited.equals(st.session_state.df_gastos_mantenimiento):
                 st.session_state.df_gastos_mantenimiento = df_mantenimiento_edited.copy() # Update session state
                 if st.button("Guardar Cambios en Registros de Mantenimiento", key="save_mantenimiento_button"):
                      # Basic validation
                      df_to_save = st.session_state.df_gastos_mantenimiento.dropna(subset=['Interno', 'Fecha', 'Tipo_Mantenimiento', 'Monto_Mantenimiento'], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Fecha'].notna()) & (df_to_save['Tipo_Mantenimiento'].astype(str).str.strip() != '')]


                      if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Interno' no puede estar vacío.")
                      elif df_to_save['Fecha'].isnull().any():
                           st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                      elif df_to_save['Tipo_Mantenimiento'].isnull().any() or (df_to_save['Tipo_Mantenimiento'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Tipo Mantenimiento' no puede estar vacío.")
                      elif df_to_save['Monto_Mantenimiento'].isnull().any():
                           st.error("Error: El campo 'Monto Mantenimiento' no puede estar vacío.")
                      else:
                           save_table(df_to_save, DATABASE_FILE, TABLE_GASTOS_Mantenimiento) # Save cleaned/validated DF
                           st.success("Cambios en registros de mantenimiento guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros de mantenimiento.")


def page_reportes_mina():
    st.title("Reportes de Mina por Fecha")
    st.write("Genera reportes de consumo y costos por equipo en un rango de fechas.")

    st.subheader("Registrar Precio del Combustible")
    st.info("Edite la tabla siguiente para modificar o eliminar precios existentes. Use el ícono de papelera para eliminar filas.")
    with st.form("form_add_precio_combustible", clear_on_submit=True):
        fecha_precio = st.date_input("Fecha del Precio", value=datetime.date.today(), key="precio_fecha_add")
        precio_litro = st.number_input("Precio por Litro", min_value=0.01, format="%.2f", key="precio_monto_add")
        submitted = st.form_submit_button("Registrar Precio")
        if submitted:
            if not fecha_precio or precio_litro <= 0:
                st.warning("Por favor, complete la fecha y el precio (mayor a cero).")
            else: # Validation passes
                # Create DataFrame for new price
                new_precio_data = {'Fecha': fecha_precio, 'Precio_Litro': precio_litro}
                new_precio = pd.DataFrame([new_precio_data])
                # Ensure all expected columns exist
                new_precio_processed = get_initialized_dataframe(TABLE_PRECIOS_COMBUSTIBLE).append(new_precio, ignore_index=True).tail(1)
                new_precio_processed.update(new_precio) # Restore specific values

                # Check if date already exists to decide whether to replace or add
                df_precios_temp = st.session_state.df_precios_combustible.copy()
                if 'Fecha' in df_precios_temp.columns:
                    # Convert both date column and input date to datetime for reliable comparison
                    df_precios_temp['Fecha_dt'] = pd.to_datetime(df_precios_temp['Fecha'], errors='coerce')
                    fecha_precio_dt = pd.to_datetime(fecha_precio, errors='coerce')

                    # Remove row with the same date if it exists
                    st.session_state.df_precios_combustible = df_precios_temp[
                        df_precios_temp['Fecha_dt'] != fecha_precio_dt
                    ].drop(columns=['Fecha_dt'])

                # Add the new/updated price row
                st.session_state.df_precios_combustible = pd.concat([st.session_state.df_precios_combustible, new_precio_processed], ignore_index=True)

                save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE) # Save to DB
                st.success("Precio del combustible registrado/actualizado.")
                st.experimental_rerun() # Rerun to update editor view


    st.subheader("Precios del Combustible Existente")
    df_precios_editable = st.session_state.df_precios_combustible.copy()
    if 'Fecha' in df_precios_editable.columns:
         df_precios_editable['Fecha'] = pd.to_datetime(df_precios_editable['Fecha'], errors='coerce') # Convert for data_editor
    df_precios_edited = st.data_editor(
        df_precios_editable,
        key="data_editor_precios",
        num_rows="dynamic", # Allow adding/deleting rows
        column_config={
            "Fecha": st.column_config.DateColumn("Fecha", required=True),
            "Precio_Litro": st.column_config.NumberColumn("Precio por Litro", min_value=0.01, format="%.2f", required=True),
        }
    )
    # Compare edited with current session state dataframe for changes
    if not df_precios_edited.equals(st.session_state.df_precios_combustible):
         st.session_state.df_precios_combustible = df_precios_edited.copy() # Update session state

         if st.button("Guardar Cambios en Precios de Combustible", key="save_precios_button"):
              # Optional: Validar fechas únicas si cada fecha debe tener un solo precio
              df_to_save = st.session_state.df_precios_combustible.dropna(subset=['Fecha', 'Precio_Litro'], how='all').copy()
              df_to_save = df_to_save[df_to_save['Fecha'].notna()] # Ensure valid dates

              if df_to_save['Fecha'].duplicated().any(): # Check duplicates based on the date column
                   st.error("Error: Hay fechas duplicadas en los precios de combustible. Por favor, corrija los duplicados antes de guardar.")
              elif df_to_save['Fecha'].isnull().any(): # Check after filtering
                   st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
              elif df_to_save['Precio_Litro'].isnull().any(): # Data editor required=True should handle, but check
                    st.error("Error: El campo 'Precio por Litro' no puede estar vacío.")
              else:
                  save_table(df_to_save, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE) # Save cleaned/validated DF
                  st.success("Cambios en precios de combustible guardados.")
                  st.experimental_rerun()
         else:
             st.info("Hay cambios sin guardar en precios de combustible.")


    st.subheader("Reporte por Rango de Fechas")
    col1, col2 = st.columns(2)

    # Recolectar todas las fechas relevantes en una lista para min/max para inputs de fecha
    all_valid_dates_list = []

    # List of dataframes and their date column names to check
    df_date_cols_pairs_for_minmax = [
        (st.session_state.df_consumo, DATE_COLUMNS[TABLE_CONSUMO]),
        (st.session_state.df_costos_salarial, DATE_COLUMNS[TABLE_COSTOS_SALARIAL]),
        (st.session_state.df_gastos_fijos, DATE_COLUMNS[TABLE_GASTOS_FIJOS]),
        (st.session_state.df_gastos_mantenimiento, DATE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]),
        (st.session_state.df_precios_combustible, DATE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])
    ]

    for df, date_col in df_date_cols_pairs_for_minmax:
        if not df.empty and date_col in df.columns:
            # Convert to datetime, drop NaT, convert to Python date objects
            valid_dates_series = pd.to_datetime(df[date_col], errors='coerce').dropna().dt.date
            if not valid_dates_series.empty:
                 all_valid_dates_list.extend(valid_dates_series.tolist())


    if all_valid_dates_list: # Check if the list is not empty
        min_app_date = min(all_valid_dates_list) # Use Python's min() function on the list of dates
        max_app_date = max(all_valid_dates_list) # Use Python's max() function on the list of dates
        # Suggest a default range ending recently, relative to max date
        today = datetime.date.today()
        default_end = min(today, max_app_date)
        default_start = default_end - pd.Timedelta(days=30) # Default to 30 days prior

        # Ensure defaults are within bounds
        default_start = max(default_start, min_app_date)
        default_end = max(default_end, default_start) # Ensure end is not before start

    else:
        # Fallback if no data dates exist
        today = datetime.date.today()
        min_app_date = today - pd.Timedelta(days=365 * 5) # Wide potential range
        max_app_date = today
        default_start = today - pd.Timedelta(days=30)
        default_end = today

    min_date_input_display = min_app_date
    max_date_input_display = max_app_date

    with col1:
        fecha_inicio = st.date_input("Fecha de Inicio del Reporte", default_start, min_value=min_date_input_display, max_value=max_date_input_display, key="reporte_fecha_inicio")
    with col2:
        fecha_fin = st.date_input("Fecha de Fin del Reporte", default_end, min_value=min_date_input_display, max_value=max_date_input_display, key="reporte_fecha_fin")


    if st.button("Generar Reporte", key="generate_reporte_button"):
        if fecha_inicio > fecha_fin:
            st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
            return

        # Define date range as pandas Timestamps for robust filtering
        start_ts = pd.Timestamp(fecha_inicio)
        end_ts = pd.Timestamp(fecha_fin)

        # Filter dataframes ensuring date columns are datetime64[ns]
        df_consumo_filtered = st.session_state.df_consumo.copy()
        if DATE_COLUMNS[TABLE_CONSUMO] in df_consumo_filtered.columns:
             df_consumo_filtered['Fecha'] = pd.to_datetime(df_consumo_filtered['Fecha'], errors='coerce')
             df_consumo_filtered = df_consumo_filtered[(df_consumo_filtered['Fecha'] >= start_ts) & (df_consumo_filtered['Fecha'] <= end_ts)].copy()
        else:
             df_consumo_filtered = pd.DataFrame(columns=st.session_state.df_consumo.columns) # Empty if no date column

        df_precios_filtered = st.session_state.df_precios_combustible.copy()
        if DATE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in df_precios_filtered.columns:
            df_precios_filtered['Fecha'] = pd.to_datetime(df_precios_filtered['Fecha'], errors='coerce')
            df_precios_filtered = df_precios_filtered[(df_precios_filtered['Fecha'] >= start_ts) & (df_precios_filtered['Fecha'] <= end_ts)].copy()
        else:
            df_precios_filtered = pd.DataFrame(columns=st.session_state.df_precios_combustible.columns)


        df_salarial_filtered = st.session_state.df_costos_salarial.copy()
        if DATE_COLUMNS[TABLE_COSTOS_SALARIAL] in df_salarial_filtered.columns:
             df_salarial_filtered['Fecha'] = pd.to_datetime(df_salarial_filtered['Fecha'], errors='coerce')
             df_salarial_filtered = df_salarial_filtered[(df_salarial_filtered['Fecha'] >= start_ts) & (df_salarial_filtered['Fecha'] <= end_ts)].copy()
        else:
             df_salarial_filtered = pd.DataFrame(columns=st.session_state.df_costos_salarial.columns)

        df_fijos_filtered = st.session_state.df_gastos_fijos.copy()
        if DATE_COLUMNS[TABLE_GASTOS_FIJOS] in df_fijos_filtered.columns:
             df_fijos_filtered['Fecha'] = pd.to_datetime(df_fijos_filtered['Fecha'], errors='coerce')
             df_fijos_filtered = df_fijos_filtered[(df_fijos_filtered['Fecha'] >= start_ts) & (df_fijos_filtered['Fecha'] <= end_ts)].copy()
        else:
             df_fijos_filtered = pd.DataFrame(columns=st.session_state.df_gastos_fijos.columns)


        df_mantenimiento_filtered = st.session_state.df_gastos_mantenimiento.copy()
        if DATE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO] in df_mantenimiento_filtered.columns:
             df_mantenimiento_filtered['Fecha'] = pd.to_datetime(df_mantenimiento_filtered['Fecha'], errors='coerce')
             df_mantenimiento_filtered = df_mantenimiento_filtered[(df_mantenimiento_filtered['Fecha'] >= start_ts) & (df_mantenimiento_filtered['Fecha'] <= end_ts)].copy()
        else:
             df_mantenimiento_filtered = pd.DataFrame(columns=st.session_state.df_gastos_mantenimiento.columns)



        if df_consumo_filtered.empty:
            st.info("No hay datos de consumo en el rango de fechas seleccionado.")
            reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Patente', 'ID_Flota', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible']) # Empty DF with all expected columns for later merge

        else:
             # Calcular métricas por equipo y fecha en el periodo
             df_consumo_filtered['Consumo_L_H'] = df_consumo_filtered.apply(
                 lambda row: 0 if pd.isna(row['Horas_Trabajadas']) or row['Horas_Trabajadas'] == 0 else (row['Consumo_Litros'] if pd.notna(row['Consumo_Litros']) else 0) / row['Horas_Trabajadas'], axis=1
             )
             df_consumo_filtered['Consumo_L_KM'] = df_consumo_filtered.apply(
                  lambda row: 0 if pd.isna(row['Kilometros_Recorridos']) or row['Kilometros_Recorridos'] == 0 else (row['Consumo_Litros'] if pd.notna(row['Consumo_Litros']) else 0) / row['Kilometros_Recorridos'], axis=1
             )

             # Unir con precios de combustible (usando el precio más reciente antes o en la fecha de consumo)
             # Ensure dates are sorted datetime for merge_asof
             consumo_for_merge = df_consumo_filtered.sort_values('Fecha')
             precios_for_merge = df_precios_filtered.sort_values('Fecha')

             # merge_asof requires 'Fecha' to be datetime64[ns]
             # Ensure unique dates in prices for merge_asof when not merging by 'by'
             # If merging by Interno (which price data doesn't support currently per schema):
             # consumo_for_merge = consumo_for_merge.sort_values(['Interno', 'Fecha']) # Also sort consumo
             # precios_for_merge_unique = precios_for_merge.dropna(subset=['Interno', 'Fecha', 'Precio_Litro']).drop_duplicates(subset=['Interno', 'Fecha']).sort_values(['Interno', 'Fecha'])
             # consumo_p1_merged = pd.merge_asof(consumo_for_merge, precios_for_merge_unique, on='Fecha', by='Interno', direction='backward')

             # Assuming price is not per equipment, only merge on date
             precios_for_merge_unique = precios_for_merge[['Fecha', 'Precio_Litro']].dropna(subset=['Fecha', 'Precio_Litro']).drop_duplicates(subset=['Fecha']).sort_values('Fecha')
             consumo_merged = pd.merge_asof(consumo_for_merge, precios_for_merge_unique, on='Fecha', direction='backward')

             # Calcular costo del combustible (handle potential missing Precio_Litro from merge)
             reporte_consumo_detail = consumo_merged.copy() # Rename for clarity
             reporte_consumo_detail['Costo_Combustible'] = reporte_consumo_detail['Consumo_Litros'].fillna(0) * reporte_consumo_detail['Precio_Litro'].fillna(0) # If no price, cost is 0


             # Resumen de Consumo y Costo Combustible por Equipo en el período
             reporte_resumen_consumo = reporte_consumo_detail.groupby('Interno').agg(
                 Total_Consumo_Litros=('Consumo_Litros', 'sum'),
                 Total_Horas=('Horas_Trabajadas', 'sum'),
                 Total_Kilometros=('Kilometros_Recorridos', 'sum'),
                 Costo_Total_Combustible=('Costo_Combustible', 'sum')
             ).reset_index()

             # Recalcular L/H y L/KM promedio *después* de sumar (avoid NaN/inf with zero checks)
             reporte_resumen_consumo['Avg_Consumo_L_H'] = reporte_resumen_consumo.apply(
                  lambda row: 0 if pd.isna(row['Total_Horas']) or row['Total_Horas'] == 0 else (row['Total_Consumo_Litros'] if pd.notna(row['Total_Consumo_Litros']) else 0) / row['Total_Horas'], axis=1
             )
             reporte_resumen_consumo['Avg_Consumo_L_KM'] = reporte_resumen_consumo.apply(
                  lambda row: 0 if pd.isna(row['Total_Kilometros']) or row['Total_Kilometros'] == 0 else (row['Total_Consumo_Litros'] if pd.notna(row['Total_Consumo_Litros']) else 0) / row['Total_Kilometros'], axis=1
             )

             # Unir con información de equipos (Patente, ID_Flota) y luego con Flotas (Nombre_Flota)
             reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_equipos[['Interno', 'Patente', 'ID_Flota']], on='Interno', how='left')
             reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_flotas[['ID_Flota', 'Nombre_Flota']], on='ID_Flota', how='left')
             reporte_resumen_consumo['Nombre_Flota'] = reporte_resumen_consumo['Nombre_Flota'].fillna('Sin Flota') # Fill missing fleet names


             st.subheader(f"Reporte Consumo y Costo Combustible ({fecha_inicio} a {fecha_fin})")
             st.dataframe(reporte_resumen_consumo[[
                 'Interno', 'Patente', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros',
                 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'
             ]].round(2))


        # --- Sumar otros costos (Salarial, Fijos, Mantenimiento) en el periodo ---
        # Agrupar por Interno, handling empty dataframes gracefully
        salarial_agg = salarial_filtered.groupby('Interno')['Monto_Salarial'].sum().reset_index(name='Total_Salarial') if not salarial_filtered.empty and 'Monto_Salarial' in salarial_filtered.columns else pd.DataFrame(columns=['Interno', 'Total_Salarial'])
        fijos_agg = fijos_filtered.groupby('Interno')['Monto_Gasto_Fijo'].sum().reset_index(name='Total_Gastos_Fijos') if not fijos_filtered.empty and 'Monto_Gasto_Fijo' in fijos_filtered.columns else pd.DataFrame(columns=['Interno', 'Total_Gastos_Fijos'])
        mantenimiento_agg = mantenimiento_filtered.groupby('Interno')['Monto_Mantenimiento'].sum().reset_index(name='Total_Gastos_Mantenimiento') if not mantenimiento_filtered.empty and 'Monto_Mantenimiento' in mantenimiento_filtered.columns else pd.DataFrame(columns=['Interno', 'Total_Gastos_Mantenimiento'])


        # Unir todos los costos
        # Empezar con la lista única de equipos que tuvieron ALGÚN costo o consumo en el periodo
        all_internos_series_list = []
        if not df_consumo_filtered.empty and 'Interno' in df_consumo_filtered.columns: all_internos_series_list.append(df_consumo_filtered['Interno'])
        if not salarial_filtered.empty and 'Interno' in salarial_filtered.columns: all_internos_series_list.append(salarial_filtered['Interno'])
        if not fijos_filtered.empty and 'Interno' in fijos_filtered.columns: all_internos_series_list.append(fijos_filtered['Interno'])
        if not mantenimiento_filtered.empty and 'Interno' in mantenimiento_filtered.columns: all_internos_series_list.append(mantenimiento_filtered['Interno'])


        if not all_internos_series_list:
             st.info("No hay datos de costos (Combustible, Salarial, Fijos, Mantenimiento) en el rango de fechas seleccionado para ningún equipo.")
        else:
             all_internos_in_period = pd.concat(all_internos_series_list).dropna().unique()
             df_all_internos = pd.DataFrame(all_internos_in_period, columns=['Interno'])

             # Merge with cost summaries
             # Start with df_all_internos and left merge to keep all 'Interno' from the period,fillna(0)
             reporte_costo_total = df_all_internos.merge(reporte_resumen_consumo[['Interno', 'Costo_Total_Combustible']], on='Interno', how='left').fillna(0)
             reporte_costo_total = reporte_costo_total.merge(salarial_agg, on='Interno', how='left').fillna(0)
             reporte_costo_total = reporte_costo_total.merge(fijos_agg, on='Interno', how='left').fillna(0)
             reporte_costo_total = reporte_costo_total.merge(mantenimiento_agg, on='Interno', how='left').fillna(0)

             # Añadir Patente y Flota (Left merge from equipo master list)
             reporte_costo_total = reporte_costo_total.merge(st.session_state.df_equipos[['Interno', 'Patente', 'ID_Flota']], on='Interno', how='left')
             reporte_costo_total = reporte_costo_total.merge(st.session_state.df_flotas[['ID_Flota', 'Nombre_Flota']], on='ID_Flota', how='left')
             reporte_costo_total['Nombre_Flota'] = reporte_costo_total['Nombre_Flota'].fillna('Sin Flota')
             # Fillna for Patente might be needed if equipos are not linked to master list correctly
             reporte_costo_total['Patente'] = reporte_costo_total['Patente'].fillna('Sin Patente')


             reporte_costo_total['Costo_Total_Equipo'] = reporte_costo_total['Costo_Total_Combustible'] + reporte_costo_total['Total_Salarial'] + reporte_costo_total['Total_Gastos_Fijos'] + reporte_costo_total['Total_Gastos_Mantenimiento']


             st.subheader(f"Reporte Costo Total por Equipo ({fecha_inicio} a {fecha_fin})")
             st.dataframe(reporte_costo_total[[
                 'Interno', 'Patente', 'Nombre_Flota', 'Costo_Total_Combustible', 'Total_Salarial',
                 'Total_Gastos_Fijos', 'Total_Gastos_Mantenimiento', 'Costo_Total_Equipo'
             ]].round(2))


def page_variacion_costos_flota():
    st.title("Variación de Costos de Flota (Gráfico de Cascada)")
    st.write("Compara los costos totales de la flota entre dos períodos para visualizar la variación.")

    st.subheader("Seleccione Períodos a Comparar")
    col1, col2, col3, col4 = st.columns(4)
    # Set default dates based on available data, similar to reportes_mina
    all_valid_dates_list = []

    df_date_cols_pairs_for_minmax = [
        (st.session_state.df_consumo, DATE_COLUMNS[TABLE_CONSUMO]),
        (st.session_state.df_costos_salarial, DATE_COLUMNS[TABLE_COSTOS_SALARIAL]),
        (st.session_state.df_gastos_fijos, DATE_COLUMNS[TABLE_GASTOS_FIJOS]),
        (st.session_state.df_gastos_mantenimiento, DATE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]),
        (st.session_state.df_precios_combustible, DATE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])
    ]

    for df, date_col in df_date_cols_pairs_for_minmax:
        if not df.empty and date_col in df.columns:
            valid_dates_series = pd.to_datetime(df[date_col], errors='coerce').dropna().dt.date
            if not valid_dates_series.empty:
                 all_valid_dates_list.extend(valid_dates_series.tolist())


    if all_valid_dates_list:
        min_app_date = min(all_valid_dates_list)
        max_app_date = max(all_valid_dates_list)
        # Suggest recent two months
        today = datetime.date.today()
        default_end_p2 = min(today, max_app_date)
        default_start_p2 = default_end_p2 - pd.Timedelta(days=30)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = default_end_p1 - pd.Timedelta(days=30)

        # Ensure defaults are within bounds
        default_start_p1 = max(default_start_p1, min_app_date)
        default_end_p1 = max(default_end_p1, min_app_date)
        default_start_p2 = max(default_start_p2, min_app_date)
        default_end_p2 = max(default_end_p2, min_app_date)

         # Ensure period end is not before period start, within the calculated defaults
        default_end_p1 = max(default_end_p1, default_start_p1)
        default_end_p2 = max(default_end_p2, default_start_p2)


    else:
        today = datetime.date.today()
        min_app_date = today - pd.Timedelta(days=365 * 5) # Wide potential range
        max_app_date = today
        default_start_p1 = today - pd.Timedelta(days=60)
        default_end_p1 = today - pd.Timedelta(days=31)
        default_start_p2 = today - pd.Timedelta(days=30)
        default_end_p2 = today


    min_date_input_display = min_app_date
    max_date_input_display = max_app_date


    with col1:
        fecha_inicio_p1 = st.date_input("Inicio Período 1", default_start_p1, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_inicio_p1")
    with col2:
        fecha_fin_p1 = st.date_input("Fin Período 1", default_end_p1, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_fin_p1")
    with col3:
        fecha_inicio_p2 = st.date_input("Inicio Período 2", default_start_p2, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_inicio_p2")
    with col4:
        fecha_fin_p2 = st.date_input("Fin Período 2", default_end_p2, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_fin_p2")


    if st.button("Generar Gráfico de Cascada", key="generate_waterfall_button"):
        if fecha_inicio_p1 > fecha_fin_p1:
             st.error("Las fechas del Período 1 no son válidas.")
             return
        if fecha_inicio_p2 > fecha_fin_p2:
             st.error("Las fechas del Período 2 no son válidas.")
             return
        # Warning for overlapping periods, but allow it
        if not (fecha_fin_p1 < fecha_inicio_p2 or fecha_fin_p2 < fecha_inicio_p1):
             st.warning("Advertencia: Los períodos seleccionados se solapan. Esto puede afectar la interpretación de la variación.")


        # --- Calcular Costos por Período y Categoría ---
        # Helper function to aggregate costs for a given date range
        def aggregate_costs_for_period(df_original, date_col_name, start_date, end_date, cost_col_name=None):
            if df_original.empty or date_col_name not in df_original.columns:
                 return 0 if cost_col_name else pd.DataFrame() # Return 0 for total cost, empty DF otherwise

            df_temp = df_original.copy()
            # Convert date column to datetime64[ns] for filtering, handling errors
            df_temp[date_col_name] = pd.to_datetime(df_temp[date_col_name], errors='coerce')
            # Remove rows with invalid/missing dates *before* filtering the range
            df_temp = df_temp.dropna(subset=[date_col_name])

            # Filter by date range (using pandas Timestamps for robust comparison)
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
            df_filtered = df_temp[(df_temp[date_col_name] >= start_ts) & (df_temp[date_col_name] <= end_ts)].copy() # Use .copy() to avoid SettingWithCopyWarning


            if cost_col_name and cost_col_name in df_filtered.columns:
                # Ensure the cost column is numeric before summing
                 df_filtered[cost_col_name] = pd.to_numeric(df_filtered[cost_col_name], errors='coerce').fillna(0)
                 return df_filtered[cost_col_name].sum()
            elif cost_col_name is None: # Return filtered DF
                 return df_filtered
            else: # Cost column requested but not found or no data
                 return 0


        # Costs for Period 1
        consumo_p1_filtered = aggregate_costs_for_period(st.session_state.df_consumo, DATE_COLUMNS[TABLE_CONSUMO], fecha_inicio_p1, fecha_fin_p1, cost_col_name=None) # Return filtered DF
        precios_p1_filtered = aggregate_costs_for_period(st.session_state.df_precios_combustible, DATE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], fecha_inicio_p1, fecha_fin_p1, cost_col_name=None) # Return filtered DF
        costo_salarial_p1 = aggregate_costs_for_period(st.session_state.df_costos_salarial, DATE_COLUMNS[TABLE_COSTOS_SALARIAL], fecha_inicio_p1, fecha_fin_p1, cost_col_name='Monto_Salarial')
        costo_fijos_p1 = aggregate_costs_for_period(st.session_state.df_gastos_fijos, DATE_COLUMNS[TABLE_GASTOS_FIJOS], fecha_inicio_p1, fecha_fin_p1, cost_col_name='Monto_Gasto_Fijo')
        costo_mantenimiento_p1 = aggregate_costs_for_period(st.session_state.df_gastos_mantenimiento, DATE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], fecha_inicio_p1, fecha_fin_p1, cost_col_name='Monto_Mantenimiento')

        # Calculate fuel cost for Period 1
        costo_combustible_p1 = 0
        if not consumo_p1_filtered.empty and not precios_p1_filtered.empty:
             consumo_p1_sorted = consumo_p1_filtered.sort_values('Fecha')
             precios_p1_sorted = precios_p1_filtered.sort_values('Fecha')

             # Assuming prices are NOT per equipment for merge_asof by date
             # Ensure unique dates in prices for merge_asof on='Fecha'
             precios_p1_sorted_unique = precios_p1_sorted[['Fecha', 'Precio_Litro']].dropna(subset=['Fecha', 'Precio_Litro']).drop_duplicates(subset=['Fecha']).sort_values('Fecha')

             if not consumo_p1_sorted.empty and not precios_p1_sorted_unique.empty:
                 # merge_asof needs Fecha to be datetime64[ns] (which it is after aggregate_costs_for_period)
                 consumo_merged = pd.merge_asof(consumo_p1_sorted, precios_p1_sorted_unique, on='Fecha', direction='backward')
                 # Calculate cost, handling missing price from merge_asof if no price existed before or on the date
                 costo_combustible_p1 = (consumo_merged['Consumo_Litros'].fillna(0) * consumo_merged['Precio_Litro'].fillna(0)).sum()


        total_costo_p1 = costo_combustible_p1 + costo_salarial_p1 + costo_fijos_p1 + costo_mantenimiento_p1


        # Costs for Period 2 (similar logic)
        consumo_p2_filtered = aggregate_costs_for_period(st.session_state.df_consumo, DATE_COLUMNS[TABLE_CONSUMO], fecha_inicio_p2, fecha_fin_p2, cost_col_name=None)
        precios_p2_filtered = aggregate_costs_for_period(st.session_state.df_precios_combustible, DATE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], fecha_inicio_p2, fecha_fin_p2, cost_col_name=None)
        costo_salarial_p2 = aggregate_costs_for_period(st.session_state.df_costos_salarial, DATE_COLUMNS[TABLE_COSTOS_SALARIAL], fecha_inicio_p2, fecha_fin_p2, cost_col_name='Monto_Salarial')
        costo_fijos_p2 = aggregate_costs_for_period(st.session_state.df_gastos_fijos, DATE_COLUMNS[TABLE_GASTOS_FIJOS], fecha_inicio_p2, fecha_fin_p2, cost_col_name='Monto_Gasto_Fijo')
        costo_mantenimiento_p2 = aggregate_costs_for_period(st.session_state.df_gastos_mantenimiento, DATE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], fecha_inicio_p2, fecha_fin_p2, cost_col_name='Monto_Mantenimiento')


        costo_combustible_p2 = 0
        if not consumo_p2_filtered.empty and not precios_p2_filtered.empty:
             consumo_p2_sorted = consumo_p2_filtered.sort_values('Fecha')
             precios_p2_sorted = precios_p2_filtered.sort_values('Fecha')
             precios_p2_sorted_unique = precios_p2_sorted[['Fecha', 'Precio_Litro']].dropna(subset=['Fecha', 'Precio_Litro']).drop_duplicates(subset=['Fecha']).sort_values('Fecha')

             if not consumo_p2_sorted.empty and not precios_p2_sorted_unique.empty:
                  consumo_merged = pd.merge_asof(consumo_p2_sorted, precios_p2_sorted_unique, on='Fecha', direction='backward')
                  costo_combustible_p2 = (consumo_merged['Consumo_Litros'].fillna(0) * consumo_merged['Precio_Litro'].fillna(0)).sum()


        total_costo_p2 = costo_combustible_p2 + costo_salarial_p2 + costo_fijos_p2 + costo_mantenimiento_p2

        # --- Preparar Datos para el Gráfico de Cascada ---
        labels = [
            f'Total Costo<br>P1<br>({fecha_inicio_p1} a {fecha_fin_p1})'
        ]
        measures = ['absolute']
        values = [total_costo_p1]
        texts = [f"${total_costo_p1:,.2f}"] # Formato de texto

        # Variaciones por categoría
        variacion_combustible = costo_combustible_p2 - costo_combustible_p1
        variacion_salarial = costo_salarial_p2 - costo_salarial_p1
        variacion_fijos = costo_fijos_p2 - costo_fijos_p1
        variacion_mantenimiento = costo_mantenimiento_p2 - costo_mantenimiento_p1
        variacion_total = total_costo_p2 - total_costo_p1 # Should equal the sum of variations

        # Collect variations to display, only add if significant non-zero change
        variation_data = []
        if abs(variacion_combustible) > 0.01: variation_data.append({'label': 'Var. Combustible', 'value': variacion_combustible})
        if abs(variacion_salarial) > 0.01: variation_data.append({'label': 'Var. Salarial', 'value': variacion_salarial})
        if abs(variacion_fijos) > 0.01: variation_data.append({'label': 'Var. Fijos', 'value': variacion_fijos})
        if abs(variacion_mantenimiento) > 0.01: variation_data.append({'label': 'Var. Mantenimiento', 'value': variacion_mantenimiento})

        # Sort variations (e.g., largest positive first, then negative)
        variation_data.sort(key=lambda x: x['value'], reverse=True) # Sort by value descending

        # Add sorted variations to plot data
        for item in variation_data:
            labels.append(item['label'])
            measures.append('relative')
            values.append(item['value'])
            texts.append(f"${item['value']:,.2f}")


        # Añadir total Periodo 2
        labels.append(f'Total Costo<br>P2<br>({fecha_inicio_p2} a {fecha_fin_p2})')
        measures.append('total')
        values.append(total_costo_p2)
        texts.append(f"${total_costo_p2:,.2f}")

        # Check if there is actual change data to plot a waterfall (more than just the two absolute points)
        if len(labels) <= 2 and abs(total_costo_p1 - total_costo_p2) < 0.01:
             st.info("No hay datos de costos o la variación entre los períodos es insignificante (<$0.01) para mostrar el gráfico de cascada.")
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
                 increasing = {"marker":{"color":"#3D9970"}}, # Green
                 decreasing = {"marker":{"color":"#FF4136"}}, # Red
                 totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}} # Blue

             ))

             fig.update_layout(
                 title = f'Variación de Costos de Flota: {fecha_inicio_p1} a {fecha_fin_p1} vs {fecha_inicio_p2} a {fecha_fin_p2}',
                 showlegend = False,
                 yaxis_title="Monto ($)",
                 margin=dict(l=20, r=20, t=100, b=20), # Adjust margins for long title
                 height=600 # Increase height for better visibility
             )

             st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detalle de Costos por Período")
        col_p_1, col_p_2 = st.columns(2)
        with col_p_1:
            st.write(f"**Periodo 1: {fecha_inicio_p1} a {fecha_fin_p1}**")
            st.write(f"- Combustible: ${costo_combustible_p1:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p1:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p1:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p1:,.2f}")
            st.write(f"**Total Periodo 1: ${total_costo_p1:,.2f}**")
        with col_p_2:
            st.write(f"**Periodo 2: {fecha_inicio_p2} a {fecha_fin_p2}**")
            st.write(f"- Combustible: ${costo_combustible_p2:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p2:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p2:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p2:,.2f}")
            st.write(f"**Total Periodo 2: ${total_costo_p2:,.2f}**")

        # Display detailed variations only if there was significant total variation
        if abs(total_costo_p1 - total_costo_p2) >= 0.01:
            st.subheader("Variaciones por Categoría")
            st.write(f"- Combustible: ${variacion_combustible:,.2f}")
            st.write(f"- Salarial: ${variacion_salarial:,.2f}")
            st.write(f"- Fijos: ${variacion_fijos:,.2f}")
            st.write(f"- Mantenimiento: ${variacion_mantenimiento:,.2f}")
            st.write(f"**Variación Total: ${variacion_total:,.2f}**")


def page_gestion_obras():
    st.title("Gestión de Obras")
    st.write("Aquí puedes crear y gestionar proyectos de obra, así como su presupuesto de materiales.")

    st.subheader("Crear Nueva Obra")
    with st.form("form_add_obra", clear_on_submit=True):
        nombre_obra = st.text_input("Nombre de la Obra").strip()
        responsable = st.text_input("Responsable de Seguimiento").strip()
        submitted = st.form_submit_button("Crear Obra")
        if submitted:
            if not nombre_obra or not responsable:
                st.warning("Por favor, complete el Nombre de la Obra y el Responsable.")
            else:
                # Generar ID único simple (usando timestamp + counter)
                current_ids = set(st.session_state.df_proyectos['ID_Obra'].astype(str)) # Get existing IDs as strings
                base_id = f"OBRA_{int(time.time() * 1000)}"
                id_obra = base_id
                counter = 0
                while id_obra in current_ids:
                    counter += 1
                    id_obra = f"{base_id}_{counter}"

                new_obra_data = {'ID_Obra': id_obra, 'Nombre_Obra': nombre_obra, 'Responsable': responsable}
                new_obra = pd.DataFrame([new_obra_data])
                # Ensure all expected columns exist
                new_obra_processed = get_initialized_dataframe(TABLE_PROYECTOS).append(new_obra, ignore_index=True).tail(1)
                new_obra_processed.update(new_obra_data) # Restore values

                st.session_state.df_proyectos = pd.concat([st.session_state.df_proyectos, new_obra_processed], ignore_index=True)
                save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS) # Save to DB
                st.success(f"Obra '{nombre_obra}' creada con ID: {id_obra}")
                st.experimental_rerun() # Rerun to update the list and selectbox


    st.subheader("Lista de Obras")
    if st.session_state.df_proyectos.empty:
        st.info("No hay obras creadas aún.")
        obras_disponibles = [] # Ensure list is empty if no projects
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar obras. Use el ícono de papelera para eliminar filas.")
        df_proyectos_editable = st.session_state.df_proyectos.copy()
        df_proyectos_edited = st.data_editor(
             df_proyectos_editable,
             key="data_editor_proyectos",
             num_rows="dynamic", # Allow adding/deleting rows
             column_config={
                  "ID_Obra": st.column_config.TextColumn("ID Obra", disabled=True), # No permitir editar ID
                  "Nombre_Obra": st.column_config.TextColumn("Nombre Obra", required=True),
                  "Responsable": st.column_config.TextColumn("Responsable", required=True)
             }
        )
        # Check if any row was deleted or added, or if content changed
        if not df_proyectos_edited.equals(st.session_state.df_proyectos):
             st.session_state.df_proyectos = df_proyectos_edited.copy() # Update session state

             if st.button("Guardar Cambios en Lista de Obras", key="save_proyectos_button"):
                  # Simple validation (non-empty, ensure ID for new rows from editor)
                  df_to_save = st.session_state.df_proyectos.dropna(subset=['Nombre_Obra', 'Responsable'], how='all').copy()
                  df_to_save = df_to_save[(df_to_save['Nombre_Obra'].astype(str).str.strip() != '') & (df_to_save['Responsable'].astype(str).str.strip() != '')] # Remove empty rows based on core fields

                  if df_to_save['Nombre_Obra'].isnull().any() or (df_to_save['Nombre_Obra'].astype(str).str.strip() == '').any():
                      st.error("Error: Los campos 'Nombre Obra' no pueden estar vacíos.")
                  elif df_to_save['Responsable'].isnull().any() or (df_to_save['Responsable'].astype(str).str.strip() == '').any():
                       st.error("Error: Los campos 'Responsable' no pueden estar vacíos.")
                  else:
                       # Add ID for any potentially new rows added directly in the editor without an ID
                       new_row_mask = df_to_save['ID_Obra'].isnull() | (df_to_save['ID_Obra'].astype(str).str.strip() == '')
                       if new_row_mask.any():
                            existing_ids = set(df_to_save['ID_Obra'].dropna().astype(str))
                            new_ids = []
                            for i in range(new_row_mask.sum()):
                                base_id = f"OBRA_EDIT_{int(time.time() * 1000)}_{i}"
                                unique_id = base_id
                                counter = 0
                                while unique_id in existing_ids:
                                    counter += 1
                                    unique_id = f"{base_id}_{counter}"
                                new_ids.append(unique_id)
                                existing_ids.add(unique_id) # Add to set for subsequent checks
                            df_to_save.loc[new_row_mask, 'ID_Obra'] = new_ids
                            st.session_state.df_proyectos = df_to_save # Update session state


                       save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS) # Save the validated DF
                       st.success("Cambios en la lista de obras guardados.")
                       st.experimental_rerun() # Recargar para actualizar selectbox y otros elementos dependientes
             else:
                 st.info("Hay cambios sin guardar en la lista de obras.")

        # Update obras_disponibles list after potential edits/deletions
        obras_disponibles = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
        # Filter out any None/NaN/empty string obra IDs
        obras_disponibles = [id for id in obras_disponibles if pd.notna(id) and str(id).strip() != '']


    st.markdown("---")
    st.subheader("Gestionar Presupuesto por Obra")

    if not obras_disponibles:
         st.info("No hay obras disponibles para gestionar presupuesto. Por favor, cree una obra primero.")
         # Ensure session state variable for selectbox doesn't hold an invalid value
         if "select_obra_gestion" in st.session_state:
              del st.session_state["select_obra_gestion"] # Reset the selectbox
         return # Exit function if no works exist

    # Build options for the selectbox (ensure we use the *current* list of valid IDs)
    obra_options_gestion = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].isin(obras_disponibles)][['ID_Obra', 'Nombre_Obra']].to_dict('records') # Use filtered list
    obra_gestion_labels = [f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})" for o in obra_options_gestion]

    # Add placeholder option if obra_gestion_labels becomes empty unexpectedly (e.g., after deleting the last work)
    if not obra_gestion_labels:
         st.info("No hay obras disponibles para gestionar presupuesto. Por favor, cree una obra primero.")
         if "select_obra_gestion" in st.session_state: del st.session_state["select_obra_gestion"]
         st.experimental_rerun() # Rerun to handle empty state gracefully
         return

    # Get default index safely
    default_obra_index = 0
    # Check if the previously selected key (if any) still exists in the new labels list
    if "select_obra_gestion" in st.session_state and st.session_state.select_obra_gestion in obra_gestion_labels:
         default_obra_index = obra_gestion_labels.index(st.session_state.select_obra_gestion)
    elif "select_obra_gestion" in st.session_state:
         del st.session_state["select_obra_gestion"] # Clear invalid selected key

    selected_obra_label_gestion = st.selectbox(
        "Seleccione una Obra:",
        obra_gestion_labels,
        index=default_obra_index,
        key="select_obra_gestion" # Use stable key
    )

    # Find the corresponding ID_Obra from the selected label
    obra_seleccionada_id = None
    for o in obra_options_gestion:
        if selected_obra_label_gestion == f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})":
             obra_seleccionada_id = o['ID_Obra']
             break # Found the ID

    # Safety check: if obra_seleccionada_id was None (shouldn't happen if list wasn't empty) or if the selected ID isn't valid
    if obra_seleccionada_id is None or obra_seleccionada_id not in obras_disponibles:
         st.warning(f"La obra seleccionada (ID: {obra_seleccionada_id}) ya no existe o no es válida. Por favor, seleccione otra.")
         # This might happen if the last work was just deleted and selectbox wasn't reset yet.
         # Re-run should fix it.
         st.experimental_rerun()
         return # Exit to prevent errors trying to filter empty/invalid ID

    obra_nombre = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_seleccionada_id]['Nombre_Obra'].iloc[0]
    st.markdown(f"#### Presupuesto de Materiales para '{obra_nombre}' (ID: {obra_seleccionada_id})")

    # Filtrar presupuesto para la obra seleccionada
    df_presupuesto_obra = st.session_state.df_presupuesto_materiales[
        st.session_state.df_presupuesto_materiales['ID_Obra'] == obra_seleccionada_id
    ].copy() # Work on a copy

    st.info("Edite la tabla siguiente para añadir, modificar o eliminar items del presupuesto de materiales de esta obra. Use el ícono de papelera para eliminar filas.")

    # Mostrar y editar presupuesto existente (data_editor)
    # Select only editable columns for the editor display, keep Costo_Presupuestado read-only
    df_presupuesto_obra_display = df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']].copy()
    # Recalculate cost here so editor shows updated values if underlying quantity/price changed (though data_editor itself might not auto-recalculate without manual handling)
    # Data editor does not run python logic per cell edit. We recalculate on form submission OR Save button click.
    # The 'Costo_Presupuestado' shown in data_editor is just the value currently in the dataframe row being displayed.
    # Recalculating BEFORE displaying it in the editor is good to show the last saved/calculated state.
    df_presupuesto_obra_display = calcular_costo_presupuestado(df_presupuesto_obra_display)


    # data_editor returns a dataframe with the edited/added/deleted rows.
    # It will only contain the columns configured in column_config.
    df_presupuesto_obra_edited = st.data_editor(
        df_presupuesto_obra_display,
        key=f"data_editor_presupuesto_{obra_seleccionada_id}", # Unique key per obra
        num_rows="dynamic", # Allow adding/deleting rows
        column_config={
            "Material": st.column_config.TextColumn("Material", required=True),
            "Cantidad_Presupuestada": st.column_config.NumberColumn("Cantidad Presupuestada", min_value=0.0, format="%.2f", required=True),
            "Precio_Unitario_Presupuestado": st.column_config.NumberColumn("Precio Unitario Presupuestado", min_value=0.0, format="%.2f", required=True),
            "Costo_Presupuestado": st.column_config.NumberColumn("Costo Presupuestado", disabled=True, format="%.2f") # Calculated, not editable in editor view
        }
    )

    # Logic to save changes from the data_editor
    # data_editor only returns the edited/displayed columns. We need to add back the 'ID_Obra'.
    df_presupuesto_obra_edited_with_id = df_presupuesto_obra_edited.copy()
    df_presupuesto_obra_edited_with_id['ID_Obra'] = obra_seleccionada_id # Add the ID_Obra column back

    # Recalculate the Costo_Presupuestado column based on potentially edited Qty/Price *in the edited data*
    df_presupuesto_obra_edited_with_id = calcular_costo_presupuestado(df_presupuesto_obra_edited_with_id)

    # Compare this reconstructed dataframe with the original filtered dataframe for this obra
    # Need to select matching columns for comparison if df_presupuesto_obra has more columns than edited_with_id
    # Assuming df_presupuesto_obra_edited_with_id now has all relevant columns in the correct types after recalculation
    cols_for_comparison = list(df_presupuesto_obra_edited_with_id.columns) # Use columns from the edited df including ID and calculated cost

    # Filter the original session state df for this work and keep only columns in the edited version for comparison
    df_presupuesto_obra_original_filtered = st.session_state.df_presupuesto_materiales[
        st.session_state.df_presupuesto_materiales['ID_Obra'] == obra_seleccionada_id
    ].copy()
    # Ensure the original has calculated cost if needed for comparison
    df_presupuesto_obra_original_filtered = calcular_costo_presupuestado(df_presupuesto_obra_original_filtered)
    # Select the same columns for comparison and ensure compatible order and index if needed
    df_presupuesto_obra_original_filtered = df_presupuesto_obra_original_filtered[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)
    df_presupuesto_obra_edited_with_id_sorted = df_presupuesto_obra_edited_with_id[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)


    if not df_presupuesto_obra_edited_with_id_sorted.equals(df_presupuesto_obra_original_filtered):
         if st.button(f"Guardar Cambios en Presupuesto de '{obra_nombre}'", key=f"save_presupuesto_{obra_seleccionada_id}_button"):
             # Validation before saving
             df_to_save_obra = df_presupuesto_obra_edited_with_id.copy()
             # Remove completely empty rows from the editor output (which data_editor might add with NaNs)
             df_to_save_obra = df_to_save_obra.dropna(subset=['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado'], how='all').copy()
             df_to_save_obra = df_to_save_obra[df_to_save_obra['Material'].astype(str).str.strip() != '']


             if df_to_save_obra['Material'].isnull().any() or (df_to_save_obra['Material'].astype(str).str.strip() == '').any():
                  st.error("Error: El nombre del material no puede estar vacío en el presupuesto.")
             elif df_to_save_obra['Cantidad_Presupuestada'].isnull().any():
                  st.error("Error: El campo 'Cantidad Presupuestada' no puede estar vacío.")
             elif df_to_save_obra['Precio_Unitario_Presupuestado'].isnull().any():
                  st.error("Error: El campo 'Precio Unitario Presupuestado' no puede estar vacío.")
             else: # All validations pass

                 # Remove the old rows for this work from the main DataFrame
                 df_rest_presupuesto = st.session_state.df_presupuesto_materiales[
                     st.session_state.df_presupuesto_materiales['ID_Obra'] != obra_seleccionada_id
                 ].copy() # Use .copy() to avoid issues

                 # Combine the rest of the data with the updated/edited data for this work
                 st.session_state.df_presupuesto_materiales = pd.concat([df_rest_presupuesto, df_to_save_obra], ignore_index=True)

                 save_table(st.session_state.df_presupuesto_materiales, DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES) # Save to DB
                 st.success(f"Presupuesto de la obra '{obra_nombre}' guardado.")
                 st.experimental_rerun() # Recargar para actualizar la vista del editor y el reporte
             else:
                 st.info(f"Hay cambios sin guardar en el presupuesto de '{obra_nombre}'.")

        # Reporte dentro de la misma página de gestión de obra
        st.markdown(f"#### Reporte de Presupuesto para '{obra_nombre}'")
        if df_presupuesto_obra.empty:
            st.info("No hay presupuesto de materiales registrado para esta obra.")
        else:
            st.subheader("Detalle del Presupuesto")
            # Ensure calculated cost column is present and updated before displaying report
            df_presupuesto_obra_with_cost = calcular_costo_presupuestado(df_presupuesto_obra.copy())
            st.dataframe(df_presupuesto_obra_with_cost[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']].round(2))

            total_cantidad_presupuestada = df_presupuesto_obra_with_cost['Cantidad_Presupuestada'].sum()
            total_costo_presupuestado = df_presupuesto_obra_with_cost['Costo_Presupuestado'].sum()

            st.subheader("Resumen del Presupuesto")
            st.write(f"**Cantidad Total Presupuestada:** {total_cantidad_presupuestada:,.2f}")
            st.write(f"**Costo Total Presupuestado:** ${total_costo_presupuestado:,.2f}")


        # Reporte de Variación dentro de la misma página de gestión de obra
        st.markdown(f"#### Variación Materiales para '{obra_nombre}' (Presupuesto vs Asignado)")

        # Ensure cost is calculated/present in allocation data
        df_asignacion_obra = st.session_state.df_asignacion_materiales[
             st.session_state.df_asignacion_materiales['ID_Obra'] == obra_seleccionada_id
        ].copy()
        df_asignacion_obra = calcular_costo_asignado(df_asignacion_obra) # Recalculate/ensure cost


        if df_presupuesto_obra.empty and df_asignacion_obra.empty:
            st.info("No hay presupuesto ni materiales asignados para esta obra.")
        else:
           # Agrupar presupuesto por material
           # Use the dataframe that includes the calculated cost
           presupuesto_agg = calcular_costo_presupuestado(df_presupuesto_obra.copy()).groupby('Material').agg(
               Cantidad_Presupuestada=('Cantidad_Presupuestada', 'sum'),
               Costo_Presupuestado=('Costo_Presupuestado', 'sum')
           ).reset_index()

           # Agrupar asignaciones por material
           # Ensure calculated cost is present in the assignment df
           asignacion_agg = calcular_costo_asignado(df_asignacion_obra.copy()).groupby('Material').agg(
               Cantidad_Asignada=('Cantidad_Asignada', 'sum'),
               Costo_Asignado=('Costo_Asignado', 'sum')
           ).reset_index()

           # Unir presupuesto y asignación
           # Use 'Material' column as string type for merge to avoid type issues
           presupuesto_agg['Material'] = presupuesto_agg['Material'].astype(str)
           asignacion_agg['Material'] = asignacion_agg['Material'].astype(str)

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
               # Only show if there's data to show or variation is significant
               if abs(total_variacion_costo_obra) > 0.01 or total_costo_presupuestado_obra > 0 or total_costo_asignado_obra > 0:
                    st.subheader("Gráfico de Variación de Costo por Obra")
                    # Ensure the chart is meaningful, especially for edge cases
                    if abs(total_costo_presupuestado_obra) < 0.01 and abs(total_costo_asignado_obra) < 0.01:
                         st.info("El presupuesto total y el costo asignado total para esta obra son ambos cero o insignificantes.")
                    else:
                        # Determine waterfall steps
                        labels_obra_cascada = [f'Presupuesto<br>{obra_nombre}']
                        values_obra_cascada = [total_costo_presupuestado_obra]
                        measures_obra_cascada = ['absolute']
                        texts_obra_cascada = [f"${total_costo_presupuestado_obra:,.2f}"]

                        if abs(total_variacion_costo_obra) >= 0.01:
                            labels_obra_cascada.append('Variación Total')
                            values_obra_cascada.append(total_variacion_costo_obra)
                            measures_obra_cascada.append('relative')
                            texts_obra_cascada.append(f"${total_variacion_costo_obra:,.2f}")
                            final_measure = 'total' # Use total if there's variation
                        else:
                             # If variation is zero or negligible, only show start and end points
                             # measures=[absolute, total] implicitly shows the link as the change
                             # Or explicitly add a 0 change relative bar if desired
                             final_measure = 'total'


                        labels_obra_cascada.append(f'Asignado<br>{obra_nombre}')
                        values_obra_cascada.append(total_costo_asignado_obra)
                        measures_obra_cascada.append(final_measure) # The last bar is the final total
                        texts_obra_cascada.append(f"${total_costo_asignado_obra:,.2f}")


                        fig_obra_variacion = go.Figure(go.Waterfall(
                           name = f"Variación Obra: {obra_nombre}",
                           orientation = "v",
                           measure = measures_obra_cascada,
                           x = labels_obra_cascada,
                           textposition = "outside",
                           text = texts_obra_cascada,
                           y = values_obra_cascada,
                           connector = {"line":{"color":"rgb(63, 63, 63)"}},
                           increasing = {"marker":{"color":"#FF4136"}}, # Red for over budget
                           decreasing = {"marker":{"color":"#3D9970"}}, # Green for under budget
                           totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}} # Blue
                        ))

                        fig_obra_variacion.update_layout(
                            title = f'Variación Costo Materiales Obra: {obra_nombre} (Presupuesto vs Asignado)',
                            showlegend = False,
                            yaxis_title="Monto ($)",
                            margin=dict(l=20, r=20, t=60, b=20),
                            height=400
                        )
                        st.plotly_chart(fig_obra_variacion, use_container_width=True)

def page_reporte_presupuesto_total_obras():
    st.title("Reporte de Presupuesto Total por Obras")
    st.write("Suma el presupuesto total de materiales de todas las obras.")

    if st.session_state.df_presupuesto_materiales.empty:
        st.info("No hay presupuesto de materiales registrado para ninguna obra.")
        return

    # Asegurar que la columna calculada existe y los datos son numéricos
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated
    # Ensure ID_Obra and Material are strings for grouping
    df_presupuesto['ID_Obra'] = df_presupuesto['ID_Obra'].astype(str)
    df_presupuesto['Material'] = df_presupuesto['Material'].astype(str)


    # Agrupar por obra
    # Only group if the necessary columns exist
    if 'ID_Obra' in df_presupuesto.columns and 'Cantidad_Presupuestada' in df_presupuesto.columns and 'Costo_Presupuestado' in df_presupuesto.columns:
        reporte_por_obra = df_presupuesto.groupby('ID_Obra').agg(
            Cantidad_Total_Presupuestada=('Cantidad_Presupuestada', 'sum'),
            Costo_Total_Presupuestado=('Costo_Presupuestado', 'sum')
        ).reset_index()

        # Unir con nombres de obras (ensure ID_Obra is string in df_proyectos too for merge)
        df_proyectos_temp = st.session_state.df_proyectos.copy()
        if 'ID_Obra' in df_proyectos_temp.columns:
            df_proyectos_temp['ID_Obra'] = df_proyectos_temp['ID_Obra'].astype(str)
            reporte_por_obra = reporte_por_obra.merge(df_proyectos_temp[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')
            reporte_por_obra['Nombre_Obra'] = reporte_por_obra['Nombre_Obra'].fillna(reporte_por_obra['ID_Obra'] + ' (Desconocida)') # Use ID if name missing
        else:
             reporte_por_obra['Nombre_Obra'] = reporte_por_obra['ID_Obra'] + ' (Desconocida)'


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

    else:
        st.warning("Los datos de presupuesto no contienen las columnas necesarias ('ID_Obra', 'Cantidad_Presupuestada', 'Costo_Presupuestado') para generar el reporte.")


def page_compras_asignacion():
    st.title("Gestión de Compras y Asignación de Materiales")
    st.write("Registra las compras y asigna materiales a las obras.")

    st.subheader("Registrar Compra de Materiales")
    with st.form("form_add_compra", clear_on_submit=True):
        fecha_compra = st.date_input("Fecha de Compra", value=datetime.date.today(), key="compra_fecha_add")
        material_compra = st.text_input("Nombre del Material Comprado", key="compra_material_add").strip()
        cantidad_comprada = st.number_input("Cantidad Comprada", min_value=0.0, format="%.2f", key="compra_cantidad_add")
        precio_unitario_comprado = st.number_input("Precio Unitario de Compra", min_value=0.0, format="%.2f", key="compra_precio_add")
        submitted = st.form_submit_button("Registrar Compra")
        if submitted:
            if not fecha_compra or not material_compra or cantidad_comprada < 0 or precio_unitario_comprado < 0:
                st.warning("Por favor, complete la fecha, material, cantidad (>=0) y precio (>=0).")
            elif cantidad_comprada == 0 and precio_unitario_comprado == 0:
                st.warning("La cantidad y el precio unitario comprados no pueden ser ambos cero.")
            else: # All validations pass
                 # Generar ID único simple (usando timestamp + counter)
                current_ids = set(st.session_state.df_compras_materiales['ID_Compra'].astype(str)) # Get existing IDs
                base_id = f"COMPRA_{int(time.time() * 1000)}"
                id_compra = base_id
                counter = 0
                while id_compra in current_ids:
                    counter += 1
                    id_compra = f"{base_id}_{counter}"


                new_compra_data = {
                    'ID_Compra': id_compra,
                    'Fecha_Compra': fecha_compra, # Handled by save_table
                    'Material': material_compra,
                    'Cantidad_Comprada': cantidad_comprada,
                    'Precio_Unitario_Comprado': precio_unitario_comprado
                }
                new_compra = pd.DataFrame([new_compra_data])
                # Ensure all expected columns exist and types handled
                new_compra_processed = get_initialized_dataframe(TABLE_COMPRAS_MATERIALES).append(new_compra, ignore_index=True).tail(1)
                new_compra_processed.update(new_compra_data) # Restore values

                new_compra_processed = calcular_costo_compra(new_compra_processed)

                st.session_state.df_compras_materiales = pd.concat([st.session_state.df_compras_materiales, new_compra_processed], ignore_index=True)
                save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES) # Save to DB
                st.success(f"Compra de '{material_compra}' registrada con ID: {id_compra}")
                st.experimental_rerun() # Rerun to update the history view and assignment options


    st.subheader("Historial de Compras")
    if st.session_state.df_compras_materiales.empty:
        st.info("No hay compras registradas aún.")
    else:
         st.info("Edite la tabla siguiente para modificar o eliminar compras. Use el ícono de papelera para eliminar filas.")
         # Usar data_editor para permitir edición
         df_compras_editable = st.session_state.df_compras_materiales.copy()
         # Ensure date is datetime64[ns] for data_editor
         if 'Fecha_Compra' in df_compras_editable.columns:
              df_compras_editable['Fecha_Compra'] = pd.to_datetime(df_compras_editable['Fecha_Compra'], errors='coerce')

         # Recalculate cost *before* display in editor if needed
         # df_compras_editable = calcular_costo_compra(df_compras_editable)


         df_compras_edited = st.data_editor(
             df_compras_editable[['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']], # Select columns to display/edit
             key="data_editor_compras",
             num_rows="dynamic", # Allow adding/deleting rows
             column_config={
                 "ID_Compra": st.column_config.TextColumn("ID Compra", disabled=True),
                 "Fecha_Compra": st.column_config.DateColumn("Fecha Compra", required=True),
                 "Material": st.column_config.TextColumn("Material", required=True),
                 "Cantidad_Comprada": st.column_config.NumberColumn("Cantidad Comprada", min_value=0.0, format="%.2f", required=True),
                 "Precio_Unitario_Comprado": st.column_config.NumberColumn("Precio Unitario Compra", min_value=0.0, format="%.2f", required=True),
                 # Show calculated cost but don't allow editing
                 "Costo_Compra": st.column_config.NumberColumn("Costo Compra", disabled=True, format="%.2f")
             }
         )

         # Logic for saving changes from data_editor
         # Data editor returns a copy of only the displayed columns.
         # Need to handle potential new rows (no ID) and recalculate cost for edited rows.
         df_compras_edited_with_all_cols = df_compras_edited.copy()

         # Recalculate Costo_Compra based on edited Qty/Price in the edited data
         df_compras_edited_with_all_cols = calcular_costo_compra(df_compras_edited_with_all_cols)

         # Handle new rows added via the editor - generate IDs if missing
         new_row_mask = df_compras_edited_with_all_cols['ID_Compra'].isnull() | (df_compras_edited_with_all_cols['ID_Compra'].astype(str).str.strip() == '')
         if new_row_mask.any():
              existing_ids = set(df_compras_edited_with_all_cols['ID_Compra'].dropna().astype(str))
              new_ids = []
              for i in range(new_row_mask.sum()):
                   base_id = f"COMPRA_EDIT_{int(time.time() * 1000)}_{i}"
                   unique_id = base_id
                   counter = 0
                   while unique_id in existing_ids:
                       counter += 1
                       unique_id = f"{base_id}_{counter}"
                   new_ids.append(unique_id)
                   existing_ids.add(unique_id) # Add to set for subsequent checks
              df_compras_edited_with_all_cols.loc[new_row_mask, 'ID_Compra'] = new_ids
              # Update session state temporarily with the version with generated IDs
              # The next check will compare against the *original* session state

         # Compare the processed edited dataframe with the original session state dataframe
         # Use consistent columns and order for comparison
         cols_for_comparison = list(df_compras_edited_with_all_cols.columns) # Use columns from edited including ID/Costo
         df_compras_original_sorted = st.session_state.df_compras_materiales[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)
         df_compras_edited_sorted = df_compras_edited_with_all_cols[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)

         if not df_compras_edited_sorted.equals(df_compras_original_sorted):

              st.session_state.df_compras_materiales = df_compras_edited_with_all_cols.copy() # Update session state with the potentially edited data

              if st.button("Guardar Cambios en Historial de Compras", key="save_compras_button"):
                 # Validar antes de guardar
                 df_to_save = st.session_state.df_compras_materiales.dropna(subset=['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado'], how='all').copy() # Drop fully empty or missing required

                 if df_to_save['ID_Compra'].isnull().any() or (df_to_save['ID_Compra'].astype(str).str.strip() == '').any(): # Check for rows where ID generation failed? (shouldn't happen with logic above)
                     st.error("Error interno: No se pudo generar ID para una o más filas.")
                 elif df_to_save['Fecha_Compra'].isnull().any():
                      st.error("Error: El campo 'Fecha Compra' no puede estar vacío o tener formato inválido.")
                 elif df_to_save['Material'].isnull().any() or (df_to_save['Material'].astype(str).str.strip() == '').any():
                      st.error("Error: El campo 'Material' no puede estar vacío.")
                 elif df_to_save['Cantidad_Comprada'].isnull().any(): # Check if required numeric fields are NaN (from data_editor)
                       st.error("Error: El campo 'Cantidad Comprada' no puede estar vacío.")
                 elif df_to_save['Precio_Unitario_Comprado'].isnull().any():
                      st.error("Error: El campo 'Precio Unitario Compra' no puede estar vacío.")
                 else: # All validations pass
                      save_table(df_to_save, DATABASE_FILE, TABLE_COMPRAS_MATERIALES) # Save the validated DF
                      st.success("Cambios en historial de compras guardados.")
                      st.experimental_rerun() # Opcional: recargar para mostrar el DF actualizado
              else:
                 st.info("Hay cambios sin guardar en el historial de compras.")


    st.markdown("---")

    st.subheader("Asignar Materiales a Obra")
    obras_disponibles_assign = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_assign = [id for id in obras_disponibles_assign if pd.notna(id) and str(id).strip() != ''] # Filter out invalid IDs

    if not obras_disponibles_assign:
        st.warning("No hay obras creadas. No se pueden asignar materiales.")
        # Ensure state variable doesn't hold invalid value
        if "asig_obra" in st.session_state: del st.session_state["asig_obra"]
        return

    # Build options for the obra selectbox
    obra_options_assign = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].isin(obras_disponibles_assign)][['ID_Obra', 'Nombre_Obra']].to_dict('records')
    obra_assign_labels = [f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})" for o in obra_options_assign]

    # List materials from purchases for convenience (optional, still allow free text)
    materiales_comprados_unicos = st.session_state.df_compras_materiales['Material'].unique().tolist()
    # Filter out potential None/NaN/empty string materials
    materiales_comprados_unicos = [m for m in materiales_comprados_unicos if pd.notna(m) and str(m).strip() != '']

    with st.form("form_asignar_material", clear_on_submit=True):
        fecha_asignacion = st.date_input("Fecha de Asignación", value=datetime.date.today(), key="asig_fecha")

        # Selectbox for Obra
        selected_obra_label_assign = st.selectbox(
            "Seleccione Obra de Destino:",
            options=obra_assign_labels,
            key="asig_obra"
        )

        # Find the corresponding ID_Obra from the selected label
        obra_destino_id = None
        for o in obra_options_assign:
            if selected_obra_label_assign == f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})":
                 obra_destino_id = o['ID_Obra']
                 break # Found the ID


        # Allow selecting from purchased materials or typing manually
        material_input_method = st.radio("¿Cómo seleccionar material?", ["Seleccionar de compras", "Escribir manualmente"], key="material_input_method_radio")

        material_asignado = None
        if material_input_method == "Seleccionar de compras":
             if materiales_comprados_unicos:
                  material_asignado = st.selectbox("Material a Asignar:", materiales_comprados_unicos, key="asig_material_select")
                  # Optionally get price suggestion from last purchase
                  if material_asignado and material_asignado != '':
                      last_purchase = st.session_state.df_compras_materiales[st.session_state.df_compras_materiales['Material'].astype(str).str.strip() == material_asignado].sort_values('Fecha_Compra').iloc[::-1]
                      suggested_price = last_purchase['Precio_Unitario_Comprado'].iloc[0] if not last_purchase.empty else 0.0
                  else:
                      suggested_price = 0.0

             else:
                  st.info("No hay materiales registrados en compras. Use 'Escribir manualmente'.")
                  # Fallback to manual input if no purchased materials available
                  material_input_method = "Escribir manualmente" # Force radio state
                  st.experimental_rerun() # Re-run to show the correct input type for material
                  # Need to return or set material_asignado and suggested_price default value if previous rerun happens

        if material_input_method == "Escribir manualmente": # Also for the fallback scenario
             material_asignado = st.text_input("Nombre del Material a Asignar", key="asig_material_manual").strip()
             suggested_price = 0.0 # No price suggestion for manual input


        cantidad_asignada = st.number_input("Cantidad a Asignar", min_value=0.0, format="%.2f", key="asig_cantidad")
        # Precio al que se ASIGNA (puede ser diferente al de compra, ej. costo promedio, o ingreso manual del costo real)
        # Use the suggested price from last purchase if selecting, otherwise default
        precio_unitario_asignado = st.number_input(
            "Precio Unitario Asignado (Costo Real)",
            min_value=0.0, format="%.2f", key="asig_precio",
            value=suggested_price if material_input_method == "Seleccionar de compras" else 0.0
        )

        submitted = st.form_submit_button("Asignar Material")
        if submitted:
            # Check for required fields and valid numeric inputs
            if obra_destino_id is None or obra_destino_id == pd.NA or str(obra_destino_id).strip() == '':
                 st.warning("Por favor, seleccione una obra de destino válida.")
            elif not fecha_asignacion:
                st.warning("Por favor, complete la fecha de asignación.")
            elif not material_asignado or str(material_asignado).strip() == '':
                 st.warning("Por favor, complete el nombre del material.")
            elif cantidad_asignada < 0 or precio_unitario_asignado < 0:
                st.warning("Cantidad y Precio Unitario a asignar deben ser >= 0.")
            elif cantidad_asignada == 0 and precio_unitario_asignado == 0:
                 st.warning("La cantidad y el precio unitario asignado no pueden ser ambos cero si desea registrar una asignación significativa.")
            else: # All validations pass
                 # Generar ID único simple (usando timestamp + counter)
                 current_ids = set(st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str)) # Get existing IDs
                 base_id = f"ASIG_{int(time.time() * 1000)}"
                 id_asignacion = base_id
                 counter = 0
                 while id_asignacion in current_ids:
                     counter += 1
                     id_asignacion = f"{base_id}_{counter}"

                 new_asignacion_data = {
                     'ID_Asignacion': id_asignacion,
                     'Fecha_Asignacion': fecha_asignacion, # Handled by save_table
                     'ID_Obra': obra_destino_id,
                     'Material': material_asignado,
                     'Cantidad_Asignada': cantidad_asignada,
                     'Precio_Unitario_Asignado': precio_unitario_asignado
                 }
                 new_asignacion = pd.DataFrame([new_asignacion_data])
                 # Ensure all expected columns exist
                 new_asignacion_processed = get_initialized_dataframe(TABLE_ASIGNACION_MATERIALES).append(new_asignacion, ignore_index=True).tail(1)
                 new_asignacion_processed.update(new_asignacion_data) # Restore values

                 new_asignacion_processed = calcular_costo_asignado(new_asignacion_processed)

                 st.session_state.df_asignacion_materiales = pd.concat([st.session_state.df_asignacion_materiales, new_asignacion_processed], ignore_index=True)
                 save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB

                 # Find obra name for success message
                 obra_name_for_success = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_destino_id]['Nombre_Obra'].iloc[0] if obra_destino_id in st.session_state.df_proyectos['ID_Obra'].values else "ID Desconocida"

                 st.success(f"Material '{material_asignado}' ({cantidad_asignada:.2f} unidades) asignado a obra '{obra_name_for_success}'.")
                 st.experimental_rerun()


    st.subheader("Historial de Asignaciones")
    if st.session_state.df_asignacion_materiales.empty:
        st.info("No hay materiales asignados aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar asignaciones. Use el ícono de papelera para eliminar filas.")
         # Usar data_editor para permitir edición
        df_asignaciones_editable = st.session_state.df_asignacion_materiales.copy()
         # Ensure date is datetime64[ns] for data_editor
        if 'Fecha_Asignacion' in df_asignaciones_editable.columns:
              df_asignaciones_editable['Fecha_Asignacion'] = pd.to_datetime(df_asignaciones_editable['Fecha_Asignacion'], errors='coerce')
         # Recalculate cost *before* display in editor if needed
         # df_asignaciones_editable = calcular_costo_asignado(df_asignaciones_editable)


        # Options for ID_Obra in data_editor (should match current valid obra IDs)
        obra_ids_for_editor = obras_disponibles_assign # Use the validated list of available work IDs
        if not obra_ids_for_editor:
             st.warning("No hay obras disponibles para editar asignaciones. Algunos registros pueden tener IDs de obra inválidos.")
             # Show the data without an interactive editor or with limited columns if obra IDs are mandatory but none exist
             # Fallback to simple dataframe display if editor config becomes impossible
             # st.dataframe(df_asignaciones_editable) # Option to just show data
             # return # Exit to prevent editor if it relies on mandatory valid obra IDs


        # If there are valid obra IDs, proceed with data editor config
        if obra_ids_for_editor:
            df_asignaciones_edited = st.data_editor(
                 df_asignaciones_editable[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']],
                 key="data_editor_asignaciones",
                 num_rows="dynamic", # Allow adding/deleting rows
                  column_config={
                      "ID_Asignacion": st.column_config.TextColumn("ID Asignación", disabled=True),
                      "Fecha_Asignacion": st.column_config.DateColumn("Fecha Asignación", required=True),
                      "ID_Obra": st.column_config.SelectboxColumn("ID Obra", options=obra_ids_for_editor, required=True), # Use Selectbox for existing valid work IDs
                      "Material": st.column_config.TextColumn("Material", required=True),
                      "Cantidad_Asignada": st.column_config.NumberColumn("Cantidad Asignada", min_value=0.0, format="%.2f", required=True),
                      "Precio_Unitario_Asignado": st.column_config.NumberColumn("Precio Unitario Asignado", min_value=0.0, format="%.2f", required=True),
                      # Show calculated cost but don't allow editing
                      "Costo_Asignado": st.column_config.NumberColumn("Costo Asignado", disabled=True, format="%.2f")
                  }
             )
             # Logic for saving changes from data_editor
             df_asignaciones_edited_with_all_cols = df_asignaciones_edited.copy()

             # Recalculate Costo_Asignado based on edited Qty/Price
             df_asignaciones_edited_with_all_cols = calcular_costo_asignado(df_asignaciones_edited_with_all_cols)

             # Handle new rows added via the editor - generate IDs if missing
             new_row_mask = df_asignaciones_edited_with_all_cols['ID_Asignacion'].isnull() | (df_asignaciones_edited_with_all_cols['ID_Asignacion'].astype(str).str.strip() == '')
             if new_row_mask.any():
                  existing_ids = set(df_asignaciones_edited_with_all_cols['ID_Asignacion'].dropna().astype(str))
                  new_ids = []
                  for i in range(new_row_mask.sum()):
                       base_id = f"ASIG_EDIT_{int(time.time() * 1000)}_{i}"
                       unique_id = base_id
                       counter = 0
                       while unique_id in existing_ids:
                           counter += 1
                           unique_id = f"{base_id}_{counter}"
                       new_ids.append(unique_id)
                       existing_ids.add(unique_id)
                  df_asignaciones_edited_with_all_cols.loc[new_row_mask, 'ID_Asignacion'] = new_ids

             # Compare edited with current session state dataframe
             cols_for_comparison = list(df_asignaciones_edited_with_all_cols.columns) # Use columns from edited including ID/Costo
             df_asignaciones_original_sorted = st.session_state.df_asignacion_materiales[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)
             df_asignaciones_edited_sorted = df_asignaciones_edited_with_all_cols[cols_for_comparison].sort_values(cols_for_comparison).reset_index(drop=True)


             if not df_asignaciones_edited_sorted.equals(df_asignaciones_original_sorted):
                  st.session_state.df_asignacion_materiales = df_asignaciones_edited_with_all_cols.copy()

                  if st.button("Guardar Cambios en Historial de Asignaciones", key="save_asignaciones_button"):
                      # Validar antes de guardar
                      df_to_save = st.session_state.df_asignacion_materiales.dropna(subset=['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado'], how='all').copy() # Drop completely empty/missing required

                      if df_to_save['ID_Asignacion'].isnull().any() or (df_to_save['ID_Asignacion'].astype(str).str.strip() == '').any():
                           st.error("Error interno: No se pudo generar ID para una o más filas.")
                      elif df_to_save['Fecha_Asignacion'].isnull().any():
                           st.error("Error: El campo 'Fecha Asignación' no puede estar vacío o tener formato inválido.")
                      elif df_to_save['ID_Obra'].isnull().any() or (df_to_save['ID_Obra'].astype(str).str.strip() == ''):
                           st.error("Error: El campo 'ID Obra' no puede estar vacío.") # Required by SelectboxColumn
                      # The SelectboxColumn prevents selecting a non-existent ID, but if data was loaded with invalid IDs,
                      # it might appear as a warning in the editor. The 'replace' save would just save these rows.
                      # You could add a strict check here if needed:
                      # elif not df_to_save['ID_Obra'].isin(obras_disponibles_assign).all():
                      #      st.error("Error: Una o más asignaciones tienen un 'ID Obra' que no existe en la lista actual de obras.")
                      elif df_to_save['Material'].isnull().any() or (df_to_save['Material'].astype(str).str.strip() == ''):
                           st.error("Error: El campo 'Material' no puede estar vacío.")
                      elif df_to_save['Cantidad_Asignada'].isnull().any():
                           st.error("Error: El campo 'Cantidad Asignada' no puede estar vacío.")
                      elif df_to_save['Precio_Unitario_Asignado'].isnull().any():
                           st.error("Error: El campo 'Precio Unitario Asignado' no puede estar vacío.")
                      else: # All validations pass
                           save_table(df_to_save, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save the validated DF
                           st.success("Cambios en historial de asignaciones guardados.")
                           st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en el historial de asignaciones.")

        else: # Case where obra_ids_for_editor is empty after all
             st.dataframe(df_asignaciones_editable) # Just display non-editable if needed

        # --- Deshacer Asignación Section ---
        # This is a specific workflow for deletion/correction, separate from the data_editor deletion
        st.markdown("---")
        st.subheader("Deshacer Asignación (Eliminar por ID)")

        # Get available assignment IDs for selectbox
        asignaciones_disponibles = st.session_state.df_asignacion_materiales['ID_Asignacion'].unique().tolist()
        asignaciones_disponibles = [id for id in asignaciones_disponibles if pd.notna(id) and str(id).strip() != '']


        if not asignaciones_disponibles:
            st.info("No hay asignaciones para eliminar por ID.")
        else:
            # Fetch brief info for the selectbox display (ensure data types for formatting)
            df_asig_info = st.session_state.df_asignacion_materiales[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada']].copy()
            if 'Fecha_Asignacion' in df_asig_info.columns:
                df_asig_info['Fecha_Asignacion'] = pd.to_datetime(df_asig_info['Fecha_Asignacion'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('Fecha Inválida')
            df_asig_info['ID_Obra'] = df_asig_info['ID_Obra'].astype(str).fillna('N/A')
            df_asig_info['Material'] = df_asig_info['Material'].astype(str).fillna('N/A')
            df_asig_info['Cantidad_Asignada'] = df_asig_info['Cantidad_Asignada'].fillna(0.0).round(2) # Ensure numeric, fillna, round


            # Create a dictionary map from ID to the info dictionary
            asig_options_dict = df_asig_info.set_index('ID_Asignacion').to_dict('index')

            # Create format func that safely accesses dict elements
            def format_assignment_option(asig_id):
                # Use the original ID string as key, fallback to dict if not found (e.g. for st.empty option or other state)
                info = asig_options_dict.get(asig_id, {})
                fecha_str = info.get('Fecha_Asignacion', 'N/A')
                obra_id = info.get('ID_Obra', 'N/A')
                material = info.get('Material', 'N/A')
                cantidad = info.get('Cantidad_Asignada', 0.0) # Get as float for formatting
                return f"{asig_id} ({fecha_str} - {obra_id} - {material} - {cantidad:.2f})" # Use formatted quantity


            id_asignacion_deshacer = st.selectbox(
                "Seleccione ID de Asignación a eliminar:",
                options=asignaciones_disponibles, # Only provide valid IDs from current DF
                format_func=format_assignment_option, # Use the safer format function
                key="deshacer_asig_select"
            )

            if st.button(f"Eliminar Asignación Seleccionada ({id_asignacion_deshacer})", key="deshacer_asig_button"):
                st.session_state.df_asignacion_materiales = st.session_state.df_asignacion_materiales[
                    st.session_state.df_asignacion_materiales['ID_Asignacion'] != id_asignacion_deshacer
                ].copy() # Use .copy() to avoid SettingWithCopyWarning later
                save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB
                st.success(f"Asignación {id_asignacion_deshacer} eliminada.")
                st.experimental_rerun()


def page_reporte_variacion_total_obras():
    st.title("Reporte de Variación Total Obras (Presupuesto vs Real)")
    st.write("Compara el costo total presupuestado vs el costo total real (asignado) para cada obra.")

    if st.session_state.df_presupuesto_materiales.empty and st.session_state.df_asignacion_materiales.empty:
        st.info("No hay datos de presupuesto ni de asignación para generar el reporte.")
        return

    # Calcular totales presupuestados por obra
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated
    # Ensure ID_Obra is string for grouping/merging
    if 'ID_Obra' in df_presupuesto.columns:
        df_presupuesto['ID_Obra'] = df_presupuesto['ID_Obra'].astype(str)
    else:
         df_presupuesto['ID_Obra'] = 'ID Desconocida' # Handle missing ID column

    presupuesto_total_obra = df_presupuesto.groupby('ID_Obra')['Costo_Presupuestado'].sum().reset_index(name='Costo_Presupuestado_Total') if not df_presupuesto.empty and 'Costo_Presupuestado' in df_presupuesto.columns else pd.DataFrame(columns=['ID_Obra', 'Costo_Presupuestado_Total'])
    presupuesto_cantidad_obra = df_presupuesto.groupby('ID_Obra')['Cantidad_Presupuestada'].sum().reset_index(name='Cantidad_Presupuestada_Total') if not df_presupuesto.empty and 'Cantidad_Presupuestada' in df_presupuesto.columns else pd.DataFrame(columns=['ID_Obra', 'Cantidad_Presupuestada_Total'])


    # Calcular totales asignados por obra
    df_asignacion = st.session_state.df_asignacion_materiales.copy()
    df_asignacion = calcular_costo_asignado(df_asignacion) # Ensure cost is calculated/updated
    # Ensure ID_Obra is string for grouping/merging
    if 'ID_Obra' in df_asignacion.columns:
         df_asignacion['ID_Obra'] = df_asignacion['ID_Obra'].astype(str)
    else:
         df_asignacion['ID_Obra'] = 'ID Desconocida' # Handle missing ID column

    asignacion_total_obra = df_asignacion.groupby('ID_Obra')['Costo_Asignado'].sum().reset_index(name='Costo_Asignado_Total') if not df_asignacion.empty and 'Costo_Asignado' in df_asignacion.columns else pd.DataFrame(columns=['ID_Obra', 'Costo_Asignado_Total'])
    asignacion_cantidad_obra = df_asignacion.groupby('ID_Obra')['Cantidad_Asignada'].sum().reset_index(name='Cantidad_Asignada_Total') if not df_asignacion.empty and 'Cantidad_Asignada' in df_asignacion.columns else pd.DataFrame(columns=['ID_Obra', 'Cantidad_Asignada_Total'])


    # Unir DataFrames de costo y cantidad (Outer merge to include all obras from both sides)
    reporte_variacion_obras = pd.merge(presupuesto_total_obra, asignacion_total_obra, on='ID_Obra', how='outer')
    reporte_variacion_obras = pd.merge(reporte_variacion_obras, presupuesto_cantidad_obra, on='ID_Obra', how='outer')
    reporte_variacion_obras = pd.merge(reporte_variacion_obras, asignacion_cantidad_obra, on='ID_Obra', how='outer').fillna(0) # Fill NaN introduced by outer merge with 0


    # Unir con nombres de obras (Left merge, ensure ID_Obra is string in df_proyectos too for merge)
    df_proyectos_temp = st.session_state.df_proyectos.copy()
    if 'ID_Obra' in df_proyectos_temp.columns:
         df_proyectos_temp['ID_Obra'] = df_proyectos_temp['ID_Obra'].astype(str)
         reporte_variacion_obras = reporte_variacion_obras.merge(df_proyectos_temp[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras['Nombre_Obra'].fillna(reporte_variacion_obras['ID_Obra'] + ' (Desconocida)') # Use ID if name missing
    else:
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras['ID_Obra'] + ' (Desconocida)'


    # Calcular variación
    reporte_variacion_obras['Variacion_Total_Costo'] = reporte_variacion_obras['Costo_Asignado_Total'] - reporte_variacion_obras['Costo_Presupuestado_Total']
    reporte_variacion_obras['Variacion_Total_Cantidad'] = reporte_variacion_obras['Cantidad_Asignada_Total'] - reporte_variacion_obras['Cantidad_Presupuestada_Total']

    # Sort by Nombre_Obra or ID_Obra for better presentation
    if 'Nombre_Obra' in reporte_variacion_obras.columns:
        reporte_variacion_obras = reporte_variacion_obras.sort_values('Nombre_Obra').reset_index(drop=True)
    elif 'ID_Obra' in reporte_variacion_obras.columns:
        reporte_variacion_obras = reporte_variacion_obras.sort_values('ID_Obra').reset_index(drop=True)


    st.subheader("Variación de Costo y Cantidad por Obra (Presupuesto vs Real)")
    if reporte_variacion_obras.empty:
        st.info("No hay datos válidos para generar el reporte de variación por obra.")
    else:
        # Ensure all display columns exist before subsetting
        display_cols = ['Nombre_Obra', 'ID_Obra',
                        'Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total', 'Variacion_Total_Cantidad',
                        'Costo_Presupuestado_Total', 'Costo_Asignado_Total', 'Variacion_Total_Costo']
        display_cols_present = [col for col in display_cols if col in reporte_variacion_obras.columns]

        st.dataframe(reporte_variacion_obras[display_cols_present].round(2))

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

            # Añadir variaciones por obra (solo costo, solo si hay variación significativa en la obra)
            reporte_variacion_obras_significant_cost_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Costo']) >= 0.01].sort_values('Variacion_Total_Costo', ascending=False).copy() # Add copy


            for index, row in reporte_variacion_obras_significant_cost_var.iterrows():
                 # Use shorter label for graph if needed
                 obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else row['ID_Obra'] + ' (Desconocida)'
                 if len(obra_label) > 20: # Arbitrary length limit for display
                      obra_label = obra_label[:17] + '...'
                 labels_costo.append(f"Var: {obra_label}")

                 values_costo.append(row['Variacion_Total_Costo'])
                 measures_costo.append('relative')
                 texts_costo.append(f"${row['Variacion_Total_Costo']:,.2f}")

            # Añadir el total asignado
            labels_costo.append('Total Asignado')
            values_costo.append(total_asignado_general)
            measures_costo.append('total')
            texts_costo.append(f"${total_asignado_general:,.2f}")

            # Check again after adding variation bars if there's more than just the start and end points
            # Need at least one relative bar OR start != end absolute totals
            if len(labels_costo) > 2 or (len(labels_costo) == 2 and abs(values_costo[0] - values_costo[1]) >= 0.01):
                fig_total_variacion_costo = go.Figure(go.Waterfall(
                    name = "Variación Total Costo",
                    orientation = "v",
                    measure = measures_costo,
                    x = labels_costo,
                    textposition = "outside",
                    text = texts_costo,
                    y = values_costo,
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                    # Define colors: green for decrease (good variance), red for increase (bad variance)
                    # Note: increasing means moving up relative to previous bar. Negative variation decreases, Positive increases.
                    increasing = {"marker":{"color":"#FF4136"}}, # Over budget -> Bad -> Red
                    decreasing = {"marker":{"color":"#3D9970"}}, # Under budget -> Good -> Green
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}} # Blue
                ))

                fig_total_variacion_costo.update_layout(
                    title = 'Variación Total de Costos Materiales (Presupuesto vs Real) - Todas las Obras',
                    showlegend = False,
                    yaxis_title="Monto ($)",
                    margin=dict(l=20, r=20, t=60, b=20),
                    height=600
                )

                st.plotly_chart(fig_total_variacion_costo, use_container_width=True)
            elif abs(total_presupuestado_general) < 0.01 and abs(total_asignado_general) < 0.01:
                 st.info("El presupuesto total y el costo asignado total son ambos cero o insignificantes.")
            else: # Only start and end bars, and start == end
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

            # Añadir variaciones por obra (solo cantidad, solo si hay variación significativa en la obra)
            reporte_variacion_obras_significant_qty_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Cantidad']) >= 0.01].sort_values('Variacion_Total_Cantidad', ascending=False).copy()


            for index, row in reporte_variacion_obras_significant_qty_var.iterrows():
                 # Use shorter label for graph
                 obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else row['ID_Obra'] + ' (Desconocida)'
                 if len(obra_label) > 20: # Arbitrary length limit
                      obra_label = obra_label[:17] + '...'
                 labels_cantidad.append(f"Var Cant: {obra_label}")

                 values_cantidad.append(row['Variacion_Total_Cantidad'])
                 measures_cantidad.append('relative')
                 texts_cantidad.append(f"{row['Variacion_Total_Cantidad']:,.2f}")

            # Añadir el total asignado
            labels_cantidad.append('Total Asignado (Cant.)')
            values_cantidad.append(total_cantidad_asignada_general)
            measures_cantidad.append('total')
            texts_cantidad.append(f"{total_cantidad_asignada_general:,.2f}")

            # Check again after adding variation bars
            if len(labels_cantidad) > 2 or (len(labels_cantidad) == 2 and abs(values_cantidad[0] - values_cantidad[1]) >= 0.01): # More than just start/end OR start!=end
                fig_total_variacion_cantidad = go.Figure(go.Waterfall(
                    name = "Variación Total Cantidad",
                    orientation = "v",
                    measure = measures_cantidad,
                    x = labels_cantidad,
                    textposition = "outside",
                    text = texts_cantidad,
                    y = values_cantidad,
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                    # Define colors: red for increase (typically bad), green for decrease (potentially good, but depends)
                    # In Quantity variation, over quantity (red) might mean waste or bad planning. Under (green) might mean efficiency or shortage.
                    increasing = {"marker":{"color":"#FF4136"}}, # Quantity over budget -> Bad -> Red
                    decreasing = {"marker":{"color":"#3D9970"}}, # Quantity under budget -> Potentially Good -> Green (depending on context)
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}} # Blue
                ))

                fig_total_variacion_cantidad.update_layout(
                    title = 'Variación Total de Cantidades Materiales (Presupuesto vs Real) - Todas las Obras',
                    showlegend = False,
                    yaxis_title="Cantidad",
                    margin=dict(l=20, r=20, t=60, b=20),
                    height=600
                )

                st.plotly_chart(fig_total_variacion_cantidad, use_container_width=True)
            elif abs(total_cantidad_presupuestada_general) < 0.01 and abs(total_cantidad_asignada_general) < 0.01:
                 st.info("La cantidad total presupuestada y asignada son ambas cero o insignificantes.")
            else: # Only start and end bars, and start == end
                 st.info("La cantidad total presupuestada es igual a la cantidad total asignada o la variación total es insignificante. No hay variación de cantidad para mostrar en el gráfico.")

        else:
            st.info("No hay cantidad presupuestada ni asignada total para mostrar el gráfico de variación de cantidad.")


# --- Main App Logic ---

# --- Sidebar Navigation ---
with st.sidebar:
    st.title("Menú Principal")

    # Define the pages for navigation
    pages = {
        "Dashboard Principal": "dashboard",
        "Gestión de Flotas": "gestion_flotas",
        "Gestión de Equipos": "equipos",
        "Registro de Consumibles": "consumibles",
        "Registro de Costos Equipos": "costos_equipos",
        "Reportes Mina (Consumo/Costo)": "reportes_mina",
        "Variación Costos Flota": "variacion_costos_flota",
        "--- Gestión de Obras y Materiales ---": None, # Separator
        "Gestión de Obras (Proyectos)": "gestion_obras",
        "Reporte Presupuesto Total Obras": "reporte_presupuesto_total_obras",
        "Gestión Compras y Asignación": "compras_asignacion",
        "Reporte Variación Total Obras": "reporte_variacion_total_obras",
    }

    # Use a stable key for the radio button
    selected_page_key = st.radio("Ir a:", list(pages.keys()), index=0, key="main_navigation_radio")
    selected_page = pages[selected_page_key]


# --- Main Content Area based on selection ---
# Call the function of the selected page
if selected_page == "dashboard":
    st.title("Dashboard Principal")
    st.write(f"Bienvenido al sistema de gestión para la empresa proveedora de minería.")
    st.info("Seleccione una opción del menú lateral para comenzar.")
    st.markdown("---")
    st.subheader("Resumen Rápido")
    total_equipos = len(st.session_state.df_equipos)
    total_obras = len(st.session_state.df_proyectos)
    total_flotas = len(st.session_state.df_flotas)
    total_presupuesto_materiales = calcular_costo_presupuestado(st.session_state.df_presupuesto_materiales.copy())['Costo_Presupuestado'].sum() if not st.session_state.df_presupuesto_materiales.empty and 'Costo_Presupuestado' in st.session_state.df_presupuesto_materiales.columns else 0
    total_comprado_materiales = calcular_costo_compra(st.session_state.df_compras_materiales.copy())['Costo_Compra'].sum() if not st.session_state.df_compras_materiales.empty and 'Costo_Compra' in st.session_state.df_compras_materiales.columns else 0

    col_summary1, col_summary2, col_summary3, col_summary4, col_summary5 = st.columns(5)
    with col_summary1:
        st.metric("Total Equipos", total_equipos)
    with col_summary2:
         st.metric("Total Flotas", total_flotas)
    with col_summary3:
         st.metric("Total Obras", total_obras)
    with col_summary4:
         st.metric("Presupuesto Materiales Total", f"${total_presupuesto_materiales:,.0f}")
    with col_summary5:
         st.metric("Compras Materiales Total", f"${total_comprado_materiales:,.0f}")


elif selected_page == "gestion_flotas":
    page_flotas()
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
    st.empty() # For separator

