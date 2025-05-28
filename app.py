import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import sqlite3
import time
import numpy as np
import datetime # Import datetime module

# --- Initial Configuration ---
st.set_page_config(layout="wide", page_title="Gestión de Equipos y Obras (Minería)")

# --- Data Files (Using SQLite Database) ---
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DATABASE_FILE = os.path.join(DATA_DIR, "app_data.db")

# Define table names for SQLite
TABLE_FLOTAS = "flotas"
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


# --- Define expected columns and their intended pandas dtypes for each table ---
# These dictate the schema expectations and how data is handled.
# Using 'object' for nullable strings or mixed types where pd.NA is suitable.
# Explicitly include calculated columns in the expected structure.
TABLE_COLUMNS = {
    TABLE_FLOTAS: {'ID_Flota': 'object', 'Nombre_Flota': 'object'},
    TABLE_EQUIPOS: {'Interno': 'object', 'Patente': 'object', 'ID_Flota': 'object'},
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

# Define which columns should be datetime types in pandas after loading
DATETIME_COLUMNS = {
    TABLE_CONSUMO: 'Fecha',
    TABLE_COSTOS_SALARIAL: 'Fecha',
    TABLE_GASTOS_FIJOS: 'Fecha',
    TABLE_GASTOS_MANTENIMIENTO: 'Fecha',
    TABLE_PRECIOS_COMBUSTIBLE: 'Fecha',
    TABLE_COMPRAS_MATERIALES: 'Fecha_Compra',
    TABLE_ASIGNACION_MATERIALES: 'Fecha_Asignacion',
}


# --- Helper function to get SQLite connection ---
@st.cache_resource
def get_db_conn():
    """Establece y retorna una conexión a la base de datos SQLite."""
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False)
    return conn

# --- Functions to Load/Save Data using SQLite ---
def load_table(db_file, table_name):
    """Carga datos de una tabla SQLite en un DataFrame."""
    conn = get_db_conn()
    df = pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE;", (table_name,))
        table_exists = cursor.fetchone() is not None

        expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
        expected_cols = list(expected_cols_dict.keys())

        if table_exists:
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)

            # Ensure all expected columns are present, add missing ones with default NA/0
            for col, dtype in expected_cols_dict.items():
                if col not in df.columns:
                    if dtype == 'object':
                        df[col] = pd.NA
                    elif 'float' in dtype or 'int' in dtype:
                        df[col] = 0.0
                    else:
                         df[col] = None # Should not happen with current setup

            # Ensure only expected columns are kept, and in the expected order
            # Use reindex to handle potential missing columns robustly
            df = df.reindex(columns=expected_cols)

            # Convert datetime columns *after* ensuring they exist
            if table_name in DATETIME_COLUMNS:
                date_col = DATETIME_COLUMNS[table_name]
                if date_col in df.columns:
                    # Convert to datetime64[ns], invalid dates become NaT
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    # NaT values are handled correctly by pandas and Streamlit data_editor

        else:
            # Table doesn't exist, return an empty DataFrame with expected columns and pd.NA/0 defaults
            st.warning(f"La tabla '{table_name}' no existe en la base de datos. Creando DataFrame vacío.") # Keep warning for first run
            df = pd.DataFrame(columns=expected_cols)
            # Populate with default NA/0 values to ensure correct dtypes especially for empty table save
            for col, dtype in expected_cols_dict.items():
                 if dtype == 'object':
                      df[col] = pd.Series(dtype=pd.StringDtype(), index=df.index) # Use nullable string dtype
                 elif 'float' in dtype:
                       df[col] = pd.Series(dtype=float, index=df.index).fillna(0.0)
                 elif 'int' in dtype:
                      df[col] = pd.Series(dtype=int, index=df.index).fillna(0)


    except sqlite3.Error as e:
        st.error(f"Error al leer la tabla '{table_name}' de la base de datos: {e}")
        # Return empty DataFrame with expected columns on error
        expected_cols = list(TABLE_COLUMNS.get(table_name, {}).keys())
        df = pd.DataFrame(columns=expected_cols)
        # Still try to initialize with appropriate dtypes/defaults
        for col, dtype in TABLE_COLUMNS.get(table_name, {}).items():
            if dtype == 'object':
                 df[col] = pd.Series(dtype=pd.StringDtype(), index=df.index)
            elif 'float' in dtype:
                 df[col] = pd.Series(dtype=float, index=df.index).fillna(0.0)
            elif 'int' in dtype:
                 df[col] = pd.Series(dtype=int, index=df.index).fillna(0)


    except Exception as e:
         st.error(f"Error general al cargar la tabla '{table_name}': {e}")
         # Return empty DataFrame with expected columns on error
         expected_cols = list(TABLE_COLUMNS.get(table_name, {}).keys())
         df = pd.DataFrame(columns=expected_cols)
         for col, dtype in TABLE_COLUMNS.get(table_name, {}).items():
             if dtype == 'object':
                 df[col] = pd.Series(dtype=pd.StringDtype(), index=df.index)
             elif 'float' in dtype:
                 df[col] = pd.Series(dtype=float, index=df.index).fillna(0.0)
             elif 'int' in dtype:
                 df[col] = pd.Series(dtype=int, index=df.index).fillna(0)


    finally:
        pass # Connection is cached

    return df


def save_table(df, db_file, table_name):
    """Guarda un DataFrame en una tabla SQLite (reemplazando la tabla si existe)."""
    conn = get_db_conn()
    try:
        df_to_save = df.copy() # Work on a copy

        # Ensure all expected columns are present in the DataFrame before saving.
        expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
        for col, dtype in expected_cols_dict.items():
            if col not in df_to_save.columns:
                 if dtype == 'object':
                      df_to_save[col] = pd.NA # Use pandas NA/None which maps to SQL NULL
                 elif 'float' in dtype or 'int' in dtype:
                      df_to_save[col] = 0.0 # Default numeric to 0
                 else:
                     df_to_save[col] = None


        # Ensure only expected columns are kept, and in the expected order for schema clarity
        df_to_save = df_to_save.reindex(columns=list(expected_cols_dict.keys()))

        # Convert datetime columns to string format 'YYYY-MM-DD' before saving
        if table_name in DATETIME_COLUMNS:
            date_col = DATETIME_COLUMNS[table_name]
            if date_col in df_to_save.columns:
                 # Convert datetime64[ns] to string, NaT becomes NaN, fill NaN/pd.NA/None with None for SQL NULL
                 df_to_save[date_col] = pd.to_datetime(df_to_save[date_col], errors='coerce').dt.strftime('%Y-%m-%d').replace({np.nan: None, pd.NA: None, None: None})
            else: # Ensure the column is still created if somehow missing
                df_to_save[date_col] = None


        # Ensure any pd.NA/empty-like strings in object columns are explicitly converted to None for SQL NULL
        for col, dtype in expected_cols_dict.items():
             if dtype == 'object' and col in df_to_save.columns:
                  # Replace pandas NA, numpy nan, and empty strings with None
                  df_to_save[col] = df_to_save[col].replace({pd.NA: None, np.nan: None, '': None}).mask(df_to_save[col].isna(), None) # Handle pd.NA and other potential NaNs


        # Use if_exists='replace' to overwrite the table
        df_to_save.to_sql(table_name, conn, if_exists='replace', index=False, dtype={c:TABLE_COLUMNS[table_name][c] for c in expected_cols_dict}) # Specify dtypes to hint SQL table creation
        conn.commit() # Commit changes

    except sqlite3.Error as e:
        st.error(f"Error al guardar la tabla '{table_name}' en la base de datos: {e}")
        if conn:
             try:
                  conn.rollback()
             except Exception as rb_e:
                  st.error(f"Error durante el rollback: {rb_e}")
    except Exception as e:
         st.error(f"Error general al guardar la tabla '{table_name}': {e}")
         if conn:
              try:
                   conn.rollback()
              except Exception as rb_e:
                   st.error(f"Error durante el rollback: {rb_e}")
    finally:
        pass


# --- Load all DataFrames on startup (if not in session_state) ---
# Ensure a default empty DataFrame structure if table is missing or empty and apply datetime conversion

def load_data_into_session_state():
    """Loads all tables from DB into session_state."""
    tables_to_load = {
        'df_flotas': TABLE_FLOTAS,
        'df_equipos': TABLE_EQUIPOS,
        'df_consumo': TABLE_CONSUMO,
        'df_costos_salarial': TABLE_COSTOS_SALARIAL,
        'df_gastos_fijos': TABLE_GASTOS_FIJOS,
        'df_gastos_mantenimiento': TABLE_GASTOS_MANTENIMIENTO,
        'df_precios_combustible': TABLE_PRECIOS_COMBUSTIBLE,
        'df_proyectos': TABLE_PROYECTOS,
        'df_presupuesto_materiales': TABLE_PRESUPUESTO_MATERIALES,
        'df_compras_materiales': TABLE_COMPRAS_MATERIALES,
        'df_asignacion_materiales': TABLE_ASIGNACION_MATERIALES,
    }
    for ss_key, table_name in tables_to_load.items():
        if ss_key not in st.session_state:
            st.session_state[ss_key] = load_table(DATABASE_FILE, table_name)
            # Recalculate cost columns immediately after loading if they exist
            if table_name == TABLE_PRESUPUESTO_MATERIALES:
                st.session_state[ss_key] = calcular_costo_presupuestado(st.session_state[ss_key])
            elif table_name == TABLE_COMPRAS_MATERIALES:
                 st.session_state[ss_key] = calcular_costo_compra(st.session_state[ss_key])
            elif table_name == TABLE_ASIGNACION_MATERIALES:
                 st.session_state[ss_key] = calcular_costo_asignado(st.session_state[ss_key])

# Load data on script start
load_data_into_session_state()


# --- Helper function to calculate costs ---
# Ensure robust handling of potential NaNs and dtypes within calculation
def calcular_costo_presupuestado(df):
    """Calcula el costo total presupuestado por fila."""
    # Ensure columns exist and are numeric, defaulting to 0 for calculation if missing, invalid, or NaN
    cantidad_col = df.get('Cantidad_Presupuestada', pd.Series(0.0, index=df.index))
    precio_unitario_col = df.get('Precio_Unitario_Presupuestado', pd.Series(0.0, index=df.index))

    cantidad = pd.to_numeric(cantidad_col, errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(precio_unitario_col, errors='coerce').fillna(0.0)

    df['Costo_Presupuestado'] = cantidad * precio_unitario
    return df

def calcular_costo_compra(df):
    """Calcula el costo total de compra por fila."""
    cantidad_col = df.get('Cantidad_Comprada', pd.Series(0.0, index=df.index))
    precio_unitario_col = df.get('Precio_Unitario_Comprado', pd.Series(0.0, index=df.index))

    cantidad = pd.to_numeric(cantidad_col, errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(precio_unitario_col, errors='coerce').fillna(0.0)

    df['Costo_Compra'] = cantidad * precio_unitario
    return df

def calcular_costo_asignado(df):
    """Calcula el costo total asignado por fila."""
    cantidad_col = df.get('Cantidad_Asignada', pd.Series(0.0, index=df.index))
    precio_unitario_col = df.get('Precio_Unitario_Asignado', pd.Series(0.0, index=df.index))

    cantidad = pd.to_numeric(cantidad_col, errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(precio_unitario_col, errors='coerce').fillna(0.0)

    df['Costo_Asignado'] = cantidad * precio_unitario
    return df


# Re-calculate costs for loaded data just in case, although load_data_into_session_state does it
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
            # Compare against stripped string values to avoid issues with trailing spaces
            elif nombre_flota in st.session_state.df_flotas['Nombre_Flota'].astype(str).str.strip().tolist():
                 st.warning(f"La flota '{nombre_flota}' ya existe.")
            else:
                # Generate simple unique ID (using timestamp + counter for robustness within session)
                current_ids = set(st.session_state.df_flotas['ID_Flota'].astype(str).tolist())
                base_id = f"FLOTA_{int(time.time() * 1e9)}" # Higher resolution timestamp
                id_flota = base_id
                counter = 0
                while id_flota in current_ids:
                    counter += 1
                    id_flota = f"{base_id}_{counter}"

                new_flota_data = {'ID_Flota': id_flota, 'Nombre_Flota': nombre_flota}
                new_flota_df = pd.DataFrame([new_flota_data])
                # Use concat which handles potential type differences better than append/update
                st.session_state.df_flotas = pd.concat([st.session_state.df_flotas, new_flota_df], ignore_index=True)

                save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS) # Save to DB
                st.success(f"Flota '{nombre_flota}' añadida con ID: {id_flota}.")
                st.experimental_rerun() # Rerun to update the list and selectboxes on other pages


    st.subheader("Lista de Flotas")
    if st.session_state.df_flotas.empty:
         st.info("No hay flotas registradas aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar flotas. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
        df_flotas_editable = st.session_state.df_flotas.copy()

        df_flotas_edited = st.data_editor(
            df_flotas_editable,
            key="data_editor_flotas",
            num_rows="dynamic", # Allow adding/deleting rows
            column_config={
                 "ID_Flota": st.column_config.TextColumn("ID Flota", disabled=True), # Prevent editing ID
                 "Nombre_Flota": st.column_config.TextColumn("Nombre Flota", required=True),
            },
             # Use column_state to keep track of initial values for comparison, not needed with replace=True
             # column_state='preserved' # Keep state during edits, potentially reduces unnecessary saves? Test needed.
        )

        # Logic to save changes from data_editor
        # Use deep comparison including index and order if exact match is needed,
        # otherwise checking length and column values (after sorting/reindexing if needed) is sufficient for 'replace'.
        # Simple comparison is often enough given the 'replace' strategy.
        # Ensure comparison dataframe has consistent column order and index.
        df_flotas_edited_compare = df_flotas_edited.sort_values(by=list(df_flotas_edited.columns)).reset_index(drop=True)
        df_flotas_original_compare = st.session_state.df_flotas.sort_values(by=list(st.session_state.df_flotas.columns)).reset_index(drop=True)

        if not df_flotas_edited_compare.equals(df_flotas_original_compare):
             # st.session_state.df_flotas is updated only when save button is clicked now for clarity
             # st.session_state.df_flotas = df_flotas_edited.copy() # Removed immediate update

             if st.button("Guardar Cambios en Lista de Flotas", key="save_flotas_button"):
                  # Validate before saving
                  df_to_save = df_flotas_edited.copy()
                  # Remove rows that are essentially empty from data editor adds
                  df_to_save = df_to_save.dropna(subset=['Nombre_Flota'], how='all').copy() # Drop rows where Nombre_Flota is completely null
                  # Further filter for rows where Nombre_Flota is empty string after strip
                  df_to_save = df_to_save[df_to_save['Nombre_Flota'].astype(str).str.strip() != '']


                  if df_to_save['Nombre_Flota'].isnull().any() or (df_to_save['Nombre_Flota'].astype(str).str.strip() == '').any():
                      st.error("Error: Los nombres de las flotas no pueden estar vacíos.")
                  elif df_to_save['Nombre_Flota'].duplicated().any():
                       st.error("Error: Hay nombres de flotas duplicados en la lista. Por favor, corrija los duplicados antes de guardar.")
                  else:
                      # Add ID for any potentially new rows added directly in the editor without an ID
                      new_row_mask = df_to_save['ID_Flota'].isnull() | (df_to_save['ID_Flota'].astype(str).str.strip() == '')
                      if new_row_mask.any():
                           existing_ids = set(st.session_state.df_flotas['ID_Flota'].astype(str).tolist()) # Use IDs from original state
                           new_ids = []
                           # Generate IDs only for rows that need one
                           for i in range(new_row_mask.sum()):
                                base_id = f"FLOTA_EDIT_{int(time.time() * 1e9)}_{i}"
                                unique_id = base_id
                                counter = 0
                                while unique_id in existing_ids or unique_id in new_ids: # Check against existing *and* newly generated IDs
                                    counter += 1
                                    unique_id = f"{base_id}_{counter}"
                                new_ids.append(unique_id)

                           df_to_save.loc[new_row_mask, 'ID_Flota'] = new_ids

                       st.session_state.df_flotas = df_to_save # Update session state ONLY upon successful save
                       save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS) # Save to DB
                       st.success("Cambios en la lista de flotas guardados.")
                       st.experimental_rerun() # Rerun to update selectboxes on other pages
             else:
                 st.info("Hay cambios sin guardar en la lista de flotas.") # Feedback al usuario


def page_equipos():
    st.title("Gestión de Equipos de Mina")
    st.write("Aquí puedes añadir, editar y eliminar equipos.")

    # Get list of available fleets for the selectbox
    flotas_disponibles = st.session_state.df_flotas[['ID_Flota', 'Nombre_Flota']].copy()
    flotas_disponibles['ID_Flota_str'] = flotas_disponibles['ID_Flota'].astype(str) # Ensure string for lookup
    # Create mapping from string ID_Flota to Nombre_Flota + "(ID: ...)" format for display
    flota_id_to_display_label = {
         row['ID_Flota_str']: f"{row['Nombre_Flota']} (ID: {row['ID_Flota']})"
         for index, row in flotas_disponibles.iterrows() if pd.notna(row['ID_Flota']) and pd.notna(row['Nombre_Flota'])
    }
    # Add "Sin Flota" options for various null/empty representations
    null_flota_label = "Sin Flota"
    flota_id_to_display_label[str(pd.NA)] = null_flota_label # Use string 'NA' as the key for pandas NA
    flota_id_to_display_label['nan'] = null_flota_label # Handle numpy NaN converted to string
    flota_id_to_display_label['None'] = null_flota_label # Handle Python None converted to string
    flota_id_to_display_label[''] = null_flota_label # Handle empty string


    # Create selectbox options: first is "Sin Flota", then others based on display labels
    # Need to map labels back to original IDs
    flota_options_list = [(null_flota_label, pd.NA)] + \
                          sorted([(flota_id_to_display_label[str(row['ID_Flota'])], row['ID_Flota'])
                                  for index, row in flotas_disponibles.iterrows() if pd.notna(row['ID_Flota']) and pd.notna(row['Nombre_Flota'])]) # Sort for consistency

    flota_option_labels = [item[0] for item in flota_options_list]
    flota_label_to_id = dict(flota_options_list) # Map label back to ID (including pd.NA)


    if not flota_option_labels:
        st.warning("No hay flotas registradas. Por favor, añada flotas primero.")
        # Default to an option for no fleet if list is empty
        flota_option_labels = ["Sin Flota"]
        flota_label_to_id = {"Sin Flota": pd.NA}


    st.subheader("Añadir Nuevo Equipo")
    with st.form("form_add_equipo", clear_on_submit=True):
        interno = st.text_input("Interno del Equipo").strip()
        patente = st.text_input("Patente").strip()

        selected_flota_label = st.selectbox(
            "Seleccionar Flota:",
            options=flota_option_labels,
            index=0, # Default to "Sin Flota"
            key="add_equipo_flota_select"
        )

        # Get the corresponding ID_Flota from the selected label using the map
        selected_flota_id = flota_label_to_id.get(selected_flota_label, pd.NA)


        submitted = st.form_submit_button("Añadir Equipo")
        if submitted:
            if not interno or not patente:
                st.warning("Por favor, complete Interno y Patente.")
            # Check for duplicate 'Interno' case-insensitively after stripping
            elif interno.lower() in st.session_state.df_equipos['Interno'].astype(str).str.strip().str.lower().tolist():
                st.warning(f"Ya existe un equipo con Interno '{interno}' (ignorando mayúsculas/minúsculas).")
            else:
                new_equipo_data = {'Interno': interno, 'Patente': patente, 'ID_Flota': selected_flota_id}
                new_equipo_df = pd.DataFrame([new_equipo_data])

                # Concat new row, pandas handles type inference, ideally matching session state df types
                st.session_state.df_equipos = pd.concat([st.session_state.df_equipos, new_equipo_df], ignore_index=True)

                save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS) # Save to DB
                flota_name_display = flota_id_to_display_label.get(str(selected_flota_id), "Sin Flota") # Use the map for display
                st.success(f"Equipo {interno} ({patente}) añadido a flota '{flota_name_display}'.")
                st.experimental_rerun() # Rerun to update editor view and selectboxes


    st.subheader("Lista de Equipos")
    if st.session_state.df_equipos.empty:
        st.info("No hay equipos registrados aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar equipos. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
        # Usar data_editor para permitir edición directa, incluyendo la Flota
        df_equipos_editable = st.session_state.df_equipos.copy()

        # Prepare options and format_func for the Fleet SelectboxColumn in data_editor
        flota_ids_for_editor = st.session_state.df_flotas['ID_Flota'].unique().tolist()
        # Ensure unique, non-NA IDs + include pd.NA for "Sin Flota" option value
        flota_editor_options_values = [pd.NA] + [id for id in flota_ids_for_editor if pd.notna(id)]
        flota_editor_options_values = list(dict.fromkeys(flota_editor_options_values)) # Get unique while preserving order

        # Rebuild the robust flota_id_to_name mapping for the format_func used by the editor
        flota_id_to_name_editor = {str(row['ID_Flota']): row['Nombre_Flota'] for index, row in st.session_state.df_flotas.iterrows() if pd.notna(row['ID_Flota'])}
        flota_id_to_name_editor[str(pd.NA)] = "Sin Flota"
        flota_id_to_name_editor['nan'] = "Sin Flota" # Handle numpy NaN converted to string
        flota_id_to_name_editor['None'] = "Sin Flota" # Handle Python None converted to string
        flota_id_to_name_editor[''] = "Sin Flota" # Handle empty strings

        # Safer format_func that handles various potential inputs gracefully
        def format_flota_for_editor_robust(id_value):
            try:
                if pd.isna(id_value) or str(id_value).strip() == '': # Treat various NA forms and empty string as "Sin Flota"
                     return "Sin Flota"
                # Ensure key is string for dictionary lookup
                id_str = str(id_value)
                # Return mapped name, or fallback with the ID value if not found
                return flota_id_to_name_editor.get(id_str, f"ID Desconocido ({id_str})")
            except Exception as e:
                 # Fallback if even string conversion fails (unlikely but safe)
                 # st.error(f"DEBUG: Formatting error for ID '{id_value}' ({type(id_value)}): {e}") # Debug line if needed
                 return f"Error ({id_value})"


        df_equipos_edited = st.data_editor(
            df_equipos_editable,
            key="data_editor_equipos",
            num_rows="dynamic", # Allow adding/deleting rows
            column_config={
                 "Interno": st.column_config.TextColumn("Interno", required=True),
                 "Patente": st.column_config.TextColumn("Patente", required=True),
                 "ID_Flota": st.column_config.SelectboxColumn(
                     "Flota",
                     options=flota_editor_options_values, # Provide the list of valid IDs + pd.NA
                     required=False, # Fleet assignment is optional
                     # Use the robust mapping for display in the dropdown
                     format_func=format_flota_for_editor_robust
                 )
            }
        )

        # Logic to save changes from data_editor
        # Ensure comparison dataframe has consistent column order and index.
        df_equipos_edited_compare = df_equipos_edited.sort_values(by=list(df_equipos_edited.columns)).reset_index(drop=True)
        df_equipos_original_compare = st.session_state.df_equipos.sort_values(by=list(st.session_state.df_equipos.columns)).reset_index(drop=True)


        if not df_equipos_edited_compare.equals(df_equipos_original_compare):
             # st.session_state.df_equipos = df_equipos_edited.copy() # Removed immediate update

             if st.button("Guardar Cambios en Lista de Equipos", key="save_equipos_button"):
                  # Validate before saving
                  df_to_save = df_equipos_edited.copy()
                   # Remove rows that are essentially empty from data editor adds
                  df_to_save = df_to_save.dropna(subset=['Interno', 'Patente'], how='all').copy()
                   # Further filter for empty strings after strip
                  df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save['Patente'].astype(str).str.strip() != '')]

                  if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any() or df_to_save['Patente'].isnull().any() or (df_to_save['Patente'].astype(str).str.strip() == '').any():
                       st.error("Error: Los campos 'Interno' y 'Patente' no pueden estar vacíos.")
                   # Check for duplicate 'Interno' case-insensitively after stripping on the cleaned data
                  elif df_to_save['Interno'].astype(str).str.strip().str.lower().duplicated().any():
                       st.error("Error: Hay Internos de Equipo duplicados (ignorando mayúsculas/minúsculas) en la lista. Por favor, corrija los duplicados antes de guardar.")
                  # No need to validate ID_Flota against existing fleets if using SelectboxColumn,
                  # as the options list guarantees they are valid IDs or pd.NA.

                  else: # All validations pass
                       # Ensure ID_Flota is consistently None/pd.NA if "Sin Flota" was selected or if originally missing
                       # .replace({np.nan: pd.NA, '': pd.NA, None: pd.NA}) might help clean up variations
                       df_to_save['ID_Flota'] = df_to_save['ID_Flota'].replace({np.nan: pd.NA, '': pd.NA, None: pd.NA})

                       st.session_state.df_equipos = df_to_save # Update session state ONLY on successful save
                       save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS) # Save the cleaned and validated DF
                       st.success("Cambios en la lista de equipos guardados.")
                       st.experimental_rerun() # Rerun to update dependent views


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
        # Use unique key and the cleaned list of internos
        interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="sal_int_add")
        fecha = st.date_input("Fecha", value=datetime.date.today(), key="sal_fecha_add")
        # Use default value 0.0 and step for clarity in number inputs
        consumo_litros = st.number_input("Consumo en Litros de Combustible", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="con_litros_add")
        horas_trabajadas = st.number_input("Cantidad de Horas Trabajadas", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="con_horas_add")
        kilometros_recorridos = st.number_input("Cantidad de Kilómetros Recorridos", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="con_km_add")


        submitted = st.form_submit_button("Registrar Consumo")
        if submitted:
            # Basic validation (ensure essential fields are not empty)
            if not interno_seleccionado or not fecha:
                 st.warning("Por favor, complete Equipo y Fecha.")
            # Check if at least one of the quantities is non-zero (allowing exactly 0)
            # Use direct comparison against numeric values after ensuring they are not None/NaN from the input widgets (Streamlit does this by default)
            elif consumo_litros <= 0 and horas_trabajadas <= 0 and kilometros_recorridos <= 0:
                 st.warning("Por favor, ingrese al menos un valor de Consumo, Horas o Kilómetros mayor a cero.")
            else: # All validations pass
                 new_consumo_data = {
                    'Interno': interno_seleccionado,
                    'Fecha': fecha, # Store as datetime.date initially, save_table handles conversion
                    'Consumo_Litros': float(consumo_litros), # Explicitly cast for clarity
                    'Horas_Trabajadas': float(horas_trabajadas),
                    'Kilometros_Recorridos': float(kilometros_recorridos)
                 }
                 new_consumo_df = pd.DataFrame([new_consumo_data])

                 # Use concat
                 st.session_state.df_consumo = pd.concat([st.session_state.df_consumo, new_consumo_df], ignore_index=True)

                 save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO) # Save to DB
                 st.success("Registro de consumo añadido.")
                 st.experimental_rerun() # Rerun to update editor view


    st.subheader("Registros de Consumo Existente")
    if st.session_state.df_consumo.empty:
         st.info("No hay registros de consumo aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
        df_consumo_editable = st.session_state.df_consumo.copy()
        # Ensure date column is datetime64[ns] for data_editor compatibility
        if DATETIME_COLUMNS[TABLE_CONSUMO] in df_consumo_editable.columns:
             df_consumo_editable[DATETIME_COLUMNS[TABLE_CONSUMO]] = pd.to_datetime(df_consumo_editable[DATETIME_COLUMNS[TABLE_CONSUMO]], errors='coerce') # NaT handled by data_editor as empty date

        df_consumo_edited = st.data_editor(
             df_consumo_editable,
             key="data_editor_consumo",
             num_rows="dynamic", # Allow adding/deleting rows
             column_config={
                  DATETIME_COLUMNS[TABLE_CONSUMO]: st.column_config.DateColumn("Fecha", required=True),
                  "Interno": st.column_config.TextColumn("Interno", required=True),
                  "Consumo_Litros": st.column_config.NumberColumn("Consumo Litros", min_value=0.0, format="%.2f", required=True),
                  "Horas_Trabajadas": st.column_config.NumberColumn("Horas Trabajadas", min_value=0.0, format="%.2f", required=True),
                  "Kilometros_Recorridos": st.column_config.NumberColumn("Kilómetros Recorridos", min_value=0.0, format="%.2f", required=True),
             }
         )
        # Compare edited with current session state dataframe for changes
        # Ensure consistent column order and index for comparison
        df_consumo_edited_compare = df_consumo_edited.sort_values(by=list(df_consumo_edited.columns)).reset_index(drop=True)
        df_consumo_original_compare = st.session_state.df_consumo.sort_values(by=list(st.session_state.df_consumo.columns)).reset_index(drop=True)


        if not df_consumo_edited_compare.equals(df_consumo_original_compare):
             # st.session_state.df_consumo = df_consumo_edited.copy() # Removed immediate update

             if st.button("Guardar Cambios en Registros de Consumo", key="save_consumo_button"):
                  # Basic validation and cleaning for data from editor
                  df_to_save = df_consumo_edited.copy()
                   # Remove rows that are essentially empty from data editor adds
                  df_to_save = df_to_save.dropna(subset=['Interno', DATETIME_COLUMNS[TABLE_CONSUMO]], how='all').copy() # Drop rows where Interno AND Fecha are null
                   # Further filter for empty strings in Interno and NaT dates
                  df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_CONSUMO]].notna())]


                  if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                       st.error("Error: El campo 'Interno' no puede estar vacío.")
                  elif df_to_save[DATETIME_COLUMNS[TABLE_CONSUMO]].isnull().any(): # Check after filtering (these are NaT from data_editor/coerce)
                       st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                   # Check numeric columns aren't entirely NaN if they were marked as required
                  elif df_to_save['Consumo_Litros'].isnull().any() or df_to_save['Horas_Trabajadas'].isnull().any() or df_to_save['Kilometros_Recorridos'].isnull().any():
                       st.error("Error: Los campos numéricos no pueden estar vacíos.")
                  else: # All validations pass
                       st.session_state.df_consumo = df_to_save # Update session state ONLY on successful save
                       save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO) # Save cleaned/validated DF
                       st.success("Cambios en registros de consumo guardados.")
                       st.experimental_rerun()


             else:
                 st.info("Hay cambios sin guardar en registros de consumo.")


def page_costos_equipos():
    st.title("Registro de Costos por Equipo")
    st.write("Aquí puedes registrar costos salariales, fijos y de mantenimiento por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].unique().tolist()
    internos_disponibles = [i for i in internos_disponibles if pd.notna(i) and str(i).strip() != ''] # Clean the list

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero para registrar costos.")
        return

    tab1, tab2, tab3 = st.tabs(["Costos Salariales", "Gastos Fijos", "Gastos Mantenimiento"])

    # Using consistent keys for selectboxes across tabs referencing the same list
    sal_int_key = "cost_sal_int"
    fij_int_key = "cost_fij_int"
    mant_int_key = "cost_mant_int"
    # Manage selectbox value persistently across tabs
    selected_interno = st.session_state.get(sal_int_key, internos_disponibles[0] if internos_disponibles else None) # Get previous value or first available


    with tab1:
        st.subheader("Registro de Costos Salariales")
        with st.form("form_add_salarial", clear_on_submit=True):
            # Use the consistent key for selectbox
            selected_interno_sal = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key=sal_int_key)
            fecha = st.date_input("Fecha", key="sal_fecha", value=datetime.date.today())
            monto_salarial = st.number_input("Monto Salarial", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="sal_monto_add")
            submitted = st.form_submit_button("Registrar Costo Salarial")
            if submitted:
                # Update the shared selected interno state after submission if different (optional but keeps state consistent)
                st.session_state[sal_int_key] = selected_interno_sal

                if not selected_interno_sal or not fecha or monto_salarial <= 0:
                    st.warning("Por favor, complete Equipo, Fecha y Monto (mayor a cero).")
                else:
                    new_costo_data = {
                       'Interno': selected_interno_sal,
                       'Fecha': fecha, # Store as datetime.date, save_table handles
                       'Monto_Salarial': float(monto_salarial)
                    }
                    new_costo_df = pd.DataFrame([new_costo_data])
                    # Use concat
                    st.session_state.df_costos_salarial = pd.concat([st.session_state.df_costos_salarial, new_costo_df], ignore_index=True)

                    save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL) # Save to DB
                    st.success("Costo salarial registrado.")
                    st.experimental_rerun() # Rerun to update editor view

        st.subheader("Registros Salariales Existente")
        if st.session_state.df_costos_salarial.empty:
             st.info("No hay registros salariales aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
            df_salarial_editable = st.session_state.df_costos_salarial.copy()
            if DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL] in df_salarial_editable.columns:
                 df_salarial_editable[DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]] = pd.to_datetime(df_salarial_editable[DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]], errors='coerce')

            df_salarial_edited = st.data_editor(
                df_salarial_editable,
                key="data_editor_salarial",
                num_rows="dynamic",
                 column_config={
                     DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]: st.column_config.DateColumn("Fecha", required=True),
                     "Interno": st.column_config.TextColumn("Interno", required=True),
                     "Monto_Salarial": st.column_config.NumberColumn("Monto Salarial", min_value=0.0, format="%.2f", required=True),
                 }
            )
            # Compare edited with current session state dataframe for changes
            df_salarial_edited_compare = df_salarial_edited.sort_values(by=list(df_salarial_edited.columns)).reset_index(drop=True)
            df_salarial_original_compare = st.session_state.df_costos_salarial.sort_values(by=list(st.session_state.df_costos_salarial.columns)).reset_index(drop=True)


            if not df_salarial_edited_compare.equals(df_salarial_original_compare):
                 # st.session_state.df_costos_salarial = df_salarial_edited.copy() # Removed immediate update

                 if st.button("Guardar Cambios en Registros Salariales", key="save_salarial_button"):
                      # Basic validation and cleaning
                      df_to_save = df_salarial_edited.copy()
                      df_to_save = df_to_save.dropna(subset=['Interno', DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]].notna())]

                      if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Interno' no puede estar vacío.")
                      elif df_to_save[DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]].isnull().any():
                           st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                      elif df_to_save['Monto_Salarial'].isnull().any(): # Data editor required=True should handle this, but safety check
                            st.error("Error: El campo 'Monto Salarial' no puede estar vacío.")
                      else: # All validations pass
                           st.session_state.df_costos_salarial = df_to_save # Update session state ONLY on successful save
                           save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL)
                           st.success("Cambios en registros salariales guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros salariales.")


    with tab2:
        st.subheader("Registro de Gastos Fijos")
        with st.form("form_add_fijos", clear_on_submit=True):
             selected_interno_fij = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key=fij_int_key, index=internos_disponibles.index(selected_interno) if selected_interno in internos_disponibles else 0) # Sync default
             fecha = st.date_input("Fecha", key="fij_fecha", value=datetime.date.today())
             tipo_gasto = st.text_input("Tipo de Gasto Fijo", key="fij_tipo_add").strip()
             monto_gasto = st.number_input("Monto del Gasto Fijo", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="fij_monto_add")
             descripcion = st.text_area("Descripción (Opcional)", key="fij_desc_add").strip()
             submitted = st.form_submit_button("Registrar Gasto Fijo")
             if submitted:
                  st.session_state[fij_int_key] = selected_interno_fij # Update shared state

                  if not selected_interno_fij or not fecha or monto_gasto <= 0 or not tipo_gasto:
                      st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto - mayor a cero).")
                  else:
                      new_gasto_data = {
                         'Interno': selected_interno_fij,
                         'Fecha': fecha,
                         'Tipo_Gasto_Fijo': tipo_gasto,
                         'Monto_Gasto_Fijo': float(monto_gasto),
                         'Descripcion': descripcion if descripcion else None # Store empty string as None/NULL
                      }
                      new_gasto_df = pd.DataFrame([new_gasto_data])
                      # Use concat
                      st.session_state.df_gastos_fijos = pd.concat([st.session_state.df_gastos_fijos, new_gasto_df], ignore_index=True)

                      save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS)
                      st.success("Gasto fijo registrado.")
                      st.experimental_rerun()

        st.subheader("Registros de Gastos Fijos Existente")
        if st.session_state.df_gastos_fijos.empty:
             st.info("No hay registros de gastos fijos aún.")
        else:
             st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
             df_fijos_editable = st.session_state.df_gastos_fijos.copy()
             if DATETIME_COLUMNS[TABLE_GASTOS_FIJOS] in df_fijos_editable.columns:
                  df_fijos_editable[DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]] = pd.to_datetime(df_fijos_editable[DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]], errors='coerce')

             df_fijos_edited = st.data_editor(
                 df_fijos_editable,
                 key="data_editor_fijos",
                 num_rows="dynamic",
                 column_config={
                      DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]: st.column_config.DateColumn("Fecha", required=True),
                      "Interno": st.column_config.TextColumn("Interno", required=True),
                      "Tipo_Gasto_Fijo": st.column_config.TextColumn("Tipo Gasto Fijo", required=True),
                      "Monto_Gasto_Fijo": st.column_config.NumberColumn("Monto Gasto Fijo", min_value=0.0, format="%.2f", required=True),
                      "Descripcion": st.column_config.TextColumn("Descripción", required=False),
                  }
             )
             # Compare edited with current session state dataframe for changes
             df_fijos_edited_compare = df_fijos_edited.sort_values(by=list(df_fijos_edited.columns)).reset_index(drop=True)
             df_fijos_original_compare = st.session_state.df_gastos_fijos.sort_values(by=list(st.session_state.df_gastos_fijos.columns)).reset_index(drop=True)


             if not df_fijos_edited_compare.equals(df_fijos_original_compare):
                  # st.session_state.df_gastos_fijos = df_fijos_edited.copy() # Removed immediate update

                  if st.button("Guardar Cambios en Registros de Gastos Fijos", key="save_fijos_button"):
                       # Basic validation and cleaning
                       df_to_save = df_fijos_edited.copy()
                       df_to_save = df_to_save.dropna(subset=['Interno', DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], 'Tipo_Gasto_Fijo'], how='all').copy()
                       df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]].notna()) & (df_to_save['Tipo_Gasto_Fijo'].astype(str).str.strip() != '')]


                       if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                            st.error("Error: El campo 'Interno' no puede estar vacío.")
                       elif df_to_save[DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]].isnull().any():
                            st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                       elif df_to_save['Tipo_Gasto_Fijo'].isnull().any() or (df_to_save['Tipo_Gasto_Fijo'].astype(str).str.strip() == '').any():
                            st.error("Error: El campo 'Tipo Gasto Fijo' no puede estar vacío.")
                       elif df_to_save['Monto_Gasto_Fijo'].isnull().any():
                            st.error("Error: El campo 'Monto Gasto Fijo' no puede estar vacío.")
                       else:
                            # Clean Description column - replace empty strings/whitespace with None/NULL
                            if 'Descripcion' in df_to_save.columns:
                                df_to_save['Descripcion'] = df_to_save['Descripcion'].astype(str).str.strip().replace({'': None})

                            st.session_state.df_gastos_fijos = df_to_save # Update session state ONLY on successful save
                            save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS)
                            st.success("Cambios en registros de gastos fijos guardados.")
                            st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en registros de gastos fijos.")


    with tab3:
        st.subheader("Registro de Gastos de Mantenimiento")
        with st.form("form_add_mantenimiento", clear_on_submit=True):
             selected_interno_mant = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key=mant_int_key, index=internos_disponibles.index(selected_interno) if selected_interno in internos_disponibles else 0) # Sync default
             fecha = st.date_input("Fecha", key="mant_fecha", value=datetime.date.today())
             tipo_mantenimiento = st.text_input("Tipo de Mantenimiento", key="mant_tipo_add").strip()
             monto_mantenimiento = st.number_input("Monto del Mantenimiento", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="mant_monto_add")
             descripcion = st.text_area("Descripción (Opcional)", key="mant_desc_add").strip()
             submitted = st.form_submit_button("Registrar Gasto Mantenimiento")
             if submitted:
                  st.session_state[mant_int_key] = selected_interno_mant # Update shared state

                  if not selected_interno_mant or not fecha or monto_mantenimiento <= 0 or not tipo_mantenimiento:
                      st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto - mayor a cero).")
                  else:
                      new_gasto_data = {
                         'Interno': selected_interno_mant,
                         'Fecha': fecha,
                         'Tipo_Mantenimiento': tipo_mantenimiento,
                         'Monto_Mantenimiento': float(monto_mantenimiento),
                         'Descripcion': descripcion if descripcion else None # Store empty string as None/NULL
                      }
                      new_gasto_df = pd.DataFrame([new_gasto_data])
                      # Use concat
                      st.session_state.df_gastos_mantenimiento = pd.concat([st.session_state.df_gastos_mantenimiento, new_gasto_df], ignore_index=True)

                      save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO)
                      st.success("Gasto de mantenimiento registrado.")
                      st.experimental_rerun()

        st.subheader("Registros de Gastos de Mantenimiento Existente")
        if st.session_state.df_gastos_mantenimiento.empty:
            st.info("No hay registros de mantenimiento aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
            df_mantenimiento_editable = st.session_state.df_gastos_mantenimiento.copy()
            if DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO] in df_mantenimiento_editable.columns:
                 df_mantenimiento_editable[DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]] = pd.to_datetime(df_mantenimiento_editable[DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]], errors='coerce')

            df_mantenimiento_edited = st.data_editor(
                df_mantenimiento_editable,
                key="data_editor_mantenimiento",
                num_rows="dynamic",
                column_config={
                     DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]: st.column_config.DateColumn("Fecha", required=True),
                     "Interno": st.column_config.TextColumn("Interno", required=True),
                     "Tipo_Mantenimiento": st.column_config.TextColumn("Tipo Mantenimiento", required=True),
                     "Monto_Mantenimiento": st.column_config.NumberColumn("Monto Mantenimiento", min_value=0.0, format="%.2f", required=True),
                     "Descripcion": st.column_config.TextColumn("Descripción", required=False),
                 }
            )
            # Compare edited with current session state dataframe for changes
            df_mantenimiento_edited_compare = df_mantenimiento_edited.sort_values(by=list(df_mantenimiento_edited.columns)).reset_index(drop=True)
            df_mantenimiento_original_compare = st.session_state.df_gastos_mantenimiento.sort_values(by=list(st.session_state.df_gastos_mantenimiento.columns)).reset_index(drop=True)

            if not df_mantenimiento_edited_compare.equals(df_mantenimiento_original_compare):
                 # st.session_state.df_gastos_mantenimiento = df_mantenimiento_edited.copy() # Removed immediate update
                 if st.button("Guardar Cambios en Registros de Mantenimiento", key="save_mantenimiento_button"):
                      # Basic validation and cleaning
                      df_to_save = df_mantenimiento_edited.copy()
                      df_to_save = df_to_save.dropna(subset=['Interno', DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], 'Tipo_Mantenimiento'], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['Interno'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]].notna()) & (df_to_save['Tipo_Mantenimiento'].astype(str).str.strip() != '')]


                      if df_to_save['Interno'].isnull().any() or (df_to_save['Interno'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Interno' no puede estar vacío.")
                      elif df_to_save[DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]].isnull().any():
                           st.error("Error: El campo 'Fecha' no puede estar vacío o tener un formato inválido.")
                      elif df_to_save['Tipo_Mantenimiento'].isnull().any() or (df_to_save['Tipo_Mantenimiento'].astype(str).str.strip() == '').any():
                           st.error("Error: El campo 'Tipo Mantenimiento' no puede estar vacío.")
                      elif df_to_save['Monto_Mantenimiento'].isnull().any():
                           st.error("Error: El campo 'Monto Mantenimiento' no puede estar vacío.")
                      else: # All validations pass
                            # Clean Description column
                           if 'Descripcion' in df_to_save.columns:
                               df_to_save['Descripcion'] = df_to_save['Descripcion'].astype(str).str.strip().replace({'': None})

                           st.session_state.df_gastos_mantenimiento = df_to_save # Update session state ONLY on successful save
                           save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO)
                           st.success("Cambios en registros de mantenimiento guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros de mantenimiento.")


def page_reportes_mina():
    st.title("Reportes de Mina por Fecha")
    st.write("Genera reportes de consumo y costos por equipo en un rango de fechas.")

    st.subheader("Registrar Precio del Combustible")
    st.info("Edite la tabla siguiente para modificar o eliminar precios existentes. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
    with st.form("form_add_precio_combustible", clear_on_submit=True):
        fecha_precio = st.date_input("Fecha del Precio", value=datetime.date.today(), key="precio_fecha_add")
        precio_litro = st.number_input("Precio por Litro", min_value=0.01, value=0.01, step=0.01, format="%.2f", key="precio_monto_add") # Ensure min_value >= 0
        submitted = st.form_submit_button("Registrar Precio")
        if submitted:
            if not fecha_precio or precio_litro <= 0:
                st.warning("Por favor, complete la fecha y el precio (mayor a cero).")
            else:
                # Create DataFrame for new price
                new_precio_data = {'Fecha': fecha_precio, 'Precio_Litro': float(precio_litro)}
                new_precio_df = pd.DataFrame([new_precio_data])

                # Convert existing and new date to datetime for comparison
                df_precios_temp = st.session_state.df_precios_combustible.copy()
                df_precios_temp['Fecha_dt'] = pd.to_datetime(df_precios_temp.get(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]), errors='coerce')
                fecha_precio_dt = pd.to_datetime(fecha_precio, errors='coerce')


                # Remove row with the same valid date if it exists
                # Check if the new date is valid before trying to match
                if pd.notna(fecha_precio_dt):
                    # Keep rows from temp that don't have this date (handling NaT properly)
                    st.session_state.df_precios_combustible = df_precios_temp[
                        df_precios_temp['Fecha_dt'] != fecha_precio_dt # Compares NaT correctly too
                    ].drop(columns=['Fecha_dt']).copy()
                else: # New date is invalid, just keep original data
                    st.warning("Fecha de precio proporcionada no es válida. No se guardará.")
                    st.session_state.df_precios_combustible = df_precios_temp.drop(columns=['Fecha_dt']).copy() # Restore original
                    st.experimental_rerun() # Rerun to clear the form and warning
                    return # Stop submission


                # Add the new/updated valid price row
                st.session_state.df_precios_combustible = pd.concat([st.session_state.df_precios_combustible, new_precio_df], ignore_index=True)

                save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE) # Save to DB
                st.success("Precio del combustible registrado/actualizado.")
                st.experimental_rerun() # Rerun to update editor view

    st.subheader("Precios del Combustible Existente")
    if st.session_state.df_precios_combustible.empty:
        st.info("No hay precios de combustible registrados aún.")
    else:
        df_precios_editable = st.session_state.df_precios_combustible.copy()
        if DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in df_precios_editable.columns:
             # Ensure it's datetime for data_editor
             df_precios_editable[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]] = pd.to_datetime(df_precios_editable[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]], errors='coerce')

        df_precios_edited = st.data_editor(
            df_precios_editable,
            key="data_editor_precios",
            num_rows="dynamic",
            column_config={
                DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]: st.column_config.DateColumn("Fecha", required=True),
                "Precio_Litro": st.column_config.NumberColumn("Precio por Litro", min_value=0.01, format="%.2f", required=True),
            }
        )
        # Compare edited with current session state dataframe for changes
        # Ensure consistent column order and index
        df_precios_edited_compare = df_precios_edited.sort_values(by=list(df_precios_edited.columns)).reset_index(drop=True)
        df_precios_original_compare = st.session_state.df_precios_combustible.sort_values(by=list(st.session_state.df_precios_combustible.columns)).reset_index(drop=True)


        if not df_precios_edited_compare.equals(df_precios_original_compare):
             # st.session_state.df_precios_combustible = df_precios_edited.copy() # Removed immediate update

             if st.button("Guardar Cambios en Precios de Combustible", key="save_precios_button"):
                  # Optional: Validar fechas únicas si cada fecha debe tener un solo precio
                  df_to_save = df_precios_edited.copy()
                   # Clean and validate
                  df_to_save = df_to_save.dropna(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], 'Precio_Litro'], how='all').copy()
                  df_to_save = df_to_save[df_to_save[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]].notna()]


                  if df_to_save[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]].duplicated().any(): # Check duplicates based on the date column
                       st.error("Error: Hay fechas duplicadas en los precios de combustible. Cada fecha debe tener un único precio. Por favor, corrija los duplicados antes de guardar.")
                  elif df_to_save[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]].isnull().any():
                        st.error("Error: El campo 'Fecha' no puede estar vacío o tener formato inválido.")
                  elif df_to_save['Precio_Litro'].isnull().any(): # Data editor required=True should handle, but check
                        st.error("Error: El campo 'Precio por Litro' no puede estar vacío.")
                  else:
                       st.session_state.df_precios_combustible = df_to_save # Update session state ONLY on successful save
                       save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE)
                       st.success("Cambios en precios de combustible guardados.")
                       st.experimental_rerun()
             else:
                 st.info("Hay cambios sin guardar en precios de combustible.")


    st.subheader("Reporte por Rango de Fechas")
    col1, col2 = st.columns(2)

    # Recolectar todas las fechas relevantes para min/max range inputs
    all_relevant_dates = pd.concat([
        st.session_state.df_consumo.get(DATETIME_COLUMNS[TABLE_CONSUMO], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_costos_salarial.get(DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_gastos_fijos.get(DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_gastos_mantenimiento.get(DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_precios_combustible.get(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], pd.Series(dtype='datetime64[ns]')),
    ]).dropna() # Combine and drop NaT

    if not all_relevant_dates.empty:
        min_app_date = all_relevant_dates.min().date() # Convert pandas Timestamp to datetime.date
        max_app_date = all_relevant_dates.max().date()

        today = datetime.date.today()
        # Suggest a default range ending recently
        default_end = min(today, max_app_date) # Default end is today or the latest data date
        default_start = default_end - pd.Timedelta(days=30) # Default start is 30 days prior

        # Ensure defaults are within the data range
        default_start = max(default_start, min_app_date)
        default_end = max(default_end, default_start) # Ensure end is not before start

    else:
        # Fallback if no data dates exist
        today = datetime.date.today()
        min_app_date = today - pd.Timedelta(days=365 * 5) # Acknowledge long past is possible
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

        # Define date range as pandas Timestamps for robust filtering (inclusive end date)
        start_ts = pd.Timestamp(fecha_inicio).normalize() # Normalize to start of day
        end_ts = pd.Timestamp(fecha_fin).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1) # Normalize to end of day

        # Filter dataframes ensuring date columns are datetime64[ns]
        # Use a helper function for filtering to reduce repetition
        def filter_df_by_date(df_original, date_col_name, start_ts, end_ts):
             if df_original.empty or date_col_name not in df_original.columns:
                 return pd.DataFrame(columns=df_original.columns) # Return empty DF with original columns

             df_temp = df_original.copy()
             # Convert date column, invalid dates become NaT
             df_temp['Date_dt'] = pd.to_datetime(df_temp[date_col_name], errors='coerce')
             # Filter based on the datetime column
             df_filtered = df_temp[(df_temp['Date_dt'] >= start_ts) & (df_temp['Date_dt'] <= end_ts)].copy()
             # Keep only original columns (discard the temporary Date_dt column)
             df_filtered = df_filtered.drop(columns=['Date_dt'])

             return df_filtered


        df_consumo_filtered = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts, end_ts)
        df_precios_filtered = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts, end_ts)
        df_salarial_filtered = filter_df_by_date(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], start_ts, end_ts)
        df_fijos_filtered = filter_df_by_date(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], start_ts, end_ts)
        df_mantenimiento_filtered = filter_df_by_date(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], start_ts, end_ts)

        # Re-convert date columns in filtered dataframes back to datetime64[ns] explicitly if needed for merge_asof etc.
        # filter_df_by_date drops the dt column, need to add it back for calculations that require it.
        if not df_consumo_filtered.empty:
             df_consumo_filtered[DATETIME_COLUMNS[TABLE_CONSUMO]] = pd.to_datetime(df_consumo_filtered[DATETIME_COLUMNS[TABLE_CONSUMO]], errors='coerce')
        if not df_precios_filtered.empty:
            df_precios_filtered[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]] = pd.to_datetime(df_precios_filtered[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]], errors='coerce')
        # No need to reconvert dates for simple aggregation below


        if df_consumo_filtered.empty:
            st.info("No hay datos de consumo en el rango de fechas seleccionado.")
            # Initialize with expected columns even if empty
            reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Patente', 'ID_Flota', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'])

        else:
             # Calculate metrics per equipment and date in the period *before* aggregation if needed (e.g., L/H, L/KM per day)
             # Let's calculate L/H and L/KM after summing totals for an average over the period
             # Need to ensure numeric types before calculation and aggregation
             for col in ['Consumo_Litros', 'Horas_Trabajadas', 'Kilometros_Recorridos']:
                 if col in df_consumo_filtered.columns:
                     df_consumo_filtered[col] = pd.to_numeric(df_consumo_filtered[col], errors='coerce').fillna(0.0)


             # Unir con precios de combustible (usando el precio más reciente antes o en la fecha de consumo)
             # Ensure dates are sorted datetime for merge_asof
             consumo_for_merge = df_consumo_filtered.sort_values(DATETIME_COLUMNS[TABLE_CONSUMO])
             precios_for_merge = df_precios_filtered.sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])

             # Assuming price is NOT per equipment for merge_asof by date
             if not precios_for_merge.empty and DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in precios_for_merge.columns and 'Precio_Litro' in precios_for_merge.columns:
                 # Ensure unique dates in prices for merge_asof on='Fecha'
                 precios_for_merge_unique = precios_for_merge.dropna(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], 'Precio_Litro']).drop_duplicates(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]]).sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])
                 consumo_merged = pd.merge_asof(consumo_for_merge, precios_for_merge_unique, left_on=DATETIME_COLUMNS[TABLE_CONSUMO], right_on=DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], direction='backward', suffixes=('_consumo', '_precio'))
                 # Rename price column back to standard name if merge changed it
                 if 'Precio_Litro_precio' in consumo_merged.columns:
                      consumo_merged['Precio_Litro'] = consumo_merged['Precio_Litro_precio']
                      consumo_merged = consumo_merged.drop(columns=['Precio_Litro_precio'])
                 # Handle potential duplicate Fecha column from merge_asof if suffixes weren't applied/needed
                 if DATETIME_COLUMNS[TABLE_CONSUMO] + '_consumo' in consumo_merged.columns:
                     consumo_merged = consumo_merged.rename(columns={DATETIME_COLUMNS[TABLE_CONSUMO] + '_consumo': DATETIME_COLUMNS[TABLE_CONSUMO]})
                 if DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in consumo_merged.columns and DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] != DATETIME_COLUMNS[TABLE_CONSUMO]: # Drop duplicate date col if different name
                     consumo_merged = consumo_merged.drop(columns=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]])

             else: # No price data available, use original consumo_for_merge and fill price with 0
                  consumo_merged = consumo_for_merge.copy()
                  consumo_merged['Precio_Litro'] = 0.0


             # Calculate fuel cost (handle potential missing Precio_Litro from merge_asof if no prior price)
             reporte_consumo_detail = consumo_merged.copy() # Rename for clarity
             # Ensure Precio_Litro is numeric after merge and before calculation
             if 'Precio_Litro' not in reporte_consumo_detail.columns:
                  reporte_consumo_detail['Precio_Litro'] = 0.0 # Add it if somehow missing
             reporte_consumo_detail['Precio_Litro'] = pd.to_numeric(reporte_consumo_detail['Precio_Litro'], errors='coerce').fillna(0.0)


             reporte_consumo_detail['Costo_Combustible'] = reporte_consumo_detail['Consumo_Litros'].fillna(0) * reporte_consumo_detail['Precio_Litro'].fillna(0)


             # Resumen de Consumo y Costo Combustible por Equipo en el período
             if 'Interno' in reporte_consumo_detail.columns:
                 reporte_resumen_consumo = reporte_consumo_detail.groupby('Interno').agg(
                     Total_Consumo_Litros=('Consumo_Litros', 'sum'),
                     Total_Horas=('Horas_Trabajadas', 'sum'),
                     Total_Kilometros=('Kilometros_Recorridos', 'sum'),
                     Costo_Total_Combustible=('Costo_Combustible', 'sum')
                 ).reset_index()
             else:
                 st.warning("La tabla de consumo filtrada no contiene la columna 'Interno'. No se puede generar el reporte de resumen.")
                 reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Costo_Total_Combustible'])


             # Recalcular L/H y L/KM promedio *después* de sumar (avoid NaN/inf with zero checks)
             if 'Interno' in reporte_resumen_consumo.columns: # Only if aggregation was successful
                 reporte_resumen_consumo['Avg_Consumo_L_H'] = reporte_resumen_consumo.apply(
                      lambda row: 0 if pd.isna(row['Total_Horas']) or row['Total_Horas'] == 0 else (row['Total_Consumo_Litros'] if pd.notna(row['Total_Consumo_Litros']) else 0) / row['Total_Horas'], axis=1
                 )
                 reporte_resumen_consumo['Avg_Consumo_L_KM'] = reporte_resumen_consumo.apply(
                      lambda row: 0 if pd.isna(row['Total_Kilometros']) or row['Total_Kilometros'] == 0 else (row['Total_Consumo_Litros'] if pd.notna(row['Total_Consumo_Litros']) else 0) / row['Total_Kilometros'], axis=1
                 )

                 # Unir con información de equipos (Patente, ID_Flota) y luego con Flotas (Nombre_Flota)
                 if 'Interno' in st.session_state.df_equipos.columns:
                      reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_equipos[['Interno', 'Patente', 'ID_Flota']], on='Interno', how='left')
                      # Fill NaN Patente
                      reporte_resumen_consumo['Patente'] = reporte_resumen_consumo['Patente'].fillna('Sin Patente')
                      # Merge with Flotas (ensure ID_Flota is compatible string type for merge if needed)
                      st.session_state.df_flotas['ID_Flota_str'] = st.session_state.df_flotas['ID_Flota'].astype(str)
                      reporte_resumen_consumo['ID_Flota_str'] = reporte_resumen_consumo['ID_Flota'].astype(str) # Match type for merge
                      reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_flotas[['ID_Flota_str', 'Nombre_Flota']], left_on='ID_Flota_str', right_on='ID_Flota_str', how='left')
                      reporte_resumen_consumo['Nombre_Flota'] = reporte_resumen_consumo['Nombre_Flota'].fillna('Sin Flota')
                      reporte_resumen_consumo = reporte_resumen_consumo.drop(columns=['ID_Flota_str']) # Drop temp merge column
                      if 'ID_Flota' in reporte_resumen_consumo.columns:
                          reporte_resumen_consumo = reporte_resumen_consumo.drop(columns=['ID_Flota']) # Drop the original ID_Flota column which might have pd.NA

                      # Ensure consistent columns after merges before displaying
                      expected_display_cols_consumo = ['Interno', 'Patente', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible']
                      for col in expected_display_cols_consumo:
                           if col not in reporte_resumen_consumo.columns:
                               reporte_resumen_consumo[col] = pd.NA # Add missing expected column

                      st.subheader(f"Reporte Consumo y Costo Combustible ({fecha_inicio} a {fecha_fin})")
                      st.dataframe(reporte_resumen_consumo[expected_display_cols_consumo].round(2))
                 else:
                     st.warning("La tabla de equipos no contiene la columna 'Interno'. No se puede vincular la información de patente/flota al reporte de consumo.")
                     # Display without equipment details
                     expected_display_cols_consumo_basic = ['Interno', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible']
                     st.dataframe(reporte_resumen_consumo[expected_display_cols_consumo_basic].round(2))


        # --- Sumar otros costos (Salarial, Fijos, Mantenimiento) in the period ---
        # Ensure Interno and cost columns exist and are numeric for aggregation
        if 'Interno' in df_salarial_filtered.columns and 'Monto_Salarial' in df_salarial_filtered.columns:
            df_salarial_filtered['Monto_Salarial'] = pd.to_numeric(df_salarial_filtered['Monto_Salarial'], errors='coerce').fillna(0.0)
            salarial_agg = df_salarial_filtered.groupby('Interno')['Monto_Salarial'].sum().reset_index(name='Total_Salarial')
        else: salarial_agg = pd.DataFrame(columns=['Interno', 'Total_Salarial'])

        if 'Interno' in df_fijos_filtered.columns and 'Monto_Gasto_Fijo' in df_fijos_filtered.columns:
             df_fijos_filtered['Monto_Gasto_Fijo'] = pd.to_numeric(df_fijos_filtered['Monto_Gasto_Fijo'], errors='coerce').fillna(0.0)
             fijos_agg = df_fijos_filtered.groupby('Interno')['Monto_Gasto_Fijo'].sum().reset_index(name='Total_Gastos_Fijos')
        else: fijos_agg = pd.DataFrame(columns=['Interno', 'Total_Gastos_Fijos'])

        if 'Interno' in df_mantenimiento_filtered.columns and 'Monto_Mantenimiento' in df_mantenimiento_filtered.columns:
             df_mantenimiento_filtered['Monto_Mantenimiento'] = pd.to_numeric(df_mantenimiento_filtered['Monto_Mantenimiento'], errors='coerce').fillna(0.0)
             mantenimiento_agg = df_mantenimiento_filtered.groupby('Interno')['Monto_Mantenimiento'].sum().reset_index(name='Total_Gastos_Mantenimiento')
        else: mantenimiento_agg = pd.DataFrame(columns=['Interno', 'Total_Gastos_Mantenimiento'])


        # Unir todos los costos
        # Get unique Internos from all sources in the period (dropna explicitly handles various NA types)
        all_internos_series_list = [
            df_consumo_filtered.get('Interno', pd.Series(dtype='object')),
            df_salarial_filtered.get('Interno', pd.Series(dtype='object')),
            df_fijos_filtered.get('Interno', pd.Series(dtype='object')),
            df_mantenimiento_filtered.get('Interno', pd.Series(dtype='object')),
        ]
        all_internos_in_period = pd.concat(all_internos_series_list).dropna().unique().tolist() # Convert to list for checking if empty

        if not all_internos_in_period:
             st.info("No hay datos de costos (Combustible, Salarial, Fijos, Mantenimiento) en el rango de fechas seleccionado para ningún equipo.")
        else:
             # Create a base DataFrame with all unique Internos from the period
             df_all_internos = pd.DataFrame({'Interno': all_internos_in_period})
             # Ensure Interno is string type for merging
             df_all_internos['Interno'] = df_all_internos['Interno'].astype(str)


             # Start with df_all_internos and left merge cost summaries
             reporte_costo_total = df_all_internos.merge(reporte_resumen_consumo[['Interno', 'Costo_Total_Combustible']], on='Interno', how='left').fillna(0) # Use resumen_consumo here
             reporte_costo_total = reporte_costo_total.merge(salarial_agg, on='Interno', how='left').fillna(0)
             reporte_costo_total = reporte_costo_total.merge(fijos_agg, on='Interno', how='left').fillna(0)
             reporte_costo_total = reporte_costo_total.merge(mantenimiento_agg, on='Interno', how='left').fillna(0)

             # Añadir Patente y Flota (Left merge from equipo master list, ensuring compatible types)
             if 'Interno' in st.session_state.df_equipos.columns:
                 df_equipos_for_merge = st.session_state.df_equipos[['Interno', 'Patente', 'ID_Flota']].copy()
                 df_equipos_for_merge['Interno'] = df_equipos_for_merge['Interno'].astype(str)
                 df_equipos_for_merge['Patente'] = df_equipos_for_merge['Patente'].astype(str).replace({'nan': 'Sin Patente', '': 'Sin Patente', 'None': 'Sin Patente'}) # Clean patente strings

                 reporte_costo_total = reporte_costo_total.merge(df_equipos_for_merge[['Interno', 'Patente', 'ID_Flota']], on='Interno', how='left')
                 reporte_costo_total['Patente'] = reporte_costo_total['Patente'].fillna('Sin Patente') # Fill NaNs introduced by merge if no match

                 # Merge with Flotas (ensure ID_Flota is compatible string type for merge if needed)
                 if 'ID_Flota' in st.session_state.df_flotas.columns:
                     df_flotas_for_merge = st.session_state.df_flotas[['ID_Flota', 'Nombre_Flota']].copy()
                     df_flotas_for_merge['ID_Flota'] = df_flotas_for_merge['ID_Flota'].astype(str) # Convert flota ID to string

                     # Join based on the ID_Flota from the equipo data
                     reporte_costo_total = reporte_costo_total.merge(df_flotas_for_merge, on='ID_Flota', how='left')
                     reporte_costo_total['Nombre_Flota'] = reporte_costo_total['Nombre_Flota'].fillna('Sin Flota') # Fill missing fleet names

                     # Drop the raw ID_Flota column now that Nombre_Flota is merged
                     if 'ID_Flota' in reporte_costo_total.columns:
                         reporte_costo_total = reporte_costo_total.drop(columns=['ID_Flota'])

                 else:
                      reporte_costo_total['Nombre_Flota'] = 'Sin Datos de Flota'


             else:
                 st.warning("La tabla de equipos no contiene la columna 'Interno'. No se puede vincular la información de patente/flota al reporte de costo total.")
                 reporte_costo_total['Patente'] = 'Sin Datos Equipo'
                 reporte_costo_total['Nombre_Flota'] = 'Sin Datos Equipo'


             # Ensure numeric cost columns before summing total cost
             cost_cols = ['Costo_Total_Combustible', 'Total_Salarial', 'Total_Gastos_Fijos', 'Total_Gastos_Mantenimiento']
             for col in cost_cols:
                  if col in reporte_costo_total.columns:
                       reporte_costo_total[col] = pd.to_numeric(reporte_costo_total[col], errors='coerce').fillna(0.0)
                  else: # Add column filled with 0.0 if it somehow got dropped
                      reporte_costo_total[col] = 0.0

             reporte_costo_total['Costo_Total_Equipo'] = reporte_costo_total[cost_cols].sum(axis=1)

             # Ensure consistent display columns and order
             expected_display_cols_total_cost = ['Interno', 'Patente', 'Nombre_Flota'] + cost_cols + ['Costo_Total_Equipo']
             for col in expected_display_cols_total_cost:
                 if col not in reporte_costo_total.columns:
                      reporte_costo_total[col] = pd.NA # Add missing column for consistent display

             st.subheader(f"Reporte Costo Total por Equipo ({fecha_inicio} a {fecha_fin})")
             if reporte_costo_total.empty: # Should not be empty if all_internos_in_period was not empty, but check
                 st.info("No hay datos de costos (Combustible, Salarial, Fijos, Mantenimiento) en el rango de fechas seleccionado para ningún equipo.")
             else:
                 st.dataframe(reporte_costo_total[expected_display_cols_total_cost].round(2))


def page_variacion_costos_flota():
    st.title("Variación de Costos de Flota (Gráfico de Cascada)")
    st.write("Compara los costos totales de la flota entre dos períodos para visualizar la variación.")

    st.subheader("Seleccione Períodos a Comparar")
    col1, col2, col3, col4 = st.columns(4)

    # Set default dates based on available data, similar to reportes_mina
    all_relevant_dates = pd.concat([
        st.session_state.df_consumo.get(DATETIME_COLUMNS[TABLE_CONSUMO], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_costos_salarial.get(DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_gastos_fijos.get(DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_gastos_mantenimiento.get(DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], pd.Series(dtype='datetime64[ns]')),
        st.session_state.df_precios_combustible.get(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], pd.Series(dtype='datetime64[ns]')),
    ]).dropna()

    if not all_relevant_dates.empty:
        min_app_date = all_relevant_dates.min().date()
        max_app_date = all_relevant_dates.max().date()

        today = datetime.date.today()
        # Suggest recent two months relative to max data date
        default_end_p2 = min(today, max_app_date)
        default_start_p2 = default_end_p2 - pd.Timedelta(days=30)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = default_end_p1 - pd.Timedelta(days=30)

        # Ensure defaults are within bounds
        min_date_input_display = min_app_date
        max_date_input_display = max_app_date

        default_start_p1 = max(default_start_p1, min_date_input_display)
        default_end_p1 = max(default_end_p1, min_date_input_display)
        default_start_p2 = max(default_start_p2, min_date_input_display)
        default_end_p2 = max(default_end_p2, min_date_input_display)

         # Ensure period end is not before period start, within the calculated defaults
        default_end_p1 = max(default_end_p1, default_start_p1)
        default_end_p2 = max(default_end_p2, default_start_p2)


    else:
        today = datetime.date.today()
        min_date_input_display = today - pd.Timedelta(days=365 * 5)
        max_date_input_display = today
        default_start_p1 = today - pd.Timedelta(days=60)
        default_end_p1 = today - pd.Timedelta(days=31)
        default_start_p2 = today - pd.Timedelta(days=30)
        default_end_p2 = today

        # Ensure default range is valid
        default_start_p1 = max(default_start_p1, min_date_input_display)
        default_end_p1 = max(default_end_p1, min_date_input_display)
        default_start_p2 = max(default_start_p2, min_date_input_display)
        default_end_p2 = max(default_end_p2, min_date_input_display)
        default_end_p1 = max(default_end_p1, default_start_p1)
        default_end_p2 = max(default_end_p2, default_start_p2)



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
        if not (fecha_fin_p1 < fecha_inicio_p2 or fecha_fin_p2 < fecha_inicio_p1 or fecha_inicio_p1 == fecha_inicio_p2 and fecha_fin_p1 == fecha_fin_p2): # Allow same periods
             st.warning("Advertencia: Los períodos seleccionados se solapan o no están en orden cronológico.")


        # --- Calcular Costos por Período y Categoría ---
        # Helper function to aggregate costs for a given date range, handling potential missing columns robustly
        def aggregate_cost_column(df_original, date_col_name, cost_col_name, start_ts, end_ts):
            if df_original.empty or date_col_name not in df_original.columns or cost_col_name not in df_original.columns:
                 return 0.0 # Return 0 if source data or necessary columns are missing

            df_temp = df_original.copy()
            # Convert date column, invalid dates become NaT
            df_temp['Date_dt'] = pd.to_datetime(df_temp[date_col_name], errors='coerce')
             # Ensure cost column is numeric, fill non-numeric/NaN with 0.0
            df_temp[cost_col_name] = pd.to_numeric(df_temp[cost_col_name], errors='coerce').fillna(0.0)

            # Filter based on valid dates and date range
            df_filtered = df_temp[df_temp['Date_dt'].notna() & (df_temp['Date_dt'] >= start_ts) & (df_temp['Date_dt'] <= end_ts)].copy()

            return df_filtered[cost_col_name].sum()


        # Define date range as pandas Timestamps (inclusive end date)
        start_ts_p1 = pd.Timestamp(fecha_inicio_p1).normalize()
        end_ts_p1 = pd.Timestamp(fecha_fin_p1).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        start_ts_p2 = pd.Timestamp(fecha_inicio_p2).normalize()
        end_ts_p2 = pd.Timestamp(fecha_fin_p2).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


        # Costo Combustible Period 1
        # Need filtered consumption and prices first to do the merge_asof logic
        consumo_p1_filtered_dt = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts_p1, end_ts_p1)
        precios_p1_filtered_dt = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts_p1, end_ts_p1)

        costo_combustible_p1 = 0
        if not consumo_p1_filtered_dt.empty and not precios_p1_filtered_dt.empty and DATETIME_COLUMNS[TABLE_CONSUMO] in consumo_p1_filtered_dt.columns and DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in precios_p1_filtered_dt.columns and 'Consumo_Litros' in consumo_p1_filtered_dt.columns and 'Precio_Litro' in precios_p1_filtered_dt.columns:
             # Convert date columns back to datetime if needed for merge_asof after filter_df_by_date
             consumo_p1_filtered_dt[DATETIME_COLUMNS[TABLE_CONSUMO]] = pd.to_datetime(consumo_p1_filtered_dt[DATETIME_COLUMNS[TABLE_CONSUMO]], errors='coerce')
             precios_p1_filtered_dt[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]] = pd.to_datetime(precios_p1_filtered_dt[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]], errors='coerce')


             consumo_p1_sorted = consumo_p1_filtered_dt.sort_values(DATETIME_COLUMNS[TABLE_CONSUMO])
             precios_p1_sorted = precios_p1_filtered_dt.sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])

             if not consumo_p1_sorted.empty and not precios_p1_sorted.empty:
                  # Assuming price is NOT per equipment, only merge on date
                  precios_p1_sorted_unique = precios_p1_sorted.dropna(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], 'Precio_Litro']).drop_duplicates(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]]).sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])
                  if not precios_p1_sorted_unique.empty:
                       consumo_merged = pd.merge_asof(consumo_p1_sorted, precios_p1_sorted_unique, left_on=DATETIME_COLUMNS[TABLE_CONSUMO], right_on=DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], direction='backward', suffixes=('_consumo', '_precio'))
                       # Use the correct price column and calculate cost
                       price_col_after_merge = 'Precio_Litro_precio' if 'Precio_Litro_precio' in consumo_merged.columns else 'Precio_Litro'
                       if price_col_after_merge in consumo_merged.columns:
                           costo_combustible_p1 = (pd.to_numeric(consumo_merged['Consumo_Litros'], errors='coerce').fillna(0) * pd.to_numeric(consumo_merged[price_col_after_merge], errors='coerce').fillna(0)).sum()

        costo_salarial_p1 = aggregate_cost_column(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], 'Monto_Salarial', start_ts_p1, end_ts_p1)
        costo_fijos_p1 = aggregate_cost_column(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], 'Monto_Gasto_Fijo', start_ts_p1, end_ts_p1)
        costo_mantenimiento_p1 = aggregate_cost_column(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], 'Monto_Mantenimiento', start_ts_p1, end_ts_p1)

        total_costo_p1 = costo_combustible_p1 + costo_salarial_p1 + costo_fijos_p1 + costo_mantenimiento_p1


        # Costs for Period 2
        consumo_p2_filtered_dt = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts_p2, end_ts_p2)
        precios_p2_filtered_dt = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts_p2, end_ts_p2)

        costo_combustible_p2 = 0
        if not consumo_p2_filtered_dt.empty and not precios_p2_filtered_dt.empty and DATETIME_COLUMNS[TABLE_CONSUMO] in consumo_p2_filtered_dt.columns and DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE] in precios_p2_filtered_dt.columns and 'Consumo_Litros' in consumo_p2_filtered_dt.columns and 'Precio_Litro' in precios_p2_filtered_dt.columns:
             # Convert date columns back to datetime
             consumo_p2_filtered_dt[DATETIME_COLUMNS[TABLE_CONSUMO]] = pd.to_datetime(consumo_p2_filtered_dt[DATETIME_COLUMNS[TABLE_CONSUMO]], errors='coerce')
             precios_p2_filtered_dt[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]] = pd.to_datetime(precios_p2_filtered_dt[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]], errors='coerce')

             consumo_p2_sorted = consumo_p2_filtered_dt.sort_values(DATETIME_COLUMNS[TABLE_CONSUMO])
             precios_p2_sorted = precios_p2_filtered_dt.sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])

             if not consumo_p2_sorted.empty and not precios_p2_sorted.empty:
                  precios_p2_sorted_unique = precios_p2_sorted.dropna(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], 'Precio_Litro']).drop_duplicates(subset=[DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]]).sort_values(DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE])
                  if not precios_p2_sorted_unique.empty:
                       consumo_merged = pd.merge_asof(consumo_p2_sorted, precios_p2_sorted_unique, left_on=DATETIME_COLUMNS[TABLE_CONSUMO], right_on=DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], direction='backward', suffixes=('_consumo', '_precio'))
                       # Use the correct price column and calculate cost
                       price_col_after_merge = 'Precio_Litro_precio' if 'Precio_Litro_precio' in consumo_merged.columns else 'Precio_Litro'
                       if price_col_after_merge in consumo_merged.columns:
                           costo_combustible_p2 = (pd.to_numeric(consumo_merged['Consumo_Litros'], errors='coerce').fillna(0) * pd.to_numeric(consumo_merged[price_col_after_merge], errors='coerce').fillna(0)).sum()
        else: # No price data or empty, price = 0
             costo_combustible_p2 = 0


        costo_salarial_p2 = aggregate_cost_column(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], 'Monto_Salarial', start_ts_p2, end_ts_p2)
        costo_fijos_p2 = aggregate_cost_column(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], 'Monto_Gasto_Fijo', start_ts_p2, end_ts_p2)
        costo_mantenimiento_p2 = aggregate_cost_column(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], 'Monto_Mantenimiento', start_ts_p2, end_ts_p2)

        total_costo_p2 = costo_combustible_p2 + costo_salarial_p2 + costo_fijos_p2 + costo_mantenimiento_p2

        # --- Prepare Data for Waterfall Chart ---
        labels = [
            f'Total Costo<br>P1<br>({fecha_inicio_p1} a {fecha_fin_p1})'
        ]
        measures = ['absolute']
        values = [total_costo_p1]
        texts = [f"${total_costo_p1:,.2f}"]

        # Variations
        variacion_combustible = costo_combustible_p2 - costo_combustible_p1
        variacion_salarial = costo_salarial_p2 - costo_salarial_p1
        variacion_fijos = costo_fijos_p2 - costo_fijos_p1
        variacion_mantenimiento = costo_mantenimiento_p2 - costo_mantenimiento_p1
        variacion_total = total_costo_p2 - total_costo_p1

        # Collect variations to display, only add if significant non-zero change
        variation_data = []
        if abs(variacion_combustible) >= 0.01: variation_data.append({'label': 'Var. Combustible', 'value': variacion_combustible})
        if abs(variacion_salarial) >= 0.01: variation_data.append({'label': 'Var. Salarial', 'value': variacion_salarial})
        if abs(variacion_fijos) >= 0.01: variation_data.append({'label': 'Var. Fijos', 'value': variacion_fijos})
        if abs(variacion_mantenimiento) >= 0.01: variation_data.append({'label': 'Var. Mantenimiento', 'value': variacion_mantenimiento})

        # Sort variations (e.g., largest positive first, then negative)
        variation_data.sort(key=lambda x: x['value'], reverse=True)

        # Add sorted variations to plot data
        for item in variation_data:
            labels.append(item['label'])
            measures.append('relative')
            values.append(item['value'])
            texts.append(f"${item['value']:,.2f}")


        # Add total Period 2
        labels.append(f'Total Costo<br>P2<br>({fecha_inicio_p2} a {fecha_fin_p2})')
        measures.append('total')
        values.append(total_costo_p2)
        texts.append(f"${total_costo_p2:,.2f}")


        # Check if there is actual change data to plot a waterfall
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
                 increasing = {"marker":{"color":"#FF4136"}}, # Increase = Bad (typically costs increasing is bad) -> Red
                 decreasing = {"marker":{"color":"#3D9970"}}, # Decrease = Good -> Green
                 totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
             ))

             fig.update_layout(
                 title = f'Variación de Costos de Flota: {fecha_inicio_p1} a {fecha_fin_p1} vs {fecha_inicio_p2} a {fecha_fin_p2}',
                 showlegend = False,
                 yaxis_title="Monto ($)",
                 margin=dict(l=20, r=20, t=100, b=20),
                 height=600
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

        if abs(variacion_total) >= 0.01: # Show detail variations only if total changes significantly
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
            elif nombre_obra.lower() in st.session_state.df_proyectos['Nombre_Obra'].astype(str).str.strip().str.lower().tolist():
                st.warning(f"La obra '{nombre_obra}' ya existe (ignorando mayúsculas/minúsculas).")
            else:
                # Generate simple unique ID (using timestamp + counter)
                current_ids = set(st.session_state.df_proyectos['ID_Obra'].astype(str).tolist())
                base_id = f"OBRA_{int(time.time() * 1e9)}"
                id_obra = base_id
                counter = 0
                while id_obra in current_ids:
                    counter += 1
                    id_obra = f"{base_id}_{counter}"

                new_obra_data = {'ID_Obra': id_obra, 'Nombre_Obra': nombre_obra, 'Responsable': responsable}
                new_obra_df = pd.DataFrame([new_obra_data])
                # Concat new row
                st.session_state.df_proyectos = pd.concat([st.session_state.df_proyectos, new_obra_df], ignore_index=True)

                save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS) # Save to DB
                st.success(f"Obra '{nombre_obra}' creada con ID: {id_obra}")
                st.experimental_rerun() # Rerun to update the list and selectbox


    st.subheader("Lista de Obras")
    obras_disponibles_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    # Filter out any None/NaN/empty string obra IDs
    obras_disponibles_list = [id for id in obras_disponibles_list if pd.notna(id) and str(id).strip() != '']

    if not obras_disponibles_list:
        st.info("No hay obras creadas aún.")
        # Ensure session state variable for selectbox doesn't hold an invalid value
        if "select_obra_gestion" in st.session_state:
             del st.session_state["select_obra_gestion"]
        return

    st.info("Edite la tabla siguiente para modificar o eliminar obras. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
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
    # Ensure consistent column order and index for comparison
    df_proyectos_edited_compare = df_proyectos_edited.sort_values(by=list(df_proyectos_edited.columns)).reset_index(drop=True)
    df_proyectos_original_compare = st.session_state.df_proyectos.sort_values(by=list(st.session_state.df_proyectos.columns)).reset_index(drop=True)


    if not df_proyectos_edited_compare.equals(df_proyectos_original_compare):
         # st.session_state.df_proyectos = df_proyectos_edited.copy() # Removed immediate update

         if st.button("Guardar Cambios en Lista de Obras", key="save_proyectos_button"):
              # Simple validation (non-empty, ensure ID for new rows from editor)
              df_to_save = df_proyectos_edited.copy()
              # Remove rows that are essentially empty from data editor adds
              df_to_save = df_to_save.dropna(subset=['Nombre_Obra', 'Responsable'], how='all').copy()
               # Further filter for empty strings after strip
              df_to_save = df_to_save[(df_to_save['Nombre_Obra'].astype(str).str.strip() != '') & (df_to_save['Responsable'].astype(str).str.strip() != '')]

              if df_to_save['Nombre_Obra'].isnull().any() or (df_to_save['Nombre_Obra'].astype(str).str.strip() == '').any():
                  st.error("Error: Los campos 'Nombre Obra' no pueden estar vacíos.")
              elif df_to_save['Responsable'].isnull().any() or (df_to_save['Responsable'].astype(str).str.strip() == '').any():
                   st.error("Error: Los campos 'Responsable' no pueden estar vacíos.")
              else:
                   # Add ID for any potentially new rows added directly in the editor without an ID
                   new_row_mask = df_to_save['ID_Obra'].isnull() | (df_to_save['ID_Obra'].astype(str).str.strip() == '')
                   if new_row_mask.any():
                        existing_ids = set(st.session_state.df_proyectos['ID_Obra'].astype(str).tolist()) # Use IDs from original state
                        new_ids = []
                        # Generate IDs only for rows that need one
                        for i in range(new_row_mask.sum()):
                            base_id = f"OBRA_EDIT_{int(time.time() * 1e9)}_{i}"
                            unique_id = base_id
                            counter = 0
                            while unique_id in existing_ids or unique_id in new_ids:
                                counter += 1
                                unique_id = f"{base_id}_{counter}"
                            new_ids.append(unique_id)
                            existing_ids.add(unique_id)
                        df_to_save.loc[new_row_mask, 'ID_Obra'] = new_ids


                   st.session_state.df_proyectos = df_to_save # Update session state ONLY on successful save
                   save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS)
                   st.success("Cambios en la lista de obras guardados.")
                   st.experimental_rerun() # Recargar para actualizar selectbox y otros elementos dependientes
         else:
             st.info("Hay cambios sin guardar en la lista de obras.")

    # Re-filter and populate obras_disponibles_list after potential edits/deletions
    obras_disponibles_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_list = [id for id in obras_disponibles_list if pd.notna(id) and str(id).strip() != ''] # Clean the list


    st.markdown("---")
    st.subheader("Gestionar Presupuesto por Obra")

    if not obras_disponibles_list:
         st.info("No hay obras disponibles para gestionar presupuesto. Por favor, cree una obra primero.")
         # Ensure session state variable for selectbox doesn't hold an invalid value
         if "select_obra_gestion" in st.session_state:
              del st.session_state["select_obra_gestion"]
         return # Exit function if no works exist

    # Build options for the selectbox (ensure we use the *current* list of valid IDs)
    # Filter df_proyectos based on the validated list before creating options
    obra_options_gestion_filtered_df = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].isin(obras_disponibles_list)].copy()

    obra_options_gestion = obra_options_gestion_filtered_df[['ID_Obra', 'Nombre_Obra']].to_dict('records')
    obra_gestion_labels = [f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})" for o in obra_options_gestion if pd.notna(o['ID_Obra'])] # Ensure ID is valid

    if not obra_gestion_labels: # Safety check in case filter resulted in empty
         st.info("No hay obras disponibles para gestionar presupuesto.")
         if "select_obra_gestion" in st.session_state: del st.session_state["select_obra_gestion"]
         st.experimental_rerun()
         return

    # Determine the default index: try to keep previously selected obra if still valid
    default_obra_index = 0
    if "select_obra_gestion" in st.session_state and st.session_state.select_obra_gestion in obra_gestion_labels:
         default_obra_index = obra_gestion_labels.index(st.session_state.select_obra_gestion)
    elif "select_obra_gestion" in st.session_state: # Previous selection invalid, reset
         del st.session_state["select_obra_gestion"]


    selected_obra_label_gestion = st.selectbox(
        "Seleccione una Obra:",
        options=obra_gestion_labels,
        index=default_obra_index,
        key="select_obra_gestion"
    )

    # Find the corresponding ID_Obra from the selected label
    obra_seleccionada_id = None
    for o in obra_options_gestion:
        if selected_obra_label_gestion == f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})":
             obra_seleccionada_id = o['ID_Obra']
             break

    # Safety check: If selected_obra_id is None or not in the current valid list (shouldn't happen with good lists, but safety first)
    if obra_seleccionada_id is None or obra_seleccionada_id not in obras_disponibles_list:
         st.warning(f"La obra seleccionada ya no es válida. Por favor, seleccione otra.")
         st.experimental_rerun()
         return


    obra_nombre = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_seleccionada_id]['Nombre_Obra'].iloc[0]
    st.markdown(f"#### Presupuesto de Materiales para '{obra_nombre}' (ID: {obra_seleccionada_id})")

    # Filter budget for the selected work
    df_presupuesto_obra = st.session_state.df_presupuesto_materiales[
        st.session_state.df_presupuesto_materiales['ID_Obra'].astype(str) == str(obra_seleccionada_id) # Ensure string comparison
    ].copy()

    st.info("Edite la tabla siguiente para añadir, modificar o eliminar items del presupuesto de materiales de esta obra. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")

    # Display and edit existing budget (data_editor)
    # Ensure columns for editor exist and have correct basic dtypes for display
    df_presupuesto_obra_display = df_presupuesto_obra.copy()
    # Recalculate cost here so editor shows updated values if underlying quantity/price changed
    df_presupuesto_obra_display = calcular_costo_presupuestado(df_presupuesto_obra_display)

    df_presupuesto_obra_edited = st.data_editor(
        df_presupuesto_obra_display, # Display all cols including calculated cost for view
        key=f"data_editor_presupuesto_{obra_seleccionada_id}", # Unique key per obra
        num_rows="dynamic", # Allow adding/deleting rows
        column_config={
            "Material": st.column_config.TextColumn("Material", required=True),
            "Cantidad_Presupuestada": st.column_config.NumberColumn("Cantidad Presupuestada", min_value=0.0, format="%.2f", required=True),
            "Precio_Unitario_Presupuestado": st.column_config.NumberColumn("Precio Unitario Presupuestado", min_value=0.0, format="%.2f", required=True),
            "Costo_Presupuestado": st.column_config.NumberColumn("Costo Presupuestado", disabled=True, format="%.2f") # Calculated, not editable directly
        }
    )

    # Logic to save changes from the data_editor
    # data_editor returns a dataframe with the edited/added/deleted rows including only configured columns.
    # We need to re-add the 'ID_Obra' column and recalculate the cost *after* editing.
    df_presupuesto_obra_edited_processed = df_presupuesto_obra_edited.copy()
    # Ensure ID_Obra is present (it was likely dropped by data_editor if not configured with display=True/disabled=True)
    df_presupuesto_obra_edited_processed['ID_Obra'] = obra_seleccionada_id # Add the ID_Obra column back to edited rows
    # Recalculate cost based on potentially edited Qty/Price
    df_presupuesto_obra_edited_processed = calcular_costo_presupuestado(df_presupuesto_obra_edited_processed)

    # Ensure consistent columns between edited data and the subset of original data for comparison
    cols_for_comparison = list(df_presupuesto_obra_edited_processed.columns)

    # Filter original session state df for this work, re-calculate cost to match structure, select columns, sort and reset index
    df_presupuesto_obra_original_filtered = st.session_state.df_presupuesto_materiales[
        st.session_state.df_presupuesto_materiales['ID_Obra'].astype(str) == str(obra_seleccionada_id) # Ensure string comparison
    ].copy()
    df_presupuesto_obra_original_filtered = calcular_costo_presupuestado(df_presupuesto_obra_original_filtered) # Re-calculate costs on original subset
    # Ensure required columns are present in the original subset before selection/comparison
    cols_in_original = [col for col in cols_for_comparison if col in df_presupuesto_obra_original_filtered.columns]
    # If columns are missing in the original (e.g. very old data without Costo), pad with NA/0 before subsetting
    for col in cols_for_comparison:
         if col not in df_presupuesto_obra_original_filtered.columns:
              df_presupuesto_obra_original_filtered[col] = pd.NA if TABLE_COLUMNS[TABLE_PRESUPUESTO_MATERIALES].get(col) == 'object' else 0.0


    df_presupuesto_obra_original_compare = df_presupuesto_obra_original_filtered[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)
    df_presupuesto_obra_edited_compare = df_presupuesto_obra_edited_processed[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)


    if not df_presupuesto_obra_edited_compare.equals(df_presupuesto_obra_original_compare):
         # st.session_state.df_presupuesto_materiales is updated only when save button is clicked
         # st.session_state.df_presupuesto_materiales = # Removed immediate update


         if st.button(f"Guardar Cambios en Presupuesto de '{obra_nombre}'", key=f"save_presupuesto_{obra_seleccionada_id}_button"):
             # Validation before saving
             df_to_save_obra = df_presupuesto_obra_edited_processed.copy()
             # Remove rows that are essentially empty or missing required fields from data editor adds
             df_to_save_obra = df_to_save_obra.dropna(subset=['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado'], how='all').copy() # Check against key required fields
             # Also check for empty strings in 'Material'
             df_to_save_obra = df_to_save_obra[df_to_save_obra['Material'].astype(str).str.strip() != '']

             if df_to_save_obra['Material'].isnull().any() or (df_to_save_obra['Material'].astype(str).str.strip() == '').any():
                  st.error("Error: El nombre del material no puede estar vacío en el presupuesto.")
             elif df_to_save_obra['Cantidad_Presupuestada'].isnull().any() or df_to_save_obra['Precio_Unitario_Presupuestado'].isnull().any(): # Check if required numeric fields became NaN
                  st.error("Error: Los campos 'Cantidad Presupuestada' y 'Precio Unitario Presupuestado' no pueden estar vacíos.")
             else: # All validations pass

                 # Remove the old rows for this work from the main DataFrame in session state
                 df_rest_presupuesto = st.session_state.df_presupuesto_materiales[
                     st.session_state.df_presupuesto_materiales['ID_Obra'].astype(str) != str(obra_seleccionada_id) # Ensure string comparison for filtering
                 ].copy()

                 # Combine the rest of the data with the updated/edited data for this work
                 st.session_state.df_presupuesto_materiales = pd.concat([df_rest_presupuesto, df_to_save_obra], ignore_index=True)

                 save_table(st.session_state.df_presupuesto_materiales, DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES) # Save to DB
                 st.success(f"Presupuesto de la obra '{obra_nombre}' guardado.")
                 st.experimental_rerun() # Recargar para actualizar la vista del editor y el reporte
         else:
             st.info(f"Hay cambios sin guardar en el presupuesto de '{obra_nombre}'.")

    # Report inside the same work management page
    st.markdown(f"#### Reporte de Presupuesto para '{obra_nombre}'")
    # Use the potentially updated df_presupuesto_obra (based on session state *if saved*) for the report display
    df_presupuesto_obra_current = st.session_state.df_presupuesto_materiales[
         st.session_state.df_presupuesto_materiales['ID_Obra'].astype(str) == str(obra_seleccionada_id)
    ].copy()

    if df_presupuesto_obra_current.empty:
        st.info("No hay presupuesto de materiales registrado para esta obra.")
    else:
        st.subheader("Detalle del Presupuesto")
        # Ensure calculated cost column is present and updated before displaying report
        df_presupuesto_obra_with_cost = calcular_costo_presupuestado(df_presupuesto_obra_current)
        # Select display columns, ensuring they exist
        report_cols_presupuesto = ['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']
        report_cols_presupuesto_present = [col for col in report_cols_presupuesto if col in df_presupuesto_obra_with_cost.columns]

        st.dataframe(df_presupuesto_obra_with_cost[report_cols_presupuesto_present].round(2))

        total_cantidad_presupuestada = df_presupuesto_obra_with_cost['Cantidad_Presupuestada'].sum() if 'Cantidad_Presupuestada' in df_presupuesto_obra_with_cost.columns else 0.0
        total_costo_presupuestado = df_presupuesto_obra_with_cost['Costo_Presupuestado'].sum() if 'Costo_Presupuestado' in df_presupuesto_obra_with_cost.columns else 0.0

        st.subheader("Resumen del Presupuesto")
        st.write(f"**Cantidad Total Presupuestada:** {total_cantidad_presupuestada:,.2f}")
        st.write(f"**Costo Total Presupuestado:** ${total_costo_presupuestado:,.2f}")


    # Variation Report inside the same work management page
    st.markdown(f"#### Variación Materiales para '{obra_nombre}' (Presupuesto vs Asignado)")

    # Ensure cost is calculated/present in allocation data
    df_asignacion_obra_current = st.session_state.df_asignacion_materiales[
         st.session_state.df_asignacion_materiales['ID_Obra'].astype(str) == str(obra_seleccionada_id) # Ensure string comparison
    ].copy()
    df_asignacion_obra_current = calcular_costo_asignado(df_asignacion_obra_current) # Recalculate/ensure cost

    if df_presupuesto_obra_current.empty and df_asignacion_obra_current.empty:
        st.info("No hay presupuesto ni materiales asignados para esta obra.")
    else:
       # Agrupar presupuesto por material
       # Use the dataframe that includes the calculated cost
       if not df_presupuesto_obra_current.empty and 'Material' in df_presupuesto_obra_current.columns:
           presupuesto_agg = df_presupuesto_obra_current.groupby(df_presupuesto_obra_current['Material'].astype(str)).agg( # Group by string material
               Cantidad_Presupuestada=('Cantidad_Presupuestada', 'sum'),
               Costo_Presupuestado=('Costo_Presupuestado', 'sum')
           ).reset_index()
       else:
            presupuesto_agg = pd.DataFrame(columns=['Material', 'Cantidad_Presupuestada', 'Costo_Presupuestado'])


       # Agrupar asignaciones por material
       if not df_asignacion_obra_current.empty and 'Material' in df_asignacion_obra_current.columns:
           asignacion_agg = df_asignacion_obra_current.groupby(df_asignacion_obra_current['Material'].astype(str)).agg( # Group by string material
               Cantidad_Asignada=('Cantidad_Asignada', 'sum'),
               Costo_Asignado=('Costo_Asignado', 'sum')
           ).reset_index()
       else:
            asignacion_agg = pd.DataFrame(columns=['Material', 'Cantidad_Asignada', 'Costo_Asignado'])


       # Unir presupuesto y asignación
       variacion_obra = pd.merge(presupuesto_agg, asignacion_agg, on='Material', how='outer').fillna(0)

       # Calcular variaciones
       variacion_obra['Cantidad_Variacion'] = variacion_obra['Cantidad_Asignada'] - variacion_obra['Cantidad_Presupuestada']
       variacion_obra['Costo_Variacion'] = variacion_obra['Costo_Asignado'] - variacion_obra['Costo_Presupuestado']

       st.subheader("Reporte de Variación por Material")
       if variacion_obra.empty:
            st.info("No hay datos de variación de materiales para esta obra.")
       else:
           # Select display columns, ensure they exist
           report_cols_variacion = ['Material', 'Cantidad_Presupuestada', 'Cantidad_Asignada', 'Cantidad_Variacion', 'Costo_Presupuestado', 'Costo_Asignado', 'Costo_Variacion']
           report_cols_variacion_present = [col for col in report_cols_variacion if col in variacion_obra.columns]

           st.dataframe(variacion_obra[report_cols_variacion_present].round(2))

           total_costo_presupuestado_obra = variacion_obra['Costo_Presupuestado'].sum() if 'Costo_Presupuestado' in variacion_obra.columns else 0.0
           total_costo_asignado_obra = variacion_obra['Costo_Asignado'].sum() if 'Costo_Asignado' in variacion_obra.columns else 0.0
           total_variacion_costo_obra = total_costo_asignado_obra - total_costo_presupuestado_obra

           st.subheader("Resumen de Variación de Costo Total")
           st.write(f"Costo Presupuestado Total: ${total_costo_presupuestado_obra:,.2f}")
           st.write(f"Costo Asignado (Real) Total: ${total_costo_asignado_obra:,.2f}")
           st.write(f"Variación Total: ${total_variacion_costo_obra:,.2f}")

           # Optional: Waterfall chart for the work (budget vs allocated)
           if abs(total_variacion_costo_obra) >= 0.01 or total_costo_presupuestado_obra > 0 or total_costo_asignado_obra > 0:
                st.subheader("Gráfico de Variación de Costo por Obra")
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

                    labels_obra_cascada.append(f'Asignado<br>{obra_nombre}')
                    values_obra_cascada.append(total_costo_asignado_obra)
                    # Use 'total' measure for the final bar if there's a base + relative steps,
                    # or if only start/end bars, they will connect visually.
                    measures_obra_cascada.append('total')
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

    # Ensure that calculated cost column exists and data are numeric
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated
    # Ensure ID_Obra is string for grouping
    if 'ID_Obra' in df_presupuesto.columns:
        df_presupuesto['ID_Obra'] = df_presupuesto['ID_Obra'].astype(str)
    else:
         # If 'ID_Obra' is somehow missing from the DF structure, create a placeholder
         df_presupuesto['ID_Obra'] = 'ID Desconocida'


    # Group by work (ensure necessary columns exist before grouping)
    if 'ID_Obra' in df_presupuesto.columns and 'Cantidad_Presupuestada' in df_presupuesto.columns and 'Costo_Presupuestado' in df_presupuesto.columns:
        reporte_por_obra = df_presupuesto.groupby('ID_Obra').agg(
            Cantidad_Total_Presupuestada=('Cantidad_Presupuestada', 'sum'),
            Costo_Total_Presupuestado=('Costo_Presupuestado', 'sum')
        ).reset_index()

        # Join with work names (ensure ID_Obra is string in df_proyectos too for merge)
        df_proyectos_temp = st.session_state.df_proyectos.copy()
        if 'ID_Obra' in df_proyectos_temp.columns:
            df_proyectos_temp['ID_Obra'] = df_proyectos_temp['ID_Obra'].astype(str)
            reporte_por_obra = reporte_por_obra.merge(df_proyectos_temp[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')
            reporte_por_obra['Nombre_Obra'] = reporte_por_obra['Nombre_Obra'].fillna(reporte_por_obra['ID_Obra'] + ' (Desconocida)') # Use ID if name missing
        else: # If 'ID_Obra' is missing in proyectos, still use the one from presupuesto
             reporte_por_obra['Nombre_Obra'] = reporte_por_obra['ID_Obra'] + ' (Sin Datos de Obra)'


        st.subheader("Presupuesto Total por Obra")
        if reporte_por_obra.empty:
             st.info("No hay presupuesto total calculado (posiblemente datos de presupuesto no válidos o missing ID_Obra).")
        else:
             # Ensure display columns exist
             display_cols = ['Nombre_Obra', 'ID_Obra', 'Cantidad_Total_Presupuestada', 'Costo_Total_Presupuestado']
             display_cols_present = [col for col in display_cols if col in reporte_por_obra.columns]
             st.dataframe(reporte_por_obra[display_cols_present].round(2))

             # Total general
             cantidad_gran_total = reporte_por_obra['Cantidad_Total_Presupuestada'].sum() if 'Cantidad_Total_Presupuestada' in reporte_por_obra.columns else 0.0
             costo_gran_total = reporte_por_obra['Costo_Total_Presupuestado'].sum() if 'Costo_Total_Presupuestado' in reporte_por_obra.columns else 0.0

             st.subheader("Gran Total Presupuestado (Todas las Obras)")
             st.write(f"**Cantidad Gran Total Presupuestada:** {cantidad_gran_total:,.2f}")
             st.write(f"**Costo Gran Total Presupuestado:** ${costo_gran_total:,.2f}")

    else:
        st.warning("Los datos de presupuesto no contienen las columnas necesarias ('ID_Obra', 'Cantidad_Presupuestada', 'Costo_Presupuestado') para generar el reporte. Verifique la tabla 'presupuesto_materiales'.")


def page_compras_asignacion():
    st.title("Gestión de Compras y Asignación de Materiales")
    st.write("Registra las compras y asigna materiales a las obras.")

    st.subheader("Registrar Compra de Materiales")
    with st.form("form_add_compra", clear_on_submit=True):
        fecha_compra = st.date_input("Fecha de Compra", value=datetime.date.today(), key="compra_fecha_add")
        material_compra = st.text_input("Nombre del Material Comprado", key="compra_material_add").strip()
        cantidad_comprada = st.number_input("Cantidad Comprada", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="compra_cantidad_add")
        precio_unitario_comprado = st.number_input("Precio Unitario de Compra", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="compra_precio_add")
        submitted = st.form_submit_button("Registrar Compra")
        if submitted:
            if not fecha_compra or not material_compra or cantidad_comprada < 0 or precio_unitario_comprado < 0:
                st.warning("Por favor, complete la fecha, material, cantidad (>=0) y precio (>=0).")
            elif cantidad_comprada == 0 and precio_unitario_comprado == 0:
                st.warning("La cantidad y el precio unitario comprados no pueden ser ambos cero si desea registrar una compra significativa.")
            else:
                 # Generar ID único (timestamp + counter)
                current_ids = set(st.session_state.df_compras_materiales['ID_Compra'].astype(str).tolist())
                base_id = f"COMPRA_{int(time.time() * 1e9)}"
                id_compra = base_id
                counter = 0
                while id_compra in current_ids:
                    counter += 1
                    id_compra = f"{base_id}_{counter}"


                new_compra_data = {
                    'ID_Compra': id_compra,
                    'Fecha_Compra': fecha_compra, # Store as datetime.date, save_table handles
                    'Material': material_compra,
                    'Cantidad_Comprada': float(cantidad_comprada),
                    'Precio_Unitario_Comprado': float(precio_unitario_comprado)
                }
                new_compra_df = pd.DataFrame([new_compra_data])

                # Calculate cost for the new row immediately
                new_compra_df = calcular_costo_compra(new_compra_df)

                # Concat new row
                st.session_state.df_compras_materiales = pd.concat([st.session_state.df_compras_materiales, new_compra_df], ignore_index=True)

                save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES)
                st.success(f"Compra de '{material_compra}' registrada con ID: {id_compra}")
                st.experimental_rerun() # Rerun to update the history view and assignment options


    st.subheader("Historial de Compras")
    if st.session_state.df_compras_materiales.empty:
        st.info("No hay compras registradas aún.")
    else:
         st.info("Edite la tabla siguiente para modificar o eliminar compras. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
         # Use data_editor to allow editing
         df_compras_editable = st.session_state.df_compras_materiales.copy()
         # Ensure date is datetime64[ns] for data_editor
         if DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES] in df_compras_editable.columns:
              df_compras_editable[DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]] = pd.to_datetime(df_compras_editable[DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]], errors='coerce')

         # Ensure Costo_Compra is recalculated for display if Qty/Price were edited previously without saving
         df_compras_editable = calcular_costo_compra(df_compras_editable)

         # Select columns to display/edit
         display_cols_compras = ['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']
         # Ensure all display columns exist in the dataframe
         display_cols_compras_present = [col for col in display_cols_compras if col in df_compras_editable.columns]


         df_compras_edited = st.data_editor(
             df_compras_editable[display_cols_compras_present],
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
         # Logic for saving changes from data_editor
         # Data editor returns a copy of only the displayed columns.
         # Need to add back any missing original columns, handle potential new rows (no ID) and recalculate cost.
         df_compras_edited_processed = df_compras_edited.copy()

         # Ensure all expected columns are present (especially those not displayed/editable like Costo_Compra)
         for col, dtype in TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].items():
             if col not in df_compras_edited_processed.columns:
                  if dtype == 'object':
                       df_compras_edited_processed[col] = pd.NA
                  elif 'float' in dtype or 'int' in dtype:
                       df_compras_edited_processed[col] = 0.0

         # Recalculate Costo_Compra based on edited Qty/Price *after* ensuring columns exist
         df_compras_edited_processed = calcular_costo_compra(df_compras_edited_processed)


         # Handle new rows added via the editor - generate IDs if missing
         new_row_mask = df_compras_edited_processed['ID_Compra'].isnull() | (df_compras_edited_processed['ID_Compra'].astype(str).str.strip() == '')
         if new_row_mask.any():
              existing_ids = set(st.session_state.df_compras_materiales['ID_Compra'].astype(str).tolist())
              new_ids = []
              for i in range(new_row_mask.sum()):
                   base_id = f"COMPRA_EDIT_{int(time.time() * 1e9)}_{i}"
                   unique_id = base_id
                   counter = 0
                   while unique_id in existing_ids or unique_id in new_ids: # Check against existing *and* newly generated
                       counter += 1
                       unique_id = f"{base_id}_{counter}"
                   new_ids.append(unique_id)
                   existing_ids.add(unique_id) # Add to set for subsequent checks
              df_compras_edited_processed.loc[new_row_mask, 'ID_Compra'] = new_ids


         # Compare the processed edited dataframe with the original session state dataframe
         # Use consistent column order and index for comparison
         cols_for_comparison = list(TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].keys()) # Use all expected columns for robust comparison

         # Ensure original dataframe has all expected columns and correct dtypes/defaults before comparison
         df_compras_original_compare = st.session_state.df_compras_materiales.copy()
         for col, dtype in TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].items():
              if col not in df_compras_original_compare.columns:
                   df_compras_original_compare[col] = pd.NA if dtype == 'object' else 0.0 # Add missing columns to original for comparison


         # Sort and reset index for comparison
         df_compras_original_compare = df_compras_original_compare[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)
         df_compras_edited_compare = df_compras_edited_processed[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)

         if not df_compras_edited_compare.equals(df_compras_original_compare):

              # st.session_state.df_compras_materiales = df_compras_edited_processed.copy() # Removed immediate update

              if st.button("Guardar Cambios en Historial de Compras", key="save_compras_button"):
                 # Validar before saving
                 df_to_save = df_compras_edited_processed.copy()
                 # Remove rows that are essentially empty or missing required fields
                 df_to_save = df_to_save.dropna(subset=['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado'], how='all').copy()
                  # Further validation for empty strings and NaT dates
                 df_to_save = df_to_save[(df_to_save['ID_Compra'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]].notna()) & (df_to_save['Material'].astype(str).str.strip() != '')]


                 if df_to_save.empty and not df_compras_edited_processed.empty:
                      # This happens if *all* rows from editor failed validation
                      st.error("Error: Ninguna fila editada/añadida es válida. Asegúrese de completar los campos obligatorios.")
                 elif df_to_save['Fecha_Compra'].isnull().any():
                      st.error("Error: El campo 'Fecha Compra' no puede estar vacío o tener formato inválido.")
                 elif df_to_save['Material'].isnull().any() or (df_to_save['Material'].astype(str).str.strip() == '').any():
                      st.error("Error: El campo 'Material' no puede estar vacío.")
                 elif df_to_save['Cantidad_Comprada'].isnull().any() or df_to_save['Precio_Unitario_Comprado'].isnull().any():
                      st.error("Error: Los campos numéricos obligatorios no pueden estar vacíos.")
                 # No need to check ID_Compra null/empty here as generation should have covered new rows

                 else: # All validations pass
                      st.session_state.df_compras_materiales = df_to_save # Update session state ONLY on successful save
                      save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES) # Save the validated DF
                      st.success("Cambios en historial de compras guardados.")
                      st.experimental_rerun()
              else:
                 st.info("Hay cambios sin guardar en el historial de compras.")


    st.markdown("---")

    st.subheader("Asignar Materiales a Obra")
    obras_disponibles_assign_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_assign_list = [id for id in obras_disponibles_assign_list if pd.notna(id) and str(id).strip() != ''] # Clean the list


    if not obras_disponibles_assign_list:
        st.warning("No hay obras creadas. No se pueden asignar materiales.")
        # Ensure state variable doesn't hold invalid value
        if "asig_obra" in st.session_state: del st.session_state["asig_obra"]
        return

    # Build options for the obra selectbox
    # Filter df_proyectos based on the validated list before creating options
    obra_options_assign_filtered_df = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].isin(obras_disponibles_assign_list)].copy()
    obra_options_assign = obra_options_assign_filtered_df[['ID_Obra', 'Nombre_Obra']].to_dict('records')
    obra_assign_labels = [f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})" for o in obra_options_assign if pd.notna(o['ID_Obra'])] # Ensure ID is valid

    if not obra_assign_labels: # Safety check in case filter resulted in empty
        st.warning("No hay obras disponibles para gestionar presupuesto.")
        if "asig_obra" in st.session_state: del st.session_state["asig_obra"]
        st.experimental_rerun()
        return

    # Determine default index, trying to keep previous selection
    default_obra_assign_index = 0
    if "asig_obra" in st.session_state and st.session_state.asig_obra in obra_assign_labels:
         default_obra_assign_index = obra_assign_labels.index(st.session_state.asig_obra)
    elif "asig_obra" in st.session_state:
         del st.session_state["asig_obra"]


    # List materials from purchases for convenience (optional, still allow free text)
    materiales_comprados_unicos = st.session_state.df_compras_materiales['Material'].unique().tolist()
    # Filter out potential None/NaN/empty string materials and ensure string type
    materiales_comprados_unicos = [str(m) for m in materiales_comprados_unicos if pd.notna(m) and str(m).strip() != '']
    materiales_comprados_unicos.sort() # Sort alphabetically


    with st.form("form_asignar_material", clear_on_submit=True):
        fecha_asignacion = st.date_input("Fecha de Asignación", value=datetime.date.today(), key="asig_fecha")

        # Selectbox for Obra
        selected_obra_label_assign = st.selectbox(
            "Seleccione Obra de Destino:",
            options=obra_assign_labels,
            index=default_obra_assign_index,
            key="asig_obra"
        )

        # Get the corresponding ID_Obra from the selected label
        obra_destino_id = None
        for o in obra_options_assign:
            if selected_obra_label_assign == f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})":
                 obra_destino_id = o['ID_Obra']
                 break


        # Allow selecting from purchased materials or typing manually
        material_input_method = st.radio("¿Cómo seleccionar material?", ["Seleccionar de compras", "Escribir manualmente"], key="material_input_method_radio")

        material_asignado = None
        suggested_price = 0.0

        if material_input_method == "Seleccionar de compras":
             if materiales_comprados_unicos:
                  # Ensure selected value is valid
                  material_asignado = st.selectbox("Material a Asignar:", [""] + materiales_comprados_unicos, key="asig_material_select") # Add empty string as default

                  # Optionally get price suggestion from last purchase *if* a material is selected
                  if material_asignado and material_asignado != '':
                      last_purchase = st.session_state.df_compras_materiales[
                           st.session_state.df_compras_materiales['Material'].astype(str).str.strip().str.lower() == material_asignado.lower().strip()
                       ].sort_values(DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES], ascending=False) # Get most recent purchase
                      # Ensure Price column exists and is numeric before taking .iloc
                      if not last_purchase.empty and 'Precio_Unitario_Comprado' in last_purchase.columns:
                          last_purchase['Precio_Unitario_Comprado'] = pd.to_numeric(last_purchase['Precio_Unitario_Comprado'], errors='coerce')
                          suggested_price = last_purchase['Precio_Unitario_Comprado'].iloc[0] if pd.notna(last_purchase['Precio_Unitario_Comprado'].iloc[0]) else 0.0
                      else:
                           suggested_price = 0.0

             else:
                  st.info("No hay materiales registrados en compras. Use 'Escribir manualmente'.")
                  # Fallback to manual input
                  material_input_method = "Escribir manualmente"
                  # If user sees this message, the radio might be "Select from purchases" but the selectbox is empty/greyed.
                  # Setting radio back forces the manual input text box to appear.
                  st.session_state.material_input_method_radio = "Escribir manualmente"
                  st.experimental_rerun() # Trigger re-run to show the manual text input


        # This will run in the next re-run if forced, or if initially selected.
        if material_input_method == "Escribir manualmente":
             material_asignado = st.text_input("Nombre del Material a Asignar", key="asig_material_manual").strip()
             # No suggested price for manual input (it defaults to 0.0 set earlier)


        cantidad_asignada = st.number_input("Cantidad a Asignar", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="asig_cantidad")
        # Precio al que se ASIGNA (costo real)
        # Use the suggested price from last purchase if selecting, otherwise default 0.0
        # Need to manage state for this input carefully so the suggestion applies initially but user can edit.
        # Use the `value` argument dynamically
        precio_unitario_asignado = st.number_input(
            "Precio Unitario Asignado (Costo Real)",
            min_value=0.0, format="%.2f", key="asig_precio",
            value=st.session_state.get('current_asig_price_suggestion', suggested_price)
        )
        # Update state variable whenever suggestion changes (or maybe clear it when input method changes?)
        st.session_state['current_asig_price_suggestion'] = suggested_price # Store suggestion


        submitted = st.form_submit_button("Asignar Material")
        if submitted:
             # After submission, clear the price suggestion so it doesn't pre-fill the *next* form
             if 'current_asig_price_suggestion' in st.session_state:
                 del st.session_state['current_asig_price_suggestion']

             # Basic validation
             if obra_destino_id is None or str(obra_destino_id).strip() == '':
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
                  # Generar ID único (timestamp + counter)
                  current_ids = set(st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str).tolist())
                  base_id = f"ASIG_{int(time.time() * 1e9)}"
                  id_asignacion = base_id
                  counter = 0
                  while id_asignacion in current_ids:
                      counter += 1
                      id_asignacion = f"{base_id}_{counter}"

                  new_asignacion_data = {
                      'ID_Asignacion': id_asignacion,
                      'Fecha_Asignacion': fecha_asignacion, # Store as datetime.date
                      'ID_Obra': obra_destino_id, # This is the raw ID (int or string or NA), save_table handles
                      'Material': material_asignado,
                      'Cantidad_Asignada': float(cantidad_asignada),
                      'Precio_Unitario_Asignado': float(precio_unitario_asignado)
                  }
                  new_asignacion_df = pd.DataFrame([new_asignacion_data])

                  # Calculate cost for the new row
                  new_asignacion_df = calcular_costo_asignado(new_asignacion_df)

                  # Concat new row
                  st.session_state.df_asignacion_materiales = pd.concat([st.session_state.df_asignacion_materiales, new_asignacion_df], ignore_index=True)

                  save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)

                  # Find obra name for success message
                  obra_name_for_success = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str) == str(obra_destino_id)]['Nombre_Obra'].iloc[0] if str(obra_destino_id) in st.session_state.df_proyectos['ID_Obra'].astype(str).tolist() else "ID Desconocida"

                  st.success(f"Material '{material_asignado}' ({cantidad_asignada:.2f} unidades) asignado a obra '{obra_name_for_success}'.")
                  st.experimental_rerun()


    st.subheader("Historial de Asignaciones")
    if st.session_state.df_asignacion_materiales.empty:
        st.info("No hay materiales asignados aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar asignaciones. Use el ícono de papelera al pasar el mouse sobre cada fila para eliminarla.")
        # Use data_editor to allow editing
        df_asignaciones_editable = st.session_state.df_asignacion_materiales.copy()
        # Ensure date is datetime64[ns] for data_editor
        if DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES] in df_asignaciones_editable.columns:
             df_asignaciones_editable[DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]] = pd.to_datetime(df_asignaciones_editable[DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]], errors='coerce')

        # Ensure Costo_Asignado is recalculated for display if Qty/Price were edited previously without saving
        df_asignaciones_editable = calcular_costo_asignado(df_asignaciones_editable)

        # Options for ID_Obra in data_editor (should match current valid obra IDs)
        # Use the list filtered from df_proyectos earlier
        obra_ids_for_editor = obras_disponibles_assign_list # This list contains valid IDs (string/object compatible)

        if not obra_ids_for_editor:
             st.warning("No hay obras válidas disponibles para editar asignaciones.")
             # Show non-editable table if no valid work IDs exist for selectbox
             display_cols_asig = ['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']
             display_cols_asig_present = [col for col in display_cols_asig if col in df_asignaciones_editable.columns]
             st.dataframe(df_asignaciones_editable[display_cols_asig_present])
             # Do not proceed to data_editor if options list for SelectboxColumn is empty
        else: # Proceed with data_editor only if obra IDs are available for the selectbox
            df_asignaciones_edited = st.data_editor(
                df_asignaciones_editable,
                key="data_editor_asignaciones",
                num_rows="dynamic",
                 column_config={
                     "ID_Asignacion": st.column_config.TextColumn("ID Asignación", disabled=True),
                     DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]: st.column_config.DateColumn("Fecha Asignación", required=True),
                     "ID_Obra": st.column_config.SelectboxColumn("ID Obra", options=obra_ids_for_editor, required=True), # Use Selectbox for existing valid work IDs
                     "Material": st.column_config.TextColumn("Material", required=True),
                     "Cantidad_Asignada": st.column_config.NumberColumn("Cantidad Asignada", min_value=0.0, format="%.2f", required=True),
                     "Precio_Unitario_Asignado": st.column_config.NumberColumn("Precio Unitario Asignado", min_value=0.0, format="%.2f", required=True),
                     "Costo_Asignado": st.column_config.NumberColumn("Costo Asignado", disabled=True, format="%.2f") # Calculated
                 }
             )
             # Logic for saving changes from data_editor
             df_asignaciones_edited_processed = df_asignaciones_edited.copy()

             # Ensure all expected columns are present and numeric ones have default 0
             for col, dtype in TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].items():
                 if col not in df_asignaciones_edited_processed.columns:
                      if dtype == 'object': df_asignaciones_edited_processed[col] = pd.NA
                      elif 'float' in dtype or 'int' in dtype: df_asignaciones_edited_processed[col] = 0.0

             # Recalculate Costo_Asignado based on edited Qty/Price
             df_asignaciones_edited_processed = calcular_costo_asignado(df_asignaciones_edited_processed)

             # Handle new rows added via the editor - generate IDs if missing
             new_row_mask = df_asignaciones_edited_processed['ID_Asignacion'].isnull() | (df_asignaciones_edited_processed['ID_Asignacion'].astype(str).str.strip() == '')
             if new_row_mask.any():
                  existing_ids = set(st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str).tolist())
                  new_ids = []
                  for i in range(new_row_mask.sum()):
                       base_id = f"ASIG_EDIT_{int(time.time() * 1e9)}_{i}"
                       unique_id = base_id
                       counter = 0
                       while unique_id in existing_ids or unique_id in new_ids:
                           counter += 1
                           unique_id = f"{base_id}_{counter}"
                       new_ids.append(unique_id)
                       existing_ids.add(unique_id) # Add to set for subsequent checks
                  df_asignaciones_edited_processed.loc[new_row_mask, 'ID_Asignacion'] = new_ids


             # Compare edited with current session state dataframe
             cols_for_comparison = list(TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].keys())

             # Ensure original dataframe has all expected columns and correct dtypes/defaults
             df_asignaciones_original_compare = st.session_state.df_asignacion_materiales.copy()
             for col, dtype in TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].items():
                 if col not in df_asignaciones_original_compare.columns:
                      df_asignaciones_original_compare[col] = pd.NA if dtype == 'object' else 0.0

             # Sort and reset index for comparison
             df_asignaciones_original_compare = df_asignaciones_original_compare[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)
             df_asignaciones_edited_compare = df_asignaciones_edited_processed[cols_for_comparison].sort_values(by=cols_for_comparison).reset_index(drop=True)


             if not df_asignaciones_edited_compare.equals(df_asignaciones_original_compare):

                  # st.session_state.df_asignacion_materiales = df_asignaciones_edited_processed.copy() # Removed immediate update

                  if st.button("Guardar Cambios en Historial de Asignaciones", key="save_asignaciones_button"):
                      # Validar before saving
                      df_to_save = df_asignaciones_edited_processed.dropna(subset=['ID_Asignacion', DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES], 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado'], how='all').copy()
                      df_to_save = df_to_save[(df_to_save['ID_Asignacion'].astype(str).str.strip() != '') & (df_to_save[DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]].notna()) & (df_to_save['ID_Obra'].astype(str).str.strip() != '') & (df_to_save['Material'].astype(str).str.strip() != '')]

                      if df_to_save.empty and not df_asignaciones_edited_processed.empty:
                           st.error("Error: Ninguna fila editada/añadida es válida. Asegúrese de completar los campos obligatorios.")
                      elif df_to_save[DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]].isnull().any():
                           st.error("Error: El campo 'Fecha Asignación' no puede estar vacío o tener formato inválido.")
                      elif df_to_save['ID_Obra'].isnull().any() or (df_to_save['ID_Obra'].astype(str).str.strip() == ''):
                           st.error("Error: El campo 'ID Obra' no puede estar vacío.") # SelectboxColumn requires this
                      # Validation that ID_Obra exists is handled by the Selectbox options
                      elif df_to_save['Material'].isnull().any() or (df_to_save['Material'].astype(str).str.strip() == ''):
                           st.error("Error: El campo 'Material' no puede estar vacío.")
                      elif df_to_save['Cantidad_Asignada'].isnull().any() or df_to_save['Precio_Unitario_Asignado'].isnull().any():
                           st.error("Error: Los campos numéricos obligatorios no pueden estar vacíos.")

                      else: # All validations pass
                           st.session_state.df_asignacion_materiales = df_to_save # Update session state ONLY on successful save
                           save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)
                           st.success("Cambios en historial de asignaciones guardados.")
                           st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en el historial de asignaciones.")


        # --- Deshacer Asignación Section ---
        st.markdown("---")
        st.subheader("Eliminar Asignación por ID")

        # Get available assignment IDs for selectbox
        asignaciones_disponibles_list = st.session_state.df_asignacion_materiales['ID_Asignacion'].unique().tolist()
        asignaciones_disponibles_list = [id for id in asignaciones_disponibles_list if pd.notna(id) and str(id).strip() != ''] # Clean the list


        if not asignaciones_disponibles_list:
            st.info("No hay asignaciones para eliminar por ID.")
        else:
            # Fetch brief info for the selectbox display
            # Ensure necessary columns exist before selecting them
            info_cols = ['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada']
            info_cols_present = [col for col in info_cols if col in st.session_state.df_asignacion_materiales.columns]

            df_asig_info = st.session_state.df_asignacion_materiales[info_cols_present].copy()

            # Format date column to string
            if 'Fecha_Asignacion' in df_asig_info.columns:
                df_asig_info['Fecha_Asignacion_str'] = pd.to_datetime(df_asig_info['Fecha_Asignacion'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('Fecha Inválida')
            else:
                df_asig_info['Fecha_Asignacion_str'] = 'Fecha No Disp.'

            # Ensure other info columns are strings and handle missing/NaN/None
            for col in ['ID_Obra', 'Material']:
                 if col in df_asig_info.columns:
                      df_asig_info[col] = df_asig_info[col].astype(str).replace({None: 'N/A', np.nan: 'N/A', '': 'N/A'}) # Handle various null forms and empty strings
                 else:
                      df_asig_info[col] = 'No Disp.'

            # Handle Quantity - ensure numeric, fillna, round
            if 'Cantidad_Asignada' in df_asig_info.columns:
                 df_asig_info['Cantidad_Asignada'] = pd.to_numeric(df_asig_info['Cantidad_Asignada'], errors='coerce').fillna(0.0).round(2)
                 df_asig_info['Cantidad_Asignada_str'] = df_asig_info['Cantidad_Asignada'].astype(str)
            else:
                 df_asig_info['Cantidad_Asignada_str'] = 'No Disp.'


            # Create a dictionary map from ID to the info dictionary
            # Filter df_asig_info to only include rows with valid IDs before setting index
            df_asig_info_valid_ids = df_asig_info[df_asig_info['ID_Asignacion'].isin(asignaciones_disponibles_list)].copy()
            if not df_asig_info_valid_ids.empty and 'ID_Asignacion' in df_asig_info_valid_ids.columns:
                 # Select only the columns needed for the display string
                 asig_options_dict = df_asig_info_valid_ids[['ID_Asignacion', 'Fecha_Asignacion_str', 'ID_Obra', 'Material', 'Cantidad_Asignada_str']].set_index('ID_Asignacion').to_dict('index')
            else:
                 asig_options_dict = {} # Empty if no valid assignments


            # Create format func that safely accesses dict elements
            def format_assignment_option_display(asig_id):
                info = asig_options_dict.get(asig_id, {}) # Get info dictionary, default to empty if ID not found
                fecha_str = info.get('Fecha_Asignacion_str', 'Fecha No Disp.')
                obra_id = info.get('ID_Obra', 'Obra No Disp.')
                material = info.get('Material', 'Material No Disp.')
                cantidad = info.get('Cantidad_Asignada_str', 'Cant. No Disp.')
                # Join non-missing info parts, including ID
                parts = [asig_id]
                if fecha_str != 'Fecha No Disp.': parts.append(fecha_str)
                if obra_id != 'Obra No Disp.': parts.append(obra_id)
                if material != 'Material No Disp.': parts.append(material)
                if cantidad != 'Cant. No Disp.': parts.append(cantidad)

                # More structured display string
                info_str_parts = []
                if fecha_str and fecha_str != 'Fecha Inválida': info_str_parts.append(fecha_str)
                if obra_id and obra_id != 'N/A': info_str_parts.append(obra_id)
                if material and material != 'N/A': info_str_parts.append(material)
                if cantidad and cantidad != '0.0': info_str_parts.append(f"{cantidad}")

                if info_str_parts:
                    return f"{asig_id} ({' - '.join(info_str_parts)})"
                else:
                     return f"{asig_id} (Detalles No Disponibles)"


            id_asignacion_eliminar = st.selectbox(
                "Seleccione ID de Asignación a eliminar:",
                options=asignaciones_disponibles_list, # Only provide valid IDs from current DF
                format_func=format_assignment_option_display,
                key="eliminar_asig_select"
            )

            if st.button(f"Eliminar Asignación Seleccionada ({id_asignacion_eliminar})", key="eliminar_asig_button"):
                # Ensure ID_Asignacion column is treated as string for comparison to the selectbox string value
                st.session_state.df_asignacion_materiales = st.session_state.df_asignacion_materiales[
                    st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str) != str(id_asignacion_eliminar) # Use string comparison
                ].copy() # Use .copy() to avoid SettingWithCopyWarning later

                save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES) # Save to DB
                st.success(f"Asignación {id_asignacion_eliminar} eliminada.")
                st.experimental_rerun()


def page_reporte_variacion_total_obras():
    st.title("Reporte de Variación Total Obras (Presupuesto vs Real)")
    st.write("Compara el costo total presupuestado vs el costo total real (asignado) para cada obra.")

    if st.session_state.df_presupuesto_materiales.empty and st.session_state.df_asignacion_materiales.empty:
        st.info("No hay datos de presupuesto ni de asignación para generar el reporte.")
        return

    # Calculate total budgeted costs per work
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto) # Ensure cost is calculated/updated
    # Ensure ID_Obra is string for grouping/merging
    if 'ID_Obra' in df_presupuesto.columns:
        df_presupuesto['ID_Obra'] = df_presupuesto['ID_Obra'].astype(str).fillna('ID Desconocida').replace('', 'ID Desconocida')
    else:
         df_presupuesto['ID_Obra'] = 'ID Desconocida'

    if not df_presupuesto.empty and 'ID_Obra' in df_presupuesto.columns and 'Cantidad_Presupuestada' in df_presupuesto.columns and 'Costo_Presupuestado' in df_presupuesto.columns:
        presupuesto_total_obra = df_presupuesto.groupby('ID_Obra').agg(
            Cantidad_Presupuestada_Total=('Cantidad_Presupuestada', 'sum'),
            Costo_Presupuestado_Total=('Costo_Presupuestado', 'sum')
        ).reset_index()
    else:
         presupuesto_total_obra = pd.DataFrame(columns=['ID_Obra', 'Cantidad_Presupuestada_Total', 'Costo_Presupuestado_Total'])


    # Calculate total allocated costs per work
    df_asignacion = st.session_state.df_asignacion_materiales.copy()
    df_asignacion = calcular_costo_asignado(df_asignacion) # Ensure cost is calculated/updated
    # Ensure ID_Obra is string for grouping/merging
    if 'ID_Obra' in df_asignacion.columns:
         df_asignacion['ID_Obra'] = df_asignacion['ID_Obra'].astype(str).fillna('ID Desconocida').replace('', 'ID Desconocida')
    else:
         df_asignacion['ID_Obra'] = 'ID Desconocida'

    if not df_asignacion.empty and 'ID_Obra' in df_asignacion.columns and 'Cantidad_Asignada' in df_asignacion.columns and 'Costo_Asignado' in df_asignacion.columns:
         asignacion_total_obra = df_asignacion.groupby('ID_Obra').agg(
            Cantidad_Asignada_Total=('Cantidad_Asignada', 'sum'),
            Costo_Asignado_Total=('Costo_Asignado', 'sum')
        ).reset_index()
    else:
         asignacion_total_obra = pd.DataFrame(columns=['ID_Obra', 'Cantidad_Asignada_Total', 'Costo_Asignado_Total'])


    # Merge cost and quantity DataFrames using outer join to include all works from both sides, fill NaNs with 0
    reporte_variacion_obras = pd.merge(presupuesto_total_obra, asignacion_total_obra, on='ID_Obra', how='outer').fillna(0)


    # Join with work names (ensure ID_Obra is string in df_proyectos too for merge)
    df_proyectos_temp = st.session_state.df_proyectos.copy()
    if 'ID_Obra' in df_proyectos_temp.columns:
         df_proyectos_temp['ID_Obra'] = df_proyectos_temp['ID_Obra'].astype(str)
         reporte_variacion_obras = reporte_variacion_obras.merge(df_proyectos_temp[['ID_Obra', 'Nombre_Obra']], on='ID_Obra', how='left')
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras['Nombre_Obra'].fillna(reporte_variacion_obras['ID_Obra'] + ' (Desconocida)')
    else:
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras['ID_Obra'] + ' (Sin Datos de Obra)'


    # Calculate variation (ensure numeric columns exist after merge/fillna)
    cost_cols = ['Costo_Presupuestado_Total', 'Costo_Asignado_Total']
    qty_cols = ['Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total']
    for col in cost_cols + qty_cols:
         if col not in reporte_variacion_obras.columns:
             reporte_variacion_obras[col] = 0.0 # Add missing aggregated cols as 0

    reporte_variacion_obras['Variacion_Total_Costo'] = reporte_variacion_obras['Costo_Asignado_Total'] - reporte_variacion_obras['Costo_Presupuestado_Total']
    reporte_variacion_obras['Variacion_Total_Cantidad'] = reporte_variacion_obras['Cantidad_Asignada_Total'] - reporte_variacion_obras['Cantidad_Presupuestada_Total']

    # Sort for better presentation
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

        # --- Total Waterfall Chart (Cost) ---
        total_presupuestado_general = reporte_variacion_obras['Costo_Presupuestado_Total'].sum() if 'Costo_Presupuestado_Total' in reporte_variacion_obras.columns else 0.0
        total_asignado_general = reporte_variacion_obras['Costo_Asignado_Total'].sum() if 'Costo_Asignado_Total' in reporte_variacion_obras.columns else 0.0
        total_variacion_general_costo = total_asignado_general - total_presupuestado_general


        if abs(total_variacion_general_costo) >= 0.01 or total_presupuestado_general > 0 or total_asignado_general > 0:
            st.subheader("Gráfico de Cascada: Presupuesto Total vs Costo Real Total")

            # Prepare data for the Cost waterfall
            labels_costo = ['Total Presupuestado']
            values_costo = [total_presupuestado_general]
            measures_costo = ['absolute']
            texts_costo = [f"${total_presupuestado_general:,.2f}"]

            # Add variations per work (cost only, only if significant variation)
            if 'Variacion_Total_Costo' in reporte_variacion_obras.columns:
                reporte_variacion_obras_significant_cost_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Costo']) >= 0.01].sort_values('Variacion_Total_Costo', ascending=False).copy()

                for index, row in reporte_variacion_obras_significant_cost_var.iterrows():
                     obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else row['ID_Obra'] + ' (Desconocida)'
                     if len(obra_label) > 20:
                          obra_label = obra_label[:17] + '...'
                     labels_costo.append(f"Var: {obra_label}")

                     values_costo.append(row['Variacion_Total_Costo'])
                     measures_costo.append('relative')
                     texts_costo.append(f"${row['Variacion_Total_Costo']:,.2f}")

            # Add the total allocated
            labels_costo.append('Total Asignado')
            values_costo.append(total_asignado_general)
            measures_costo.append('total')
            texts_costo.append(f"${total_asignado_general:,.2f}")

            # Check again if there's more than just the start and end points OR if start != end
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
                    increasing = {"marker":{"color":"#FF4136"}},
                    decreasing = {"marker":{"color":"#3D9970"}},
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
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
            else:
                 st.info("El costo total presupuestado es igual al costo total asignado o la variación total es insignificante. No hay variación de costo para mostrar en el gráfico.")

        else:
             st.info("No hay costo presupuestado ni asignado total para mostrar el gráfico de variación de costo.")

        # --- Total Waterfall Chart (Quantity) ---
        total_cantidad_presupuestada_general = reporte_variacion_obras['Cantidad_Presupuestada_Total'].sum() if 'Cantidad_Presupuestada_Total' in reporte_variacion_obras.columns else 0.0
        total_cantidad_asignada_general = reporte_variacion_obras['Cantidad_Asignada_Total'].sum() if 'Cantidad_Asignada_Total' in reporte_variacion_obras.columns else 0.0
        total_variacion_general_cantidad = total_cantidad_asignada_general - total_cantidad_presupuestada_general

        if abs(total_variacion_general_cantidad) >= 0.01 or total_cantidad_presupuestada_general > 0 or total_cantidad_asignada_general > 0:
            st.subheader("Gráfico de Cascada: Cantidad Total Presupuestada vs Cantidad Real Total")

            # Prepare data for the Quantity waterfall
            labels_cantidad = ['Total Presupuestado (Cant.)']
            values_cantidad = [total_cantidad_presupuestada_general]
            measures_cantidad = ['absolute']
            texts_cantidad = [f"{total_cantidad_presupuestada_general:,.2f}"]

            # Add variations per work (quantity only, only if significant variation)
            if 'Variacion_Total_Cantidad' in reporte_variacion_obras.columns:
                reporte_variacion_obras_significant_qty_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Cantidad']) >= 0.01].sort_values('Variacion_Total_Cantidad', ascending=False).copy()

                for index, row in reporte_variacion_obras_significant_qty_var.iterrows():
                     obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else row['ID_Obra'] + ' (Desconocida)'
                     if len(obra_label) > 20:
                          obra_label = obra_label[:17] + '...'
                     labels_cantidad.append(f"Var Cant: {obra_label}")

                     values_cantidad.append(row['Variacion_Total_Cantidad'])
                     measures_cantidad.append('relative')
                     texts_cantidad.append(f"{row['Variacion_Total_Cantidad']:,.2f}")

            # Add the total allocated
            labels_cantidad.append('Total Asignado (Cant.)')
            values_cantidad.append(total_cantidad_asignada_general)
            measures_cantidad.append('total')
            texts_cantidad.append(f"{total_cantidad_asignada_general:,.2f}")

            # Check again after adding variation bars
            if len(labels_cantidad) > 2 or (len(labels_cantidad) == 2 and abs(values_cantidad[0] - values_cantidad[1]) >= 0.01):
                fig_total_variacion_cantidad = go.Figure(go.Waterfall(
                    name = "Variación Total Cantidad",
                    orientation = "v",
                    measure = measures_cantidad,
                    x = labels_cantidad,
                    textposition = "outside",
                    text = texts_cantidad,
                    y = values_cantidad,
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                    increasing = {"marker":{"color":"#FF4136"}},
                    decreasing = {"marker":{"color":"#3D9970"}},
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
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
            else:
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
    total_equipos = len(st.session_state.df_equipos.dropna(subset=['Interno']).copy()) # Count based on non-empty Interno
    total_obras = len(st.session_state.df_proyectos.dropna(subset=['ID_Obra']).copy()) # Count based on non-empty ID_Obra
    total_flotas = len(st.session_state.df_flotas.dropna(subset=['ID_Flota']).copy()) # Count based on non-empty ID_Flota

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

