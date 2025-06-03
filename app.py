import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import sqlite3
import time
import numpy as np
import datetime

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
TABLE_PRECIOS_COMBUSTIBLE = "precios_combustible" # No ID for this table, keyed by Date
TABLE_PROYECTOS = "proyectos"
TABLE_PRESUPUESTO_MATERIALES = "presupuesto_materiales"
TABLE_COMPRAS_MATERIALES = "compras_materiales"
TABLE_ASIGNACION_MATERIALES = "asignacion_materiales"


# --- Define expected columns and their intended pandas dtypes for each table ---
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

DATETIME_COLUMNS = {
    TABLE_CONSUMO: 'Fecha',
    TABLE_COSTOS_SALARIAL: 'Fecha',
    TABLE_GASTOS_FIJOS: 'Fecha',
    TABLE_GASTOS_MANTENIMIENTO: 'Fecha',
    TABLE_PRECIOS_COMBUSTIBLE: 'Fecha',
    TABLE_COMPRAS_MATERIALES: 'Fecha_Compra',
    TABLE_ASIGNACION_MATERIALES: 'Fecha_Asignacion',
}

PANDAS_INT_DTYPE = pd.Int64Dtype() if hasattr(pd, 'Int64Dtype') else 'float64'

@st.cache_resource
def get_db_conn():
    conn = sqlite3.connect(DATABASE_FILE, check_same_thread=False, timeout=10)
    conn.execute('PRAGMA journal_mode=WAL')
    return conn

def load_table(db_file, table_name):
    conn = get_db_conn()
    expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
    expected_cols = list(expected_cols_dict.keys())
    df = pd.DataFrame()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE;", (table_name,))
        table_exists = cursor.fetchone() is not None
        if table_exists:
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        else:
             st.warning(f"La tabla '{table_name}' no existe. Creando DataFrame vacío.")
    except pd.io.sql.DatabaseError as e:
        st.error(f"Error DB al leer '{table_name}': {e}")
    except Exception as e:
         st.error(f"Error al cargar '{table_name}': {e}")

    df = df.reindex(columns=expected_cols)
    for col, dtype in expected_cols_dict.items():
        if col in df.columns:
             try:
                  if dtype == 'object':
                       df[col] = df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                       df[col] = df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA}).mask(df[col].isna(), pd.NA)
                  elif 'float' in dtype:
                       df[col] = pd.to_numeric(df[col], errors='coerce').astype(float).fillna(0.0)
                  elif 'int' in dtype:
                       if hasattr(pd, 'Int64Dtype'):
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                       else:
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float).fillna(0.0)
             except Exception:
                  if 'float' in dtype or 'int' in dtype:
                       df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    if table_name in DATETIME_COLUMNS:
         date_col = DATETIME_COLUMNS[table_name]
         if date_col in df.columns:
              df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

def save_table(df, db_file, table_name):
    conn = get_db_conn()
    try:
        df_to_save = df.copy()
        expected_cols_dict = TABLE_COLUMNS.get(table_name, {})
        df_to_save = df_to_save.reindex(columns=list(expected_cols_dict.keys()))
        for col, dtype in expected_cols_dict.items():
             if 'float' in dtype or 'int' in dtype:
                  if col in df_to_save.columns:
                       df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0.0)
             if dtype == PANDAS_INT_DTYPE:
                 if col in df_to_save.columns:
                     df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0).astype(int)
        if table_name in DATETIME_COLUMNS:
            date_col = DATETIME_COLUMNS[table_name]
            if date_col in df_to_save.columns:
                 df_to_save.loc[:, date_col] = pd.to_datetime(df_to_save[date_col], errors='coerce').dt.strftime('%Y-%m-%d').replace({np.nan: None, pd.NA: None, None: None})
        for col, dtype in expected_cols_dict.items():
             if dtype == 'object' and col in df_to_save.columns:
                  df_to_save.loc[:, col] = df_to_save[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None, str(pd.NA): None}).mask(df_to_save[col].isna(), None)
        sqlite_dtypes = {col: 'TEXT' for col in expected_cols_dict.keys()}
        for col, dtype in expected_cols_dict.items():
             if 'float' in dtype: sqlite_dtypes[col] = 'REAL'
             elif 'int' in dtype or dtype == PANDAS_INT_DTYPE: sqlite_dtypes[col] = 'INTEGER'
        df_to_save.to_sql(table_name, conn, if_exists='replace', index=False, dtype=sqlite_dtypes)
        conn.commit()
    except sqlite3.Error as e:
        st.error(f"Error SQLite al guardar '{table_name}': {e}")
        if conn: conn.rollback()
    except Exception as e:
         st.error(f"Error al guardar '{table_name}': {e}")
         if conn: conn.rollback()

def calcular_costo_presupuestado(df):
    df_calc = df.copy()
    cantidad = pd.to_numeric(df_calc.get('Cantidad_Presupuestada', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(df_calc.get('Precio_Unitario_Presupuestado', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    if 'Costo_Presupuestado' not in df_calc.columns:
         df_calc['Costo_Presupuestado'] = 0.0
    df_calc.loc[:, 'Costo_Presupuestado'] = cantidad * precio_unitario
    df_calc['Costo_Presupuestado'] = df_calc['Costo_Presupuestado'].astype(float)
    return df_calc

def calcular_costo_compra(df):
    df_calc = df.copy()
    cantidad = pd.to_numeric(df_calc.get('Cantidad_Comprada', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(df_calc.get('Precio_Unitario_Comprado', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    if 'Costo_Compra' not in df_calc.columns:
         df_calc['Costo_Compra'] = 0.0
    df_calc.loc[:, 'Costo_Compra'] = cantidad * precio_unitario
    df_calc['Costo_Compra'] = df_calc['Costo_Compra'].astype(float)
    return df_calc

def calcular_costo_asignado(df):
    df_calc = df.copy()
    cantidad = pd.to_numeric(df_calc.get('Cantidad_Asignada', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    precio_unitario = pd.to_numeric(df_calc.get('Precio_Unitario_Asignado', pd.Series(0.0, index=df_calc.index)), errors='coerce').fillna(0.0)
    if 'Costo_Asignado' not in df_calc.columns:
         df_calc['Costo_Asignado'] = 0.0
    df_calc.loc[:, 'Costo_Asignado'] = cantidad * precio_unitario
    df_calc['Costo_Asignado'] = df_calc['Costo_Asignado'].astype(float)
    return df_calc

def load_data_into_session_state():
    tables_to_load = {
        'df_flotas': TABLE_FLOTAS, 'df_equipos': TABLE_EQUIPOS, 'df_consumo': TABLE_CONSUMO,
        'df_costos_salarial': TABLE_COSTOS_SALARIAL, 'df_gastos_fijos': TABLE_GASTOS_FIJOS,
        'df_gastos_mantenimiento': TABLE_GASTOS_MANTENIMIENTO, 'df_precios_combustible': TABLE_PRECIOS_COMBUSTIBLE,
        'df_proyectos': TABLE_PROYECTOS, 'df_presupuesto_materiales': TABLE_PRESUPUESTO_MATERIALES,
        'df_compras_materiales': TABLE_COMPRAS_MATERIALES, 'df_asignacion_materiales': TABLE_ASIGNACION_MATERIALES,
    }
    for ss_key, table_name in tables_to_load.items():
        if ss_key not in st.session_state:
            st.session_state[ss_key] = load_table(DATABASE_FILE, table_name)
            if table_name == TABLE_PRESUPUESTO_MATERIALES:
                st.session_state[ss_key] = calcular_costo_presupuestado(st.session_state[ss_key])
            elif table_name == TABLE_COMPRAS_MATERIALES:
                 st.session_state[ss_key] = calcular_costo_compra(st.session_state[ss_key])
            elif table_name == TABLE_ASIGNACION_MATERIALES:
                 st.session_state[ss_key] = calcular_costo_asignado(st.session_state[ss_key])

load_data_into_session_state()

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
            elif nombre_flota.lower() in st.session_state.df_flotas['Nombre_Flota'].astype(str).str.strip().str.lower().tolist():
                 st.warning(f"La flota '{nombre_flota}' ya existe.")
            else:
                existing_ids = set(st.session_state.df_flotas['ID_Flota'].astype(str).tolist())
                base_id = f"FLOTA_{int(time.time() * 1e6)}"
                id_flota = base_id
                counter = 0
                while id_flota in existing_ids:
                    counter += 1
                    id_flota = f"{base_id}_{counter}"
                new_flota_data = {'ID_Flota': id_flota, 'Nombre_Flota': nombre_flota}
                new_flota_df = pd.DataFrame([new_flota_data])
                expected_cols_flotas = list(TABLE_COLUMNS[TABLE_FLOTAS].keys())
                new_flota_df = new_flota_df.reindex(columns=expected_cols_flotas)
                for col, dtype in TABLE_COLUMNS[TABLE_FLOTAS].items():
                     if col in new_flota_df.columns:
                           try:
                                if dtype == 'object':
                                     new_flota_df[col] = new_flota_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                     new_flota_df[col] = new_flota_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                elif 'float' in dtype: new_flota_df[col] = pd.to_numeric(new_flota_df[col], errors='coerce').astype(float).fillna(0.0)
                                elif 'int' in dtype:
                                     if hasattr(pd, 'Int64Dtype'): new_flota_df[col] = pd.to_numeric(new_flota_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                     else: new_flota_df[col] = pd.to_numeric(new_flota_df[col], errors='coerce').astype(float).fillna(0.0)
                           except Exception as dtype_e:
                                st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                df_current_flotas_reindexed = st.session_state.df_flotas.reindex(columns=expected_cols_flotas)
                st.session_state.df_flotas = pd.concat([df_current_flotas_reindexed, new_flota_df], ignore_index=True)
                save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS)
                st.success(f"Flota '{nombre_flota}' añadida con ID: {id_flota}.")
                st.experimental_rerun()

    st.subheader("Lista de Flotas")
    if st.session_state.df_flotas.empty:
         st.info("No hay flotas registradas aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar flotas.")
        df_flotas_editable = st.session_state.df_flotas.copy()
        expected_cols_flotas = list(TABLE_COLUMNS[TABLE_FLOTAS].keys())
        df_flotas_editable = df_flotas_editable.reindex(columns=expected_cols_flotas)
        if 'ID_Flota' in df_flotas_editable.columns:
             df_flotas_editable['ID_Flota'] = df_flotas_editable['ID_Flota'].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
        df_flotas_edited = st.data_editor(
            df_flotas_editable, key="data_editor_flotas", num_rows="dynamic",
            column_config={
                 "ID_Flota": st.column_config.TextColumn("ID Flota", disabled=True),
                 "Nombre_Flota": st.column_config.TextColumn("Nombre Flota", required=True),
            }
        )
        df_flotas_edited_processed = df_flotas_edited.copy()
        df_flotas_edited_processed = df_flotas_edited_processed.reindex(columns=expected_cols_flotas)
        new_row_mask = df_flotas_edited_processed['ID_Flota'].isna() | (df_flotas_edited_processed['ID_Flota'].astype(str).str.strip() == '')
        if new_row_mask.any():
             existing_ids = set(st.session_state.df_flotas['ID_Flota'].astype(str).tolist())
             new_ids_batch = []
             for i in range(new_row_mask.sum()):
                  base_id = f"FLOTA_EDIT_{int(time.time() * 1e6)}_{i}"
                  unique_id = base_id
                  counter = 0
                  while unique_id in existing_ids or unique_id in new_ids_batch:
                      counter += 1
                      unique_id = f"{base_id}_{counter}"
                  new_ids_batch.append(unique_id)
             df_flotas_edited_processed.loc[new_row_mask, 'ID_Flota'] = new_ids_batch
        if 'Nombre_Flota' in df_flotas_edited_processed.columns:
             df_flotas_edited_processed['Nombre_Flota'] = df_flotas_edited_processed['Nombre_Flota'].astype(str).str.strip().replace({'': None}).mask(df_flotas_edited_processed['Nombre_Flota'].isna(), None)
        df_flotas_original_compare = st.session_state.df_flotas.reindex(columns=expected_cols_flotas).sort_values(by=expected_cols_flotas).reset_index(drop=True)
        df_flotas_edited_compare = df_flotas_edited_processed.reindex(columns=expected_cols_flotas).sort_values(by=expected_cols_flotas).reset_index(drop=True)
        if not df_flotas_edited_compare.equals(df_flotas_original_compare):
             if st.button("Guardar Cambios en Lista de Flotas", key="save_flotas_button"):
                  df_to_save = df_flotas_edited_processed.copy()
                  df_to_save = df_to_save[df_to_save['Nombre_Flota'].notna()].copy()
                  if df_to_save.empty and not df_flotas_edited_processed.empty:
                       st.error("Error: Ninguna fila válida. Complete Nombre de Flota.")
                  elif df_to_save['Nombre_Flota'].astype(str).str.strip().str.lower().duplicated().any():
                       st.error("Error: Nombres de flotas duplicados.")
                  elif df_to_save['ID_Flota'].astype(str).str.strip().duplicated().any():
                        st.error("Error: IDs de flota duplicados.")
                  else:
                       if 'ID_Flota' in df_to_save.columns:
                           df_to_save['ID_Flota'] = df_to_save['ID_Flota'].astype(str).str.strip().replace({'': None}).mask(df_to_save['ID_Flota'].isna(), None)
                       st.session_state.df_flotas = df_to_save
                       save_table(st.session_state.df_flotas, DATABASE_FILE, TABLE_FLOTAS)
                       st.success("Cambios en flotas guardados.")
                       st.experimental_rerun()
             else:
                 st.info("Hay cambios sin guardar en la lista de flotas.")

def page_equipos():
    st.title("Gestión de Equipos de Mina")
    # ... (rest of the page_equipos function, no st.number_input with required=True here)
    # Code for page_equipos is extensive but doesn't use st.number_input directly in its forms.
    # It uses st.data_editor which has its own way of handling required for NumberColumn.
    # So, no changes needed in page_equipos for the 'required' argument issue with st.number_input.
    # The existing logic for st.data_editor's NumberColumn seems fine.
    # For brevity, I'll skip pasting the whole function if no 'required' argument issue is present.
    # However, the user asked for the *complete* code. I'll paste it and ensure no st.number_input(..., required=True) is there.
    st.write("Aquí puedes añadir, editar y eliminar equipos.")
    flotas_disponibles_for_select = st.session_state.df_flotas[
         (st.session_state.df_flotas['ID_Flota'].astype(str).str.strip() != '') &
         (st.session_state.df_flotas['Nombre_Flota'].astype(str).str.strip() != '') &
         (st.session_state.df_flotas['ID_Flota'].notna()) &
         (st.session_state.df_flotas['Nombre_Flota'].notna())
    ].copy()
    flota_id_to_display_label = {
         str(row['ID_Flota']): f"{row['Nombre_Flota']} (ID: {row['ID_Flota']})"
         for index, row in flotas_disponibles_for_select.iterrows()
    }
    null_flota_label = "Sin Flota"
    flota_id_to_display_label[str(pd.NA)] = null_flota_label
    flota_id_to_display_label['nan'] = null_flota_label
    flota_id_to_display_label['None'] = null_flota_label
    flota_id_to_display_label[''] = null_flota_label
    flota_options_list = [(null_flota_label, pd.NA)] + \
                          sorted([(flota_id_to_display_label[str(row['ID_Flota'])], row['ID_Flota'])
                                  for index, row in flotas_disponibles_for_select.iterrows()],
                                 key=lambda x: x[0])
    flota_option_labels = [item[0] for item in flota_options_list]
    flota_label_to_value = dict(flota_options_list)
    if not flota_option_labels or (len(flota_option_labels) == 1 and flota_option_labels[0] == null_flota_label):
        st.warning("No hay flotas registradas. Añada flotas primero.")
        flota_option_labels = [null_flota_label]
        flota_label_to_value = {null_flota_label: pd.NA}

    st.subheader("Añadir Nuevo Equipo")
    with st.form("form_add_equipo", clear_on_submit=True):
        interno = st.text_input("Interno del Equipo").strip()
        patente = st.text_input("Patente").strip()
        selected_flota_label = st.selectbox(
            "Seleccionar Flota:", options=flota_option_labels, index=0, key="add_equipo_flota_select"
        )
        selected_flota_value = flota_label_to_value.get(selected_flota_label, pd.NA)
        submitted = st.form_submit_button("Añadir Equipo")
        if submitted:
            if not interno or not patente:
                st.warning("Por favor, complete Interno y Patente.")
            elif interno.lower() in st.session_state.df_equipos['Interno'].astype(str).str.strip().str.lower().tolist():
                st.warning(f"Ya existe un equipo con Interno '{interno}'.")
            else:
                new_equipo_data = {'Interno': interno, 'Patente': patente, 'ID_Flota': selected_flota_value}
                new_equipo_df = pd.DataFrame([new_equipo_data])
                expected_cols_equipos = list(TABLE_COLUMNS[TABLE_EQUIPOS].keys())
                new_equipo_df = new_equipo_df.reindex(columns=expected_cols_equipos)
                for col, dtype in TABLE_COLUMNS[TABLE_EQUIPOS].items():
                     if col in new_equipo_df.columns:
                          try:
                              if dtype == 'object':
                                  new_equipo_df[col] = new_equipo_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                  new_equipo_df[col] = new_equipo_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                              elif 'float' in dtype: new_equipo_df[col] = pd.to_numeric(new_equipo_df[col], errors='coerce').astype(float).fillna(0.0)
                              elif 'int' in dtype:
                                   if hasattr(pd, 'Int64Dtype'): new_equipo_df[col] = pd.to_numeric(new_equipo_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                   else: new_equipo_df[col] = pd.to_numeric(new_equipo_df[col], errors='coerce').astype(float).fillna(0.0)
                          except Exception as dtype_e:
                               st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                df_current_equipos_reindexed = st.session_state.df_equipos.reindex(columns=expected_cols_equipos)
                st.session_state.df_equipos = pd.concat([df_current_equipos_reindexed, new_equipo_df], ignore_index=True)
                save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS)
                flota_name_display = flota_id_to_display_label.get(str(selected_flota_value), null_flota_label)
                st.success(f"Equipo {interno} ({patente}) añadido a flota '{flota_name_display}'.")
                st.experimental_rerun()

    st.subheader("Lista de Equipos")
    if st.session_state.df_equipos.empty:
        st.info("No hay equipos registrados aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar equipos.")
        df_equipos_editable = st.session_state.df_equipos.copy()
        flota_ids_for_editor = st.session_state.df_flotas['ID_Flota'].dropna().astype(str).unique().tolist()
        flota_editor_options_values = [str(pd.NA)] + flota_ids_for_editor
        flota_editor_options_values = list(dict.fromkeys(flota_editor_options_values))
        flota_id_to_name_editor = {str(row['ID_Flota']).strip(): str(row['Nombre_Flota'])
                                   for index, row in st.session_state.df_flotas.iterrows()
                                   if pd.notna(row['ID_Flota']) and pd.notna(row['Nombre_Flota']) and str(row['ID_Flota']).strip() != ''}
        flota_id_to_name_editor[str(pd.NA)] = null_flota_label
        flota_id_to_name_editor['nan'] = null_flota_label
        flota_id_to_name_editor['None'] = null_flota_label
        flota_id_to_name_editor[''] = null_flota_label
        def format_flota_for_editor_robust(id_value):
            try:
                if pd.isna(id_value) or str(id_value).strip() == '' or str(id_value).lower() in ['nan', 'none', 'na']:
                     return null_flota_label
                id_str_clean = str(id_value).strip()
                return flota_id_to_name_editor.get(id_str_clean, f"ID Desconocido ({id_str_clean})")
            except Exception:
                 return f"Error ({id_value})"
        expected_cols_equipos = list(TABLE_COLUMNS[TABLE_EQUIPOS].keys())
        df_equipos_editable = df_equipos_editable.reindex(columns=expected_cols_equipos)
        df_equipos_editable['ID_Flota'] = df_equipos_editable['ID_Flota'].apply(
             lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != '' else pd.NA
        )
        df_equipos_edited = st.data_editor(
            df_equipos_editable, key="data_editor_equipos", num_rows="dynamic",
            column_config={
                 "Interno": st.column_config.TextColumn("Interno", required=True),
                 "Patente": st.column_config.TextColumn("Patente", required=True),
                 "ID_Flota": st.column_config.SelectboxColumn(
                     "Flota", options=flota_editor_options_values, required=False,
                     format_func=format_flota_for_editor_robust
                 )
            }
        )
        df_equipos_edited_processed = df_equipos_edited.copy()
        df_equipos_edited_processed = df_equipos_edited_processed.reindex(columns=expected_cols_equipos)
        if 'ID_Flota' in df_equipos_edited_processed.columns:
             df_equipos_edited_processed['ID_Flota'] = df_equipos_edited_processed['ID_Flota'].apply(
                 lambda x: pd.NA if pd.isna(x) or str(x).strip() == '' or str(x).lower() in ['nan', 'none', 'na'] else x
             )
             df_equipos_edited_processed['ID_Flota'] = df_equipos_edited_processed['ID_Flota'].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({pd.NA: None})
        for col in ['Interno', 'Patente']:
            if col in df_equipos_edited_processed.columns:
                 df_equipos_edited_processed[col] = df_equipos_edited_processed[col].astype(str).str.strip().replace({'': None}).mask(df_equipos_edited_processed[col].isna(), None)
        df_equipos_original_compare = st.session_state.df_equipos.reindex(columns=expected_cols_equipos).sort_values(by=expected_cols_equipos).reset_index(drop=True)
        df_equipos_edited_compare = df_equipos_edited_processed.reindex(columns=expected_cols_equipos).sort_values(by=expected_cols_equipos).reset_index(drop=True)
        if not df_equipos_edited_compare.equals(df_equipos_original_compare):
             if st.button("Guardar Cambios en Lista de Equipos", key="save_equipos_button"):
                  df_to_save = df_equipos_edited_processed.copy()
                  df_to_save = df_to_save[(df_to_save['Interno'].notna()) & (df_to_save['Patente'].notna())].copy()
                  if df_to_save.empty and not df_equipos_edited_processed.empty:
                       st.error("Error: Ninguna fila válida. Complete Interno y Patente.")
                  elif df_to_save['Interno'].astype(str).str.strip().str.lower().duplicated().any():
                       st.error("Error: Internos de Equipo duplicados.")
                  else:
                       st.session_state.df_equipos = df_to_save
                       save_table(st.session_state.df_equipos, DATABASE_FILE, TABLE_EQUIPOS)
                       st.success("Cambios en equipos guardados.")
                       st.experimental_rerun()
             else:
                 st.info("Hay cambios sin guardar en la lista de equipos.")

def page_consumibles():
    st.title("Registro de Consumibles por Equipo")
    st.write("Aquí puedes registrar el consumo de combustible, horas y kilómetros por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].unique().tolist()
    internos_disponibles = [str(i).strip() for i in internos_disponibles if pd.notna(i) and str(i).strip() != '']
    internos_disponibles.sort()

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero para registrar consumibles.")
        return

    st.subheader("Añadir Registro de Consumo")
    with st.form("form_add_consumo", clear_on_submit=True):
        interno_seleccionado = st.selectbox("Seleccione Equipo (Interno)", internos_disponibles, key="con_int_add")
        fecha = st.date_input("Fecha", value=datetime.date.today(), key="con_fecha_add")
        consumo_litros = st.number_input("Consumo en Litros de Combustible", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="con_litros_add") # Removed required
        horas_trabajadas = st.number_input("Cantidad de Horas Trabajadas", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="con_horas_add") # Removed required
        kilometros_recorridos = st.number_input("Cantidad de Kilómetros Recorridos", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="con_km_add") # Removed required

        submitted = st.form_submit_button("Registrar Consumo")
        if submitted:
            if not interno_seleccionado or str(interno_seleccionado).strip() == '':
                 st.warning("Por favor, complete el campo 'Equipo'.")
            elif not fecha:
                 st.warning("Por favor, complete el campo 'Fecha' con una fecha válida.")
            elif consumo_litros is None or horas_trabajadas is None or kilometros_recorridos is None: # Added None check
                 st.warning("Por favor, complete todos los campos numéricos (Consumo, Horas, Kilómetros).")
            elif consumo_litros == 0 and horas_trabajadas == 0 and kilometros_recorridos == 0:
                 st.warning("Por favor, ingrese al menos un valor de Consumo, Horas o Kilómetros mayor a cero.")
            else:
                 new_consumo_data = {
                    'Interno': str(interno_seleccionado).strip(),
                    'Fecha': fecha,
                    'Consumo_Litros': float(consumo_litros if consumo_litros is not None else 0.0), # Handle None before float conversion
                    'Horas_Trabajadas': float(horas_trabajadas if horas_trabajadas is not None else 0.0),
                    'Kilometros_Recorridos': float(kilometros_recorridos if kilometros_recorridos is not None else 0.0)
                 }
                 new_consumo_df = pd.DataFrame([new_consumo_data])
                 expected_cols_consumo = list(TABLE_COLUMNS[TABLE_CONSUMO].keys())
                 new_consumo_df = new_consumo_df.reindex(columns=expected_cols_consumo)
                 date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
                 if date_col_name_consumo in new_consumo_df.columns:
                      new_consumo_df[date_col_name_consumo] = pd.to_datetime(new_consumo_df[date_col_name_consumo], errors='coerce')
                 for col, dtype in TABLE_COLUMNS[TABLE_CONSUMO].items():
                     if col in new_consumo_df.columns:
                           try:
                                if dtype == 'object':
                                     new_consumo_df[col] = new_consumo_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                     new_consumo_df[col] = new_consumo_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                elif 'float' in dtype:
                                      new_consumo_df[col] = pd.to_numeric(new_consumo_df[col], errors='coerce').astype(float).fillna(0.0)
                                elif 'int' in dtype:
                                     if hasattr(pd, 'Int64Dtype'): new_consumo_df[col] = pd.to_numeric(new_consumo_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                     else: new_consumo_df[col] = pd.to_numeric(new_consumo_df[col], errors='coerce').astype(float).fillna(0.0)
                           except Exception as dtype_e:
                                st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                 df_current_consumo_reindexed = st.session_state.df_consumo.reindex(columns=expected_cols_consumo)
                 st.session_state.df_consumo = pd.concat([df_current_consumo_reindexed, new_consumo_df], ignore_index=True)
                 save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO)
                 st.success("Registro de consumo añadido.")
                 st.experimental_rerun()

    st.subheader("Registros de Consumo Existente")
    # ... (rest of page_consumibles, data_editor does not use st.number_input with required)
    if st.session_state.df_consumo.empty:
         st.info("No hay registros de consumo aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar registros.")
        df_consumo_editable = st.session_state.df_consumo.copy()
        date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
        if date_col_name_consumo in df_consumo_editable.columns:
             df_consumo_editable[date_col_name_consumo] = pd.to_datetime(df_consumo_editable[date_col_name_consumo], errors='coerce')
        else:
             df_consumo_editable[date_col_name_consumo] = pd.Series(dtype='datetime64[ns]', index=df_consumo_editable.index)
        expected_cols_consumo = list(TABLE_COLUMNS[TABLE_CONSUMO].keys())
        df_consumo_editable = df_consumo_editable.reindex(columns=expected_cols_consumo)
        if 'Interno' in df_consumo_editable.columns:
             df_consumo_editable['Interno'] = df_consumo_editable['Interno'].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
        df_consumo_edited = st.data_editor(
             df_consumo_editable, key="data_editor_consumo", num_rows="dynamic",
             column_config={
                  date_col_name_consumo: st.column_config.DateColumn("Fecha", required=True),
                  "Interno": st.column_config.TextColumn("Interno", required=True),
                  "Consumo_Litros": st.column_config.NumberColumn("Consumo Litros", min_value=0.0, format="%.2f", required=True),
                  "Horas_Trabajadas": st.column_config.NumberColumn("Horas Trabajadas", min_value=0.0, format="%.2f", required=True),
                  "Kilometros_Recorridos": st.column_config.NumberColumn("Kilómetros Recorridos", min_value=0.0, format="%.2f", required=True),
             }
         )
        df_consumo_edited_processed = df_consumo_edited.copy()
        df_consumo_edited_processed = df_consumo_edited_processed.reindex(columns=expected_cols_consumo)
        if 'Interno' in df_consumo_edited_processed.columns:
             df_consumo_edited_processed['Interno'] = df_consumo_edited_processed['Interno'].astype(str).str.strip().replace({'': None}).mask(df_consumo_edited_processed['Interno'].isna(), None)
        df_consumo_original_compare = st.session_state.df_consumo.reindex(columns=expected_cols_consumo).sort_values(by=expected_cols_consumo).reset_index(drop=True)
        df_consumo_edited_compare = df_consumo_edited_processed.reindex(columns=expected_cols_consumo).sort_values(by=expected_cols_consumo).reset_index(drop=True)
        if not df_consumo_edited_compare.equals(df_consumo_original_compare):
             if st.button("Guardar Cambios en Registros de Consumo", key="save_consumo_button"):
                  df_to_save = df_consumo_edited_processed.copy()
                  date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
                  df_to_save = df_to_save[(df_to_save['Interno'].notna()) & (df_to_save[date_col_name_consumo].notna())].copy()
                  if df_to_save.empty and not df_consumo_edited_processed.empty:
                       st.error("Error: Ninguna fila válida. Complete Interno y Fecha.")
                  elif df_to_save['Consumo_Litros'].isnull().any() or df_to_save['Horas_Trabajadas'].isnull().any() or df_to_save['Kilometros_Recorridos'].isnull().any():
                       st.error("Error: Los campos numéricos no pueden estar vacíos.")
                  elif ((pd.to_numeric(df_to_save['Consumo_Litros'], errors='coerce').fillna(0) == 0) &
                        (pd.to_numeric(df_to_save['Horas_Trabajadas'], errors='coerce').fillna(0) == 0) &
                        (pd.to_numeric(df_to_save['Kilometros_Recorridos'], errors='coerce').fillna(0) == 0)).any():
                       st.warning("Advertencia: Algunas filas tienen Consumo, Horas y Kilómetros todos cero.")
                  internos_disponibles_set = set(internos_disponibles)
                  invalid_internos = df_to_save[~df_to_save['Interno'].astype(str).isin(internos_disponibles_set)]['Interno'].unique().tolist()
                  if invalid_internos:
                       st.error(f"Error: Internos no existen: {', '.join(invalid_internos)}.")
                  else:
                       st.session_state.df_consumo = df_to_save
                       save_table(st.session_state.df_consumo, DATABASE_FILE, TABLE_CONSUMO)
                       st.success("Cambios en registros de consumo guardados.")
                       st.experimental_rerun()
             else:
                 st.info("Hay cambios sin guardar en registros de consumo.")


def page_costos_equipos():
    st.title("Registro de Costos por Equipo")
    st.write("Aquí puedes registrar costos salariales, fijos y de mantenimiento por equipo y fecha.")

    internos_disponibles = st.session_state.df_equipos['Interno'].unique().tolist()
    internos_disponibles = [str(i).strip() for i in internos_disponibles if pd.notna(i) and str(i).strip() != '']
    internos_disponibles.sort()

    if not internos_disponibles:
        st.warning("No hay equipos registrados. Por favor, añada equipos primero para registrar costos.")
        if "selected_cost_interno_selectbox_persistent" in st.session_state: del st.session_state["selected_cost_interno_selectbox_persistent"]
        if "selected_cost_interno" in st.session_state: del st.session_state["selected_cost_interno"]
        return

    default_interno = internos_disponibles[0] if internos_disponibles else None
    if "selected_cost_interno" not in st.session_state or st.session_state.selected_cost_interno not in internos_disponibles:
         st.session_state.selected_cost_interno = default_interno
    try:
        default_index = internos_disponibles.index(st.session_state.selected_cost_interno)
    except ValueError:
        default_index = 0
        st.session_state.selected_cost_interno = internos_disponibles[0] if internos_disponibles else None

    selected_interno = st.selectbox(
        "Seleccione Equipo (Interno) para añadir registros:",
        options=internos_disponibles, index=default_index, key="selected_cost_interno_selectbox_persistent"
    )
    st.session_state.selected_cost_interno = selected_interno
    tab1, tab2, tab3 = st.tabs(["Costos Salariales", "Gastos Fijos", "Gastos Mantenimiento"])
    internos_disponibles_set = set(internos_disponibles)

    with tab1:
        st.subheader("Registro de Costos Salariales")
        with st.form("form_add_salarial", clear_on_submit=True):
            fecha = st.date_input("Fecha", key="sal_fecha", value=datetime.date.today())
            monto_salarial = st.number_input("Monto Salarial", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="sal_monto_add") # Removed required
            submitted = st.form_submit_button("Registrar Costo Salarial")
            if submitted:
                if not selected_interno or str(selected_interno).strip() == '':
                     st.warning("Por favor, seleccione un Equipo.")
                elif not fecha:
                     st.warning("Por favor, complete el campo 'Fecha'.")
                elif monto_salarial is None or monto_salarial <= 0: # Added None check
                    st.warning("Por favor, complete el campo 'Monto Salarial' con un valor mayor a cero.")
                else:
                    new_costo_data = {
                       'Interno': str(selected_interno).strip(),
                       'Fecha': fecha,
                       'Monto_Salarial': float(monto_salarial if monto_salarial is not None else 0.0) # Handle None
                    }
                    new_costo_df = pd.DataFrame([new_costo_data])
                    expected_cols_salarial = list(TABLE_COLUMNS[TABLE_COSTOS_SALARIAL].keys())
                    new_costo_df = new_costo_df.reindex(columns=expected_cols_salarial)
                    date_col_name_salarial = DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]
                    if date_col_name_salarial in new_costo_df.columns:
                         new_costo_df[date_col_name_salarial] = pd.to_datetime(new_costo_df[date_col_name_salarial], errors='coerce')
                    for col, dtype in TABLE_COLUMNS[TABLE_COSTOS_SALARIAL].items():
                       if col in new_costo_df.columns:
                            try:
                                 if dtype == 'object':
                                      new_costo_df[col] = new_costo_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                      new_costo_df[col] = new_costo_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                 elif 'float' in dtype: new_costo_df[col] = pd.to_numeric(new_costo_df[col], errors='coerce').astype(float).fillna(0.0)
                                 elif 'int' in dtype:
                                      if hasattr(pd, 'Int64Dtype'): new_costo_df[col] = pd.to_numeric(new_costo_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                      else: new_costo_df[col] = pd.to_numeric(new_costo_df[col], errors='coerce').astype(float).fillna(0.0)
                            except Exception as dtype_e:
                                 st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                    df_current_salarial_reindexed = st.session_state.df_costos_salarial.reindex(columns=expected_cols_salarial)
                    st.session_state.df_costos_salarial = pd.concat([df_current_salarial_reindexed, new_costo_df], ignore_index=True)
                    save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL)
                    st.success("Costo salarial registrado.")
                    st.experimental_rerun()
        st.subheader("Registros Salariales Existente")
        # ... (rest of tab1, data_editor)
        if st.session_state.df_costos_salarial.empty:
             st.info("No hay registros salariales aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros.")
            df_salarial_editable = st.session_state.df_costos_salarial.copy()
            date_col_name_salarial = DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]
            if date_col_name_salarial in df_salarial_editable.columns:
                 df_salarial_editable[date_col_name_salarial] = pd.to_datetime(df_salarial_editable[date_col_name_salarial], errors='coerce')
            else:
                 df_salarial_editable[date_col_name_salarial] = pd.Series(dtype='datetime64[ns]', index=df_salarial_editable.index)
            expected_cols_salarial = list(TABLE_COLUMNS[TABLE_COSTOS_SALARIAL].keys())
            df_salarial_editable = df_salarial_editable.reindex(columns=expected_cols_salarial)
            if 'Interno' in df_salarial_editable.columns:
                 df_salarial_editable['Interno'] = df_salarial_editable['Interno'].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
            df_salarial_edited = st.data_editor(
                df_salarial_editable, key="data_editor_salarial", num_rows="dynamic",
                 column_config={
                     date_col_name_salarial: st.column_config.DateColumn("Fecha", required=True),
                     "Interno": st.column_config.TextColumn("Interno", required=True),
                     "Monto_Salarial": st.column_config.NumberColumn("Monto Salarial", min_value=0.0, format="%.2f", required=True),
                 }
            )
            df_salarial_edited_processed = df_salarial_edited.copy()
            df_salarial_edited_processed = df_salarial_edited_processed.reindex(columns=expected_cols_salarial)
            if 'Interno' in df_salarial_edited_processed.columns:
                 df_salarial_edited_processed['Interno'] = df_salarial_edited_processed['Interno'].astype(str).str.strip().replace({'': None}).mask(df_salarial_edited_processed['Interno'].isna(), None)
            df_salarial_original_compare = st.session_state.df_costos_salarial.reindex(columns=expected_cols_salarial).sort_values(by=expected_cols_salarial).reset_index(drop=True)
            df_salarial_edited_compare = df_salarial_edited_processed.reindex(columns=expected_cols_salarial).sort_values(by=expected_cols_salarial).reset_index(drop=True)
            if not df_salarial_edited_compare.equals(df_salarial_original_compare):
                 if st.button("Guardar Cambios en Registros Salariales", key="save_salarial_button"):
                      df_to_save = df_salarial_edited_processed.copy()
                      date_col_name_salarial = DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL]
                      df_to_save = df_to_save[(df_to_save['Interno'].notna()) & (df_to_save[date_col_name_salarial].notna())].copy()
                      if df_to_save.empty and not df_salarial_edited_processed.empty:
                           st.error("Error: Ninguna fila válida. Complete Interno y Fecha.")
                      elif df_to_save['Monto_Salarial'].isnull().any():
                            st.error("Error: El campo 'Monto Salarial' no puede estar vacío.")
                      elif (pd.to_numeric(df_to_save['Monto_Salarial'], errors='coerce').fillna(0) <= 0).any():
                           st.warning("Advertencia: Algunos registros tienen 'Monto Salarial' <= 0.")
                      invalid_internos = df_to_save[~df_to_save['Interno'].astype(str).isin(internos_disponibles_set)]['Interno'].unique().tolist()
                      if invalid_internos:
                           st.error(f"Error: Internos no existen: {', '.join(invalid_internos)}.")
                      else:
                           st.session_state.df_costos_salarial = df_to_save
                           save_table(st.session_state.df_costos_salarial, DATABASE_FILE, TABLE_COSTOS_SALARIAL)
                           st.success("Cambios en registros salariales guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros salariales.")

    with tab2:
        st.subheader("Registro de Gastos Fijos")
        with st.form("form_add_fijos", clear_on_submit=True):
             fecha = st.date_input("Fecha", key="fij_fecha", value=datetime.date.today())
             tipo_gasto = st.text_input("Tipo de Gasto Fijo", key="fij_tipo_add").strip()
             monto_gasto = st.number_input("Monto del Gasto Fijo", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="fij_monto_add") # Removed required
             descripcion = st.text_area("Descripción (Opcional)", key="fij_desc_add").strip()
             submitted = st.form_submit_button("Registrar Gasto Fijo")
             if submitted:
                  if not selected_interno or str(selected_interno).strip() == '':
                       st.warning("Por favor, seleccione un Equipo.")
                  elif not fecha:
                      st.warning("Por favor, complete el campo 'Fecha'.")
                  elif monto_gasto is None or monto_gasto <= 0: # Added None check
                       st.warning("Por favor, complete el campo 'Monto del Gasto Fijo' con un valor mayor a cero.")
                  elif not tipo_gasto:
                       st.warning("Por favor, complete el campo 'Tipo de Gasto Fijo'.")
                  else:
                      new_gasto_data = {
                         'Interno': str(selected_interno).strip(),
                         'Fecha': fecha,
                         'Tipo_Gasto_Fijo': tipo_gasto,
                         'Monto_Gasto_Fijo': float(monto_gasto if monto_gasto is not None else 0.0), # Handle None
                         'Descripcion': descripcion if descripcion else None
                      }
                      new_gasto_df = pd.DataFrame([new_gasto_data])
                      expected_cols_fijos = list(TABLE_COLUMNS[TABLE_GASTOS_FIJOS].keys())
                      new_gasto_df = new_gasto_df.reindex(columns=expected_cols_fijos)
                      date_col_name_fijos = DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]
                      if date_col_name_fijos in new_gasto_df.columns:
                           new_gasto_df[date_col_name_fijos] = pd.to_datetime(new_gasto_df[date_col_name_fijos], errors='coerce')
                      for col, dtype in TABLE_COLUMNS[TABLE_GASTOS_FIJOS].items():
                          if col in new_gasto_df.columns:
                               try:
                                    if dtype == 'object':
                                         new_gasto_df[col] = new_gasto_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                         new_gasto_df[col] = new_gasto_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                    elif 'float' in dtype: new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(float).fillna(0.0)
                                    elif 'int' in dtype:
                                         if hasattr(pd, 'Int64Dtype'): new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                         else: new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(float).fillna(0.0)
                               except Exception as dtype_e:
                                    st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                      df_current_fijos_reindexed = st.session_state.df_gastos_fijos.reindex(columns=expected_cols_fijos)
                      st.session_state.df_gastos_fijos = pd.concat([df_current_fijos_reindexed, new_gasto_df], ignore_index=True)
                      save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS)
                      st.success("Gasto fijo registrado.")
                      st.experimental_rerun()
        st.subheader("Registros de Gastos Fijos Existente")
        # ... (rest of tab2, data_editor)
        if st.session_state.df_gastos_fijos.empty:
             st.info("No hay registros de gastos fijos aún.")
        else:
             st.info("Edite la tabla siguiente para modificar o eliminar registros.")
             df_fijos_editable = st.session_state.df_gastos_fijos.copy()
             date_col_name_fijos = DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]
             if date_col_name_fijos in df_fijos_editable.columns:
                  df_fijos_editable[date_col_name_fijos] = pd.to_datetime(df_fijos_editable[date_col_name_fijos], errors='coerce')
             else:
                  df_fijos_editable[date_col_name_fijos] = pd.Series(dtype='datetime64[ns]', index=df_fijos_editable.index)
             expected_cols_fijos = list(TABLE_COLUMNS[TABLE_GASTOS_FIJOS].keys())
             df_fijos_editable = df_fijos_editable.reindex(columns=expected_cols_fijos)
             for col in ['Interno', 'Tipo_Gasto_Fijo', 'Descripcion']:
                 if col in df_fijos_editable.columns:
                      df_fijos_editable[col] = df_fijos_editable[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
             df_fijos_edited = st.data_editor(
                 df_fijos_editable, key="data_editor_fijos", num_rows="dynamic",
                 column_config={
                      date_col_name_fijos: st.column_config.DateColumn("Fecha", required=True),
                      "Interno": st.column_config.TextColumn("Interno", required=True),
                      "Tipo_Gasto_Fijo": st.column_config.TextColumn("Tipo Gasto Fijo", required=True),
                      "Monto_Gasto_Fijo": st.column_config.NumberColumn("Monto Gasto Fijo", min_value=0.0, format="%.2f", required=True),
                      "Descripcion": st.column_config.TextColumn("Descripción", required=False),
                  }
             )
             df_fijos_edited_processed = df_fijos_edited.copy()
             df_fijos_edited_processed = df_fijos_edited_processed.reindex(columns=expected_cols_fijos)
             for col in ['Interno', 'Tipo_Gasto_Fijo', 'Descripcion']:
                  if col in df_fijos_edited_processed.columns:
                       df_fijos_edited_processed[col] = df_fijos_edited_processed[col].astype(str).str.strip().replace({'': None}).mask(df_fijos_edited_processed[col].isna(), None)
             df_fijos_original_compare = st.session_state.df_gastos_fijos.reindex(columns=expected_cols_fijos).sort_values(by=expected_cols_fijos).reset_index(drop=True)
             df_fijos_edited_compare = df_fijos_edited_processed.reindex(columns=expected_cols_fijos).sort_values(by=expected_cols_fijos).reset_index(drop=True)
             if not df_fijos_edited_compare.equals(df_fijos_original_compare):
                  if st.button("Guardar Cambios en Registros de Gastos Fijos", key="save_fijos_button"):
                       df_to_save = df_fijos_edited_processed.copy()
                       date_col_name_fijos = DATETIME_COLUMNS[TABLE_GASTOS_FIJOS]
                       df_to_save = df_to_save[(df_to_save['Interno'].notna()) & (df_to_save[date_col_name_fijos].notna()) & (df_to_save['Tipo_Gasto_Fijo'].notna())].copy()
                       if df_to_save.empty and not df_fijos_edited_processed.empty:
                            st.error("Error: Ninguna fila válida. Complete campos obligatorios.")
                       elif df_to_save['Monto_Gasto_Fijo'].isnull().any():
                            st.error("Error: El campo 'Monto Gasto Fijo' no puede estar vacío.")
                       elif (pd.to_numeric(df_to_save['Monto_Gasto_Fijo'], errors='coerce').fillna(0) <= 0).any():
                            st.warning("Advertencia: Algunos registros tienen 'Monto Gasto Fijo' <= 0.")
                       invalid_internos = df_to_save[~df_to_save['Interno'].astype(str).isin(internos_disponibles_set)]['Interno'].unique().tolist()
                       if invalid_internos:
                            st.error(f"Error: Internos no existen: {', '.join(invalid_internos)}.")
                       else:
                           st.session_state.df_gastos_fijos = df_to_save
                           save_table(st.session_state.df_gastos_fijos, DATABASE_FILE, TABLE_GASTOS_FIJOS)
                           st.success("Cambios en registros de gastos fijos guardados.")
                           st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en registros de gastos fijos.")

    with tab3:
        st.subheader("Registro de Gastos de Mantenimiento")
        with st.form("form_add_mantenimiento", clear_on_submit=True):
             fecha = st.date_input("Fecha", key="mant_fecha", value=datetime.date.today())
             tipo_mantenimiento = st.text_input("Tipo de Mantenimiento", key="mant_tipo_add").strip()
             monto_mantenimiento = st.number_input("Monto del Mantenimiento", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="mant_monto_add") # Removed required
             descripcion = st.text_area("Descripción (Opcional)", key="mant_desc_add").strip()
             submitted = st.form_submit_button("Registrar Gasto Mantenimiento")
             if submitted:
                  if not selected_interno or str(selected_interno).strip() == '':
                       st.warning("Por favor, seleccione un Equipo.")
                  elif not fecha:
                      st.warning("Por favor, complete el campo 'Fecha'.")
                  elif monto_mantenimiento is None or monto_mantenimiento <= 0: # Added None check
                       st.warning("Por favor, complete el campo 'Monto del Mantenimiento' con un valor mayor a cero.")
                  elif not tipo_mantenimiento:
                       st.warning("Por favor, complete el campo 'Tipo de Mantenimiento'.")
                  else:
                      new_gasto_data = {
                         'Interno': str(selected_interno).strip(),
                         'Fecha': fecha,
                         'Tipo_Mantenimiento': tipo_mantenimiento,
                         'Monto_Mantenimiento': float(monto_mantenimiento if monto_mantenimiento is not None else 0.0), # Handle None
                         'Descripcion': descripcion if descripcion else None
                      }
                      new_gasto_df = pd.DataFrame([new_gasto_data])
                      expected_cols_mantenimiento = list(TABLE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO].keys())
                      new_gasto_df = new_gasto_df.reindex(columns=expected_cols_mantenimiento)
                      date_col_name_mantenimiento = DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]
                      if date_col_name_mantenimiento in new_gasto_df.columns:
                           new_gasto_df[date_col_name_mantenimiento] = pd.to_datetime(new_gasto_df[date_col_name_mantenimiento], errors='coerce')
                      for col, dtype in TABLE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO].items():
                          if col in new_gasto_df.columns:
                               try:
                                    if dtype == 'object':
                                         new_gasto_df[col] = new_gasto_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                         new_gasto_df[col] = new_gasto_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                    elif 'float' in dtype: new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(float).fillna(0.0)
                                    elif 'int' in dtype:
                                         if hasattr(pd, 'Int64Dtype'): new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                         else: new_gasto_df[col] = pd.to_numeric(new_gasto_df[col], errors='coerce').astype(float).fillna(0.0)
                               except Exception as dtype_e:
                                    st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                      df_current_mantenimiento_reindexed = st.session_state.df_gastos_mantenimiento.reindex(columns=expected_cols_mantenimiento)
                      st.session_state.df_gastos_mantenimiento = pd.concat([df_current_mantenimiento_reindexed, new_gasto_df], ignore_index=True)
                      save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO)
                      st.success("Gasto de mantenimiento registrado.")
                      st.experimental_rerun()
        st.subheader("Registros de Gastos de Mantenimiento Existente")
        # ... (rest of tab3, data_editor)
        if st.session_state.df_gastos_mantenimiento.empty:
            st.info("No hay registros de mantenimiento aún.")
        else:
            st.info("Edite la tabla siguiente para modificar o eliminar registros.")
            df_mantenimiento_editable = st.session_state.df_gastos_mantenimiento.copy()
            date_col_name_mantenimiento = DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]
            if date_col_name_mantenimiento in df_mantenimiento_editable.columns:
                 df_mantenimiento_editable[date_col_name_mantenimiento] = pd.to_datetime(df_mantenimiento_editable[date_col_name_mantenimiento], errors='coerce')
            else:
                 df_mantenimiento_editable[date_col_name_mantenimiento] = pd.Series(dtype='datetime64[ns]', index=df_mantenimiento_editable.index)
            expected_cols_mantenimiento = list(TABLE_COLUMNS[TABLE_GASTOS_MANTENIMIENTO].keys())
            df_mantenimiento_editable = df_mantenimiento_editable.reindex(columns=expected_cols_mantenimiento)
            for col in ['Interno', 'Tipo_Mantenimiento', 'Descripcion']:
                 if col in df_mantenimiento_editable.columns:
                      df_mantenimiento_editable[col] = df_mantenimiento_editable[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
            df_mantenimiento_edited = st.data_editor(
                df_mantenimiento_editable, key="data_editor_mantenimiento", num_rows="dynamic",
                column_config={
                     date_col_name_mantenimiento: st.column_config.DateColumn("Fecha", required=True),
                     "Interno": st.column_config.TextColumn("Interno", required=True),
                     "Tipo_Mantenimiento": st.column_config.TextColumn("Tipo Mantenimiento", required=True),
                     "Monto_Mantenimiento": st.column_config.NumberColumn("Monto Mantenimiento", min_value=0.0, format="%.2f", required=True),
                     "Descripcion": st.column_config.TextColumn("Descripción", required=False),
                 }
            )
            df_mantenimiento_edited_processed = df_mantenimiento_edited.copy()
            df_mantenimiento_edited_processed = df_mantenimiento_edited_processed.reindex(columns=expected_cols_mantenimiento)
            for col in ['Interno', 'Tipo_Mantenimiento', 'Descripcion']:
                 if col in df_mantenimiento_edited_processed.columns:
                      df_mantenimiento_edited_processed[col] = df_mantenimiento_edited_processed[col].astype(str).str.strip().replace({'': None}).mask(df_mantenimiento_edited_processed[col].isna(), None)
            df_mantenimiento_original_compare = st.session_state.df_gastos_mantenimiento.reindex(columns=expected_cols_mantenimiento).sort_values(by=expected_cols_mantenimiento).reset_index(drop=True)
            df_mantenimiento_edited_compare = df_mantenimiento_edited_processed.reindex(columns=expected_cols_mantenimiento).sort_values(by=expected_cols_mantenimiento).reset_index(drop=True)
            if not df_mantenimiento_edited_compare.equals(df_mantenimiento_original_compare):
                 if st.button("Guardar Cambios en Registros de Mantenimiento", key="save_mantenimiento_button"):
                      df_to_save = df_mantenimiento_edited_processed.copy()
                      date_col_name_mantenimiento = DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO]
                      df_to_save = df_to_save[(df_to_save['Interno'].notna()) & (df_to_save[date_col_name_mantenimiento].notna()) & (df_to_save['Tipo_Mantenimiento'].notna())].copy()
                      if df_to_save.empty and not df_mantenimiento_edited_processed.empty:
                           st.error("Error: Ninguna fila válida. Complete campos obligatorios.")
                      elif df_to_save['Monto_Mantenimiento'].isnull().any():
                           st.error("Error: El campo 'Monto Mantenimiento' no puede estar vacío.")
                      elif (pd.to_numeric(df_to_save['Monto_Mantenimiento'], errors='coerce').fillna(0) <= 0).any():
                           st.warning("Advertencia: Algunos registros tienen 'Monto Mantenimiento' <= 0.")
                      invalid_internos = df_to_save[~df_to_save['Interno'].astype(str).isin(internos_disponibles_set)]['Interno'].unique().tolist()
                      if invalid_internos:
                           st.error(f"Error: Internos no existen: {', '.join(invalid_internos)}.")
                      else:
                           st.session_state.df_gastos_mantenimiento = df_to_save
                           save_table(st.session_state.df_gastos_mantenimiento, DATABASE_FILE, TABLE_GASTOS_MANTENIMIENTO)
                           st.success("Cambios en registros de mantenimiento guardados.")
                           st.experimental_rerun()
                 else:
                     st.info("Hay cambios sin guardar en registros de mantenimiento.")

def page_reportes_mina():
    st.title("Reportes de Mina por Fecha")
    st.write("Genera reportes de consumo y costos por equipo en un rango de fechas.")

    st.subheader("Registrar Precio del Combustible")
    st.info("Edite la tabla siguiente para modificar o eliminar precios existentes.")
    with st.form("form_add_precio_combustible", clear_on_submit=True):
        fecha_precio = st.date_input("Fecha del Precio", value=datetime.date.today(), key="precio_fecha_add")
        precio_litro = st.number_input("Precio por Litro", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="precio_monto_add") # Removed required
        submitted = st.form_submit_button("Registrar Precio")
        if submitted:
            if not fecha_precio:
                st.warning("Por favor, complete la fecha.")
            elif precio_litro is None or precio_litro <= 0: # Added None check
                st.warning("Por favor, complete el precio (mayor a cero).")
            else:
                new_precio_data = {'Fecha': fecha_precio, 'Precio_Litro': float(precio_litro if precio_litro is not None else 0.0)} # Handle None
                new_precio_df = pd.DataFrame([new_precio_data])
                date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
                df_precios_temp = st.session_state.df_precios_combustible.copy()
                if date_col_name_precio not in df_precios_temp.columns:
                     df_precios_temp[date_col_name_precio] = pd.Series(dtype='datetime64[ns]', index=df_precios_temp.index)
                else:
                     df_precios_temp[date_col_name_precio] = pd.to_datetime(df_precios_temp[date_col_name_precio], errors='coerce')
                fecha_precio_dt = pd.to_datetime(fecha_precio, errors='coerce')
                if pd.notna(fecha_precio_dt):
                    df_filtered_for_duplicate = df_precios_temp[
                        df_precios_temp[date_col_name_precio].dt.date != fecha_precio_dt.date()
                    ].copy()
                else:
                    st.warning("Fecha de precio proporcionada no es válida. No se guardará.")
                    st.experimental_rerun()
                    return
                expected_cols_precios = list(TABLE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE].keys())
                new_precio_df = new_precio_df.reindex(columns=expected_cols_precios)
                if date_col_name_precio in new_precio_df.columns:
                     new_precio_df[date_col_name_precio] = pd.to_datetime(new_precio_df[date_col_name_precio], errors='coerce')
                for col, dtype in TABLE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE].items():
                     if col in new_precio_df.columns:
                           try:
                                if dtype == 'object':
                                     new_precio_df[col] = new_precio_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                     new_precio_df[col] = new_precio_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                elif 'float' in dtype: new_precio_df[col] = pd.to_numeric(new_precio_df[col], errors='coerce').astype(float).fillna(0.0)
                                elif 'int' in dtype:
                                     if hasattr(pd, 'Int64Dtype'): new_precio_df[col] = pd.to_numeric(new_precio_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                     else: new_precio_df[col] = pd.to_numeric(new_precio_df[col], errors='coerce').astype(float).fillna(0.0)
                           except Exception as dtype_e:
                                st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                df_filtered_for_duplicate_reindexed = df_filtered_for_duplicate.reindex(columns=expected_cols_precios)
                st.session_state.df_precios_combustible = pd.concat([df_filtered_for_duplicate_reindexed, new_precio_df], ignore_index=True)
                save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE)
                st.success("Precio del combustible registrado/actualizado.")
                st.experimental_rerun()
    st.subheader("Precios del Combustible Existente")
    # ... (rest of page_reportes_mina, no other st.number_input with required)
    if st.session_state.df_precios_combustible.empty:
        st.info("No hay precios de combustible registrados aún.")
    else:
        df_precios_editable = st.session_state.df_precios_combustible.copy()
        date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
        if date_col_name_precio in df_precios_editable.columns:
             df_precios_editable[date_col_name_precio] = pd.to_datetime(df_precios_editable[date_col_name_precio], errors='coerce')
        else:
             df_precios_editable[date_col_name_precio] = pd.Series(dtype='datetime64[ns]', index=df_precios_editable.index)
        expected_cols_precios = list(TABLE_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE].keys())
        df_precios_editable = df_precios_editable.reindex(columns=expected_cols_precios)
        df_precios_edited = st.data_editor(
            df_precios_editable, key="data_editor_precios", num_rows="dynamic",
            column_config={
                date_col_name_precio: st.column_config.DateColumn("Fecha", required=True),
                "Precio_Litro": st.column_config.NumberColumn("Precio por Litro", min_value=0.0, format="%.2f", required=True),
            }
        )
        df_precios_edited_processed = df_precios_edited.copy()
        df_precios_edited_processed = df_precios_edited_processed.reindex(columns=expected_cols_precios)
        df_to_save = df_precios_edited_processed.copy()
        date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
        df_to_save = df_to_save[df_to_save[date_col_name_precio].notna()].copy()
        df_precios_original_compare = st.session_state.df_precios_combustible.reindex(columns=expected_cols_precios).sort_values(by=expected_cols_precios).reset_index(drop=True)
        df_precios_edited_compare = df_to_save.reindex(columns=expected_cols_precios).sort_values(by=expected_cols_precios).reset_index(drop=True)
        if not df_precios_edited_compare.equals(df_precios_original_compare):
             if st.button("Guardar Cambios en Precios de Combustible", key="save_precios_button"):
                  if df_to_save.empty and not df_precios_edited_processed.empty:
                       st.error("Error: Ninguna fila válida. Complete Fecha.")
                  elif df_to_save[date_col_name_precio].duplicated().any():
                       st.error("Error: Fechas duplicadas en precios. Cada fecha debe tener un único precio.")
                  elif df_to_save['Precio_Litro'].isnull().any():
                        st.error("Error: El campo 'Precio por Litro' no puede estar vacío.")
                  elif (pd.to_numeric(df_to_save['Precio_Litro'], errors='coerce').fillna(0) <= 0).any():
                        st.error("Error: El 'Precio por Litro' debe ser mayor a cero.")
                  else:
                       st.session_state.df_precios_combustible = df_to_save
                       save_table(st.session_state.df_precios_combustible, DATABASE_FILE, TABLE_PRECIOS_COMBUSTIBLE)
                       st.success("Cambios en precios de combustible guardados.")
                       st.experimental_rerun()
             else:
                 st.info("Hay cambios sin guardar en precios de combustible.")

    st.subheader("Reporte por Rango de Fechas")
    # ... (The rest of page_reportes_mina uses complex logic but no st.number_input directly with 'required')
    # This section involves data filtering and aggregation for reports, so no direct 'required' issue.
    # For brevity, I'll skip pasting this large reporting section as it's unaffected by the primary error.
    # However, since "complete code" was requested, I'll include it.
    col1, col2 = st.columns(2)
    all_relevant_dates = pd.Series(dtype='datetime64[ns]')
    for table_name, date_col in DATETIME_COLUMNS.items():
         df_temp = st.session_state.get(f'df_{table_name.lower()}', pd.DataFrame())
         if date_col in df_temp.columns and not df_temp.empty:
              all_relevant_dates = pd.concat([all_relevant_dates, pd.to_datetime(df_temp[date_col], errors='coerce')])
    all_relevant_dates = all_relevant_dates.dropna()
    if not all_relevant_dates.empty:
        min_app_date = all_relevant_dates.min().date()
        max_app_date = all_relevant_dates.max().date()
        today = datetime.date.today()
        default_end = min(today, max_app_date)
        default_start = max(default_end - pd.Timedelta(days=30), min_app_date)
        default_end = max(default_end, default_start)
    else:
        today = datetime.date.today()
        min_app_date = today - pd.Timedelta(days=365 * 5)
        max_app_date = today
        default_start = today - pd.Timedelta(days=30)
        default_end = today
    min_date_input_display = all_relevant_dates.min().date() if not all_relevant_dates.empty else datetime.date.today() - pd.Timedelta(days=365 * 5)
    max_date_input_display = all_relevant_dates.max().date() if not all_relevant_dates.empty else datetime.date.today()
    with col1:
        fecha_inicio = st.date_input("Fecha de Inicio del Reporte", default_start, min_value=min_date_input_display, max_value=max_date_input_display, key="reporte_fecha_inicio")
    with col2:
        fecha_fin = st.date_input("Fecha de Fin del Reporte", default_end, min_value=min_date_input_display, max_value=max_date_input_display, key="reporte_fecha_fin")

    if st.button("Generar Reporte", key="generate_reporte_button"):
        if fecha_inicio > fecha_fin:
            st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
            return
        start_ts = pd.Timestamp(fecha_inicio).normalize()
        end_ts = pd.Timestamp(fecha_fin) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        def filter_df_by_date(df_original, date_col_name, start_ts, end_ts, expected_cols_dict):
             if df_original.empty or date_col_name not in df_original.columns or not expected_cols_dict:
                  empty_df = pd.DataFrame(columns=expected_cols_dict.keys())
                  for col, dtype in expected_cols_dict.items():
                       if dtype == 'object': empty_df[col] = pd.Series(dtype=pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                       elif 'float' in dtype: empty_df[col] = pd.Series(dtype=float)
                       elif 'int' in dtype: empty_df[col] = pd.Series(dtype=PANDAS_INT_DTYPE)
                  return empty_df
             df_temp = df_original.copy()
             df_temp['Date_dt'] = pd.to_datetime(df_temp.get(date_col_name), errors='coerce')
             df_filtered = df_temp[df_temp['Date_dt'].notna() & (df_temp['Date_dt'] >= start_ts) & (df_temp['Date_dt'] <= end_ts)].copy()
             df_filtered = df_filtered.drop(columns=['Date_dt'])
             df_filtered = df_filtered.reindex(columns=expected_cols_dict.keys())
             for col, dtype in expected_cols_dict.items():
                  if col in df_filtered.columns:
                       try:
                            if dtype == 'object':
                                 df_filtered[col] = df_filtered[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                            elif 'float' in dtype:
                                 df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype(float)
                            elif 'int' in dtype:
                                 if hasattr(pd, 'Int64Dtype'): df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype(pd.Int64Dtype())
                                 else: df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype(float)
                       except Exception:
                            pass
             if date_col_name in df_filtered.columns:
                  df_filtered[date_col_name] = pd.to_datetime(df_filtered[date_col_name], errors='coerce')
             return df_filtered
        df_consumo_filtered = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts, end_ts, TABLE_COLUMNS.get(TABLE_CONSUMO, {}))
        df_precios_filtered = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts, end_ts, TABLE_COLUMNS.get(TABLE_PRECIOS_COMBUSTIBLE, {}))
        df_salarial_filtered = filter_df_by_date(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], start_ts, end_ts, TABLE_COLUMNS.get(TABLE_COSTOS_SALARIAL, {}))
        df_fijos_filtered = filter_df_by_date(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], start_ts, end_ts, TABLE_COLUMNS.get(TABLE_GASTOS_FIJOS, {}))
        df_mantenimiento_filtered = filter_df_by_date(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], start_ts, end_ts, TABLE_COLUMNS.get(TABLE_GASTOS_MANTENIMIENTO, {}))

        if df_consumo_filtered.empty:
            st.info("No hay datos de consumo en el rango de fechas seleccionado.")
            reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Patente', 'ID_Flota', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'])
        else:
             for col in ['Consumo_Litros', 'Horas_Trabajadas', 'Kilometros_Recorridos']:
                 if col in df_consumo_filtered.columns:
                     df_consumo_filtered[col] = pd.to_numeric(df_consumo_filtered[col], errors='coerce').fillna(0.0)
                 else:
                      df_consumo_filtered[col] = 0.0
             date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
             date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
             consumo_for_merge = df_consumo_filtered.dropna(subset=[date_col_name_consumo]).sort_values(date_col_name_consumo).copy()
             precios_for_merge = df_precios_filtered.dropna(subset=[date_col_name_precio, 'Precio_Litro']).drop_duplicates(subset=[date_col_name_precio]).sort_values(date_col_name_precio).copy()
             if not precios_for_merge.empty and date_col_name_precio in precios_for_merge.columns and 'Precio_Litro' in precios_for_merge.columns:
                 consumo_for_merge[date_col_name_consumo] = pd.to_datetime(consumo_for_merge[date_col_name_consumo], errors='coerce')
                 precios_for_merge[date_col_name_precio] = pd.to_datetime(precios_for_merge[date_col_name_precio], errors='coerce')
                 consumo_merged = pd.merge_asof(consumo_for_merge, precios_for_merge[[date_col_name_precio, 'Precio_Litro']], left_on=date_col_name_consumo, right_on=date_col_name_precio, direction='backward', suffixes=('_consumo', '_precio'))
                 price_col_after_merge = 'Precio_Litro_precio' if 'Precio_Litro_precio' in consumo_merged.columns else 'Precio_Litro'
                 if price_col_after_merge in consumo_merged.columns:
                      consumo_merged['Precio_Litro'] = pd.to_numeric(consumo_merged[price_col_after_merge], errors='coerce').fillna(0.0)
                      if price_col_after_merge != 'Precio_Litro':
                          consumo_merged = consumo_merged.drop(columns=[price_col_after_merge])
                 else:
                      consumo_merged['Precio_Litro'] = 0.0
                 if date_col_name_consumo + '_consumo' in consumo_merged.columns:
                     consumo_merged = consumo_merged.rename(columns={date_col_name_consumo + '_consumo': date_col_name_consumo})
                 if date_col_name_precio in consumo_merged.columns and date_col_name_precio != date_col_name_consumo:
                     consumo_merged = consumo_merged.drop(columns=[date_col_name_precio])
             else:
                  consumo_merged = consumo_for_merge.copy()
                  consumo_merged['Precio_Litro'] = 0.0
             reporte_consumo_detail = consumo_merged.copy()
             if 'Consumo_Litros' not in reporte_consumo_detail.columns: reporte_consumo_detail['Consumo_Litros'] = 0.0
             reporte_consumo_detail['Consumo_Litros'] = pd.to_numeric(reporte_consumo_detail['Consumo_Litros'], errors='coerce').fillna(0.0)
             reporte_consumo_detail['Costo_Combustible'] = reporte_consumo_detail['Consumo_Litros'] * reporte_consumo_detail['Precio_Litro']
             if 'Interno' in reporte_consumo_detail.columns:
                 reporte_consumo_detail['Interno'] = reporte_consumo_detail['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_consumo_detail['Interno'].isna(), None)
                 reporte_consumo_detail_valid_interno = reporte_consumo_detail.dropna(subset=['Interno']).copy()
                 if not reporte_consumo_detail_valid_interno.empty:
                      reporte_resumen_consumo = reporte_consumo_detail_valid_interno.groupby('Interno', dropna=True).agg(
                          Total_Consumo_Litros=('Consumo_Litros', 'sum'), Total_Horas=('Horas_Trabajadas', 'sum'),
                          Total_Kilometros=('Kilometros_Recorridos', 'sum'), Costo_Total_Combustible=('Costo_Combustible', 'sum')
                      ).reset_index()
                 else:
                      st.info("No hay datos de consumo válidos en el rango de fechas.")
                      reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Costo_Total_Combustible'])
             else:
                 st.warning("La tabla de consumo filtrada no contiene 'Interno'.")
                 reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Costo_Total_Combustible'])
             if 'Interno' in reporte_resumen_consumo.columns and not reporte_resumen_consumo.empty:
                 for col in ['Total_Horas', 'Total_Kilometros', 'Total_Consumo_Litros']:
                      if col in reporte_resumen_consumo.columns:
                           reporte_resumen_consumo[col] = pd.to_numeric(reporte_resumen_consumo[col], errors='coerce').fillna(0.0)
                      else:
                           reporte_resumen_consumo[col] = 0.0
                 reporte_resumen_consumo['Avg_Consumo_L_H'] = np.divide(
                     reporte_resumen_consumo['Total_Consumo_Litros'], reporte_resumen_consumo['Total_Horas'],
                     out=np.zeros_like(reporte_resumen_consumo['Total_Consumo_Litros'], dtype=float), where=reporte_resumen_consumo['Total_Horas'] != 0
                 )
                 reporte_resumen_consumo['Avg_Consumo_L_KM'] = np.divide(
                     reporte_resumen_consumo['Total_Consumo_Litros'], reporte_resumen_consumo['Total_Kilometros'],
                     out=np.zeros_like(reporte_resumen_consumo['Total_Consumo_Litros'], dtype=float), where=reporte_resumen_consumo['Total_Kilometros'] != 0
                 )
                 df_equipos_for_merge = st.session_state.get('df_equipos', pd.DataFrame())
                 if 'Interno' in df_equipos_for_merge.columns:
                      df_equipos_for_merge = df_equipos_for_merge[['Interno', 'Patente', 'ID_Flota']].copy()
                      df_equipos_for_merge['Interno'] = df_equipos_for_merge['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_equipos_for_merge['Interno'].isna(), None)
                      df_equipos_for_merge = df_equipos_for_merge.dropna(subset=['Interno'])
                      reporte_resumen_consumo['Interno_str_for_merge'] = reporte_resumen_consumo['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_resumen_consumo['Interno'].isna(), None)
                      reporte_resumen_consumo = reporte_resumen_consumo.merge(df_equipos_for_merge[['Interno', 'Patente', 'ID_Flota']], left_on='Interno_str_for_merge', right_on='Interno', how='left', suffixes=('', '_equipo_merge'))
                      reporte_resumen_consumo['Patente'] = reporte_resumen_consumo.get('Patente_equipo_merge', pd.Series(dtype='object', index=reporte_resumen_consumo.index)).fillna('Sin Patente').astype(str).str.strip().replace({'': 'Sin Patente', 'nan': 'Sin Patente', 'None': 'Sin Patente'})
                      reporte_resumen_consumo['ID_Flota'] = reporte_resumen_consumo.get('ID_Flota_equipo_merge', pd.Series(dtype='object', index=reporte_resumen_consumo.index)).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_resumen_consumo.get('ID_Flota_equipo_merge', pd.Series(dtype='object', index=reporte_resumen_consumo.index)).isna(), None)
                      reporte_resumen_consumo = reporte_resumen_consumo.drop(columns=['Interno_str_for_merge', 'Interno_equipo_merge', 'Patente_equipo_merge', 'ID_Flota_equipo_merge'], errors='ignore')
                      df_flotas_for_merge = st.session_state.get('df_flotas', pd.DataFrame())
                      if 'ID_Flota' in df_flotas_for_merge.columns:
                           df_flotas_for_merge = df_flotas_for_merge[['ID_Flota', 'Nombre_Flota']].copy()
                           df_flotas_for_merge['ID_Flota'] = df_flotas_for_merge['ID_Flota'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_flotas_for_merge['ID_Flota'].isna(), None)
                           df_flotas_for_merge = df_flotas_for_merge.dropna(subset=['ID_Flota'])
                           reporte_resumen_consumo['ID_Flota_str_for_merge_flota'] = reporte_resumen_consumo['ID_Flota'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_resumen_consumo['ID_Flota'].isna(), None)
                           reporte_resumen_consumo = reporte_resumen_consumo.merge(df_flotas_for_merge[['ID_Flota', 'Nombre_Flota']], left_on='ID_Flota_str_for_merge_flota', right_on='ID_Flota', how='left', suffixes=('', '_flota_merge'))
                           reporte_resumen_consumo['Nombre_Flota'] = reporte_resumen_consumo.get('Nombre_Flota_flota_merge', pd.Series(dtype='object', index=reporte_resumen_consumo.index)).fillna('Sin Flota')
                           reporte_resumen_consumo = reporte_resumen_consumo.drop(columns=['ID_Flota_str_for_merge_flota', 'ID_Flota_flota_merge'], errors='ignore')
                      else:
                           reporte_resumen_consumo['Nombre_Flota'] = 'Sin Datos de Flota'
                 else:
                     st.warning("La tabla de equipos no contiene 'Interno'.")
                     reporte_resumen_consumo['Patente'] = 'Sin Datos Equipo'
                     reporte_resumen_consumo['Nombre_Flota'] = 'Sin Datos Equipo'
                     reporte_resumen_consumo['ID_Flota'] = pd.NA
                 expected_display_cols_consumo = ['Interno', 'Patente', 'Nombre_Flota', 'ID_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible']
                 for col in expected_display_cols_consumo:
                      if col not in reporte_resumen_consumo.columns:
                           reporte_resumen_consumo[col] = pd.NA
                 st.subheader(f"Reporte Consumo y Costo Combustible ({fecha_inicio} a {fecha_fin})")
                 st.dataframe(reporte_resumen_consumo[expected_display_cols_consumo].round(2))
             else:
                 st.info("No hay datos de consumo válidos en el rango de fechas.")
                 reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Patente', 'ID_Flota', 'Nombre_Flota', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros', 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'])

        salarial_agg = pd.DataFrame(columns=['Interno', 'Total_Salarial'])
        if 'Interno' in df_salarial_filtered.columns and 'Monto_Salarial' in df_salarial_filtered.columns:
            df_salarial_filtered_clean = df_salarial_filtered.copy()
            df_salarial_filtered_clean['Interno'] = df_salarial_filtered_clean['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_salarial_filtered_clean['Interno'].isna(), None)
            df_salarial_filtered_clean['Monto_Salarial'] = pd.to_numeric(df_salarial_filtered_clean['Monto_Salarial'], errors='coerce').fillna(0.0)
            salarial_agg = df_salarial_filtered_clean.dropna(subset=['Interno']).groupby('Interno', dropna=True)['Monto_Salarial'].sum().reset_index(name='Total_Salarial')
        fijos_agg = pd.DataFrame(columns=['Interno', 'Total_Gastos_Fijos'])
        if 'Interno' in df_fijos_filtered.columns and 'Monto_Gasto_Fijo' in df_fijos_filtered.columns:
             df_fijos_filtered_clean = df_fijos_filtered.copy()
             df_fijos_filtered_clean['Interno'] = df_fijos_filtered_clean['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_fijos_filtered_clean['Interno'].isna(), None)
             df_fijos_filtered_clean['Monto_Gasto_Fijo'] = pd.to_numeric(df_fijos_filtered_clean['Monto_Gasto_Fijo'], errors='coerce').fillna(0.0)
             fijos_agg = df_fijos_filtered_clean.dropna(subset=['Interno']).groupby('Interno', dropna=True)['Monto_Gasto_Fijo'].sum().reset_index(name='Total_Gastos_Fijos')
        mantenimiento_agg = pd.DataFrame(columns=['Interno', 'Total_Gastos_Mantenimiento'])
        if 'Interno' in df_mantenimiento_filtered.columns and 'Monto_Mantenimiento' in df_mantenimiento_filtered.columns:
             df_mantenimiento_filtered_clean = df_mantenimiento_filtered.copy()
             df_mantenimiento_filtered_clean['Interno'] = df_mantenimiento_filtered_clean['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_mantenimiento_filtered_clean['Interno'].isna(), None)
             df_mantenimiento_filtered_clean['Monto_Mantenimiento'] = pd.to_numeric(df_mantenimiento_filtered_clean['Monto_Mantenimiento'], errors='coerce').fillna(0.0)
             mantenimiento_agg = df_mantenimiento_filtered_clean.dropna(subset=['Interno']).groupby('Interno', dropna=True)['Monto_Mantenimiento'].sum().reset_index(name='Total_Gastos_Mantenimiento')
        all_internos_series_list = [
            df_consumo_filtered.get('Interno', pd.Series(dtype='object')).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}),
            df_salarial_filtered.get('Interno', pd.Series(dtype='object')).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}),
            df_fijos_filtered.get('Interno', pd.Series(dtype='object')).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}),
            df_mantenimiento_filtered.get('Interno', pd.Series(dtype='object')).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}),
        ]
        all_internos_in_period = pd.concat(all_internos_series_list).dropna().unique().tolist()
        if not all_internos_in_period:
             st.info("No hay datos de costos en el rango de fechas para ningún equipo.")
        else:
             df_all_internos = pd.DataFrame({'Interno': all_internos_in_period})
             df_all_internos['Interno'] = df_all_internos['Interno'].astype(str)
             df_equipos_for_merge = st.session_state.get('df_equipos', pd.DataFrame())
             if 'Interno' in df_equipos_for_merge.columns:
                  df_equipos_for_merge = df_equipos_for_merge[['Interno', 'Patente', 'ID_Flota']].copy()
                  df_equipos_for_merge['Interno'] = df_equipos_for_merge['Interno'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_equipos_for_merge['Interno'].isna(), None)
                  df_equipos_for_merge = df_equipos_for_merge.dropna(subset=['Interno'])
                  reporte_costo_total = df_all_internos.merge(df_equipos_for_merge, on='Interno', how='left')
                  reporte_costo_total['Patente'] = reporte_costo_total.get('Patente', pd.Series(dtype='object', index=reporte_costo_total.index)).fillna('Sin Patente').astype(str).str.strip().replace({'': 'Sin Patente', 'nan': 'Sin Patente', 'None': 'Sin Patente'})
                  reporte_costo_total['ID_Flota'] = reporte_costo_total.get('ID_Flota', pd.Series(dtype='object', index=reporte_costo_total.index)).astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_costo_total.get('ID_Flota', pd.Series(dtype='object', index=reporte_costo_total.index)).isna(), None)
                  df_flotas_for_merge = st.session_state.get('df_flotas', pd.DataFrame())
                  if 'ID_Flota' in df_flotas_for_merge.columns:
                       df_flotas_for_merge = df_flotas_for_merge[['ID_Flota', 'Nombre_Flota']].copy()
                       df_flotas_for_merge['ID_Flota'] = df_flotas_for_merge['ID_Flota'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_flotas_for_merge['ID_Flota'].isna(), None)
                       df_flotas_for_merge = df_flotas_for_merge.dropna(subset=['ID_Flota'])
                       reporte_costo_total['ID_Flota_str_for_merge_flota'] = reporte_costo_total['ID_Flota'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(reporte_costo_total['ID_Flota'].isna(), None)
                       reporte_costo_total = reporte_costo_total.merge(df_flotas_for_merge[['ID_Flota', 'Nombre_Flota']], left_on='ID_Flota_str_for_merge_flota', right_on='ID_Flota', how='left', suffixes=('', '_flota_merge'))
                       reporte_costo_total['Nombre_Flota'] = reporte_costo_total.get('Nombre_Flota_flota_merge', pd.Series(dtype='object', index=reporte_costo_total.index)).fillna('Sin Flota')
                       reporte_costo_total = reporte_costo_total.drop(columns=['ID_Flota_str_for_merge_flota', 'ID_Flota_flota_merge'], errors='ignore')
                  else:
                       reporte_costo_total['Nombre_Flota'] = 'Sin Datos de Flota'
             else:
                 st.warning("La tabla de equipos no contiene 'Interno'.")
                 reporte_costo_total = df_all_internos.copy()
                 reporte_costo_total['Patente'] = 'Sin Datos Equipo'
                 reporte_costo_total['Nombre_Flota'] = 'Sin Datos Equipo'
                 reporte_costo_total['ID_Flota'] = pd.NA
             reporte_costo_total = reporte_costo_total.merge(reporte_resumen_consumo[['Interno', 'Costo_Total_Combustible']].astype({'Interno':'str'}), on='Interno', how='left').fillna({'Costo_Total_Combustible': 0.0})
             reporte_costo_total = reporte_costo_total.merge(salarial_agg.astype({'Interno':'str'}), on='Interno', how='left').fillna({'Total_Salarial': 0.0})
             reporte_costo_total = reporte_costo_total.merge(fijos_agg.astype({'Interno':'str'}), on='Interno', how='left').fillna({'Total_Gastos_Fijos': 0.0})
             reporte_costo_total = reporte_costo_total.merge(mantenimiento_agg.astype({'Interno':'str'}), on='Interno', how='left').fillna({'Total_Gastos_Mantenimiento': 0.0})
             cost_cols = ['Costo_Total_Combustible', 'Total_Salarial', 'Total_Gastos_Fijos', 'Total_Gastos_Mantenimiento']
             for col in cost_cols:
                  if col not in reporte_costo_total.columns:
                      reporte_costo_total[col] = 0.0
                  else:
                      reporte_costo_total[col] = pd.to_numeric(reporte_costo_total[col], errors='coerce').fillna(0.0)
             reporte_costo_total['Costo_Total_Equipo'] = reporte_costo_total[cost_cols].sum(axis=1)
             expected_display_cols_total_cost = ['Interno', 'Patente', 'Nombre_Flota', 'ID_Flota'] + cost_cols + ['Costo_Total_Equipo']
             for col in expected_display_cols_total_cost:
                 if col not in reporte_costo_total.columns:
                      reporte_costo_total[col] = pd.NA
             st.subheader(f"Reporte Costo Total por Equipo ({fecha_inicio} a {fecha_fin})")
             if reporte_costo_total.empty:
                 st.info("No hay datos de costos en el rango de fechas para ningún equipo.")
             else:
                 st.dataframe(reporte_costo_total[expected_display_cols_total_cost].round(2))

# ... (page_variacion_costos_flota has no st.number_input with required, safe to skip for brevity unless full code strictly needed)
# ... (page_gestion_obras has no st.number_input with required in its direct form, data_editor is used)
# ... (page_reporte_presupuesto_total_obras has no st.number_input with required)

def page_variacion_costos_flota():
    st.title("Variación de Costos de Flota (Gráfico de Cascada)")
    # This page uses date inputs and button, then calculations. No direct st.number_input with 'required'.
    # For brevity, skipping pasting the full function unless strictly required.
    # It was included in the previous response, so I'll include it again.
    st.write("Compara los costos totales de la flota entre dos períodos para visualizar la variación.")
    st.subheader("Seleccione Períodos a Comparar")
    col1, col2, col3, col4 = st.columns(4)
    all_relevant_dates = pd.Series(dtype='datetime64[ns]')
    for table_name, date_col in DATETIME_COLUMNS.items():
         df_temp = st.session_state.get(f'df_{table_name.lower()}', pd.DataFrame())
         if date_col in df_temp.columns and not df_temp.empty:
              all_relevant_dates = pd.concat([all_relevant_dates, pd.to_datetime(df_temp[date_col], errors='coerce')])
    all_relevant_dates = all_relevant_dates.dropna()
    if not all_relevant_dates.empty:
        min_app_date = all_relevant_dates.min().date()
        max_app_date = all_relevant_dates.max().date()
        today = datetime.date.today()
        default_end_p2 = min(today, max_app_date)
        default_start_p2 = max(default_end_p2 - pd.Timedelta(days=30), min_app_date)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = max(default_end_p1 - pd.Timedelta(days=30), min_app_date)
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
        default_start_p1 = max(default_start_p1, min_date_input_display)
        default_end_p1 = max(default_end_p1, min_date_input_display)
        default_start_p2 = max(default_start_p2, min_date_input_display)
        default_end_p2 = max(default_end_p2, min_date_input_display)
        default_end_p1 = max(default_end_p1, default_start_p1)
        default_end_p2 = max(default_end_p2, default_start_p2)
    min_date_input_display = all_relevant_dates.min().date() if not all_relevant_dates.empty else datetime.date.today() - pd.Timedelta(days=365 * 5)
    max_date_input_display = all_relevant_dates.max().date() if not all_relevant_dates.empty else datetime.date.today()
    with col1:
        fecha_inicio_p1 = st.date_input("Inicio Período 1", default_start_p1, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_inicio_p1")
    with col2:
        fecha_fin_p1 = st.date_input("Fin Período 1", default_end_p1, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_fin_p1")
    with col3:
        fecha_inicio_p2 = st.date_input("Inicio Período 2", default_start_p2, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_inicio_p2")
    with col4:
        fecha_fin_p2 = st.date_input("Fin Período 2", default_end_p2, min_value=min_date_input_display, max_value=max_date_input_display, key="fecha_fin_p2")
    if fecha_inicio_p1 > fecha_fin_p1 or fecha_inicio_p2 > fecha_fin_p2:
         st.error("Las fechas de los períodos no son válidas.")
    elif not (fecha_fin_p1 < fecha_inicio_p2 or fecha_fin_p2 < fecha_inicio_p1 or (fecha_inicio_p1 == fecha_inicio_p2 and fecha_fin_p1 == fecha_fin_p2)):
         st.warning("Advertencia: Los períodos seleccionados se solapan o no están en orden.")
    if st.button("Generar Gráfico de Cascada", key="generate_waterfall_button"):
        def aggregate_cost_column(df_original, date_col_name, cost_col_name, start_ts, end_ts, expected_cols_dict):
            if df_original.empty or date_col_name not in df_original.columns or cost_col_name not in df_original.columns:
                 return 0.0
            df_temp = df_original.copy()
            df_temp['Date_dt'] = pd.to_datetime(df_temp.get(date_col_name), errors='coerce')
            df_temp[cost_col_name] = pd.to_numeric(df_temp.get(cost_col_name, pd.Series(0.0, index=df_temp.index)), errors='coerce').fillna(0.0)
            df_filtered = df_temp[df_temp['Date_dt'].notna() & (df_temp['Date_dt'] >= start_ts) & (df_temp['Date_dt'] <= end_ts)].copy()
            return df_filtered[cost_col_name].sum()
        start_ts_p1 = pd.Timestamp(fecha_inicio_p1).normalize()
        end_ts_p1 = pd.Timestamp(fecha_fin_p1) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        start_ts_p2 = pd.Timestamp(fecha_inicio_p2).normalize()
        end_ts_p2 = pd.Timestamp(fecha_fin_p2) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        consumo_p1_filtered_dt = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts_p1, end_ts_p1, TABLE_COLUMNS.get(TABLE_CONSUMO, {}))
        precios_p1_filtered_dt = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts_p1, end_ts_p1, TABLE_COLUMNS.get(TABLE_PRECIOS_COMBUSTIBLE, {}))
        costo_combustible_p1 = 0
        date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
        date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
        if not consumo_p1_filtered_dt.empty and not precios_p1_filtered_dt.empty and date_col_name_consumo in consumo_p1_filtered_dt.columns and date_col_name_precio in precios_p1_filtered_dt.columns and 'Consumo_Litros' in consumo_p1_filtered_dt.columns and 'Precio_Litro' in precios_p1_filtered_dt.columns:
             consumo_p1_filtered_dt[date_col_name_consumo] = pd.to_datetime(consumo_p1_filtered_dt[date_col_name_consumo], errors='coerce')
             precios_p1_filtered_dt[date_col_name_precio] = pd.to_datetime(precios_p1_filtered_dt[date_col_name_precio], errors='coerce')
             consumo_p1_sorted = consumo_p1_filtered_dt.dropna(subset=[date_col_name_consumo]).sort_values(date_col_name_consumo).copy()
             precios_p1_sorted = precios_p1_filtered_dt.dropna(subset=[date_col_name_precio, 'Precio_Litro']).drop_duplicates(subset=[date_col_name_precio]).sort_values(date_col_name_precio).copy()
             if not consumo_p1_sorted.empty and not precios_p1_sorted.empty:
                  consumo_merged = pd.merge_asof(consumo_p1_sorted, precios_p1_sorted[[date_col_name_precio, 'Precio_Litro']], left_on=date_col_name_consumo, right_on=date_col_name_precio, direction='backward', suffixes=('_consumo', '_precio'))
                  price_col_after_merge = 'Precio_Litro_precio' if 'Precio_Litro_precio' in consumo_merged.columns else 'Precio_Litro'
                  if price_col_after_merge in consumo_merged.columns and 'Consumo_Litros' in consumo_merged.columns:
                       consumo_merged['Consumo_Litros'] = pd.to_numeric(consumo_merged['Consumo_Litros'], errors='coerce').fillna(0.0)
                       consumo_merged[price_col_after_merge] = pd.to_numeric(consumo_merged[price_col_after_merge], errors='coerce').fillna(0.0)
                       costo_combustible_p1 = (consumo_merged['Consumo_Litros'] * consumo_merged[price_col_after_merge]).sum()
                  consumo_merged = consumo_merged.loc[:,~consumo_merged.columns.duplicated()].copy()
        costo_salarial_p1 = aggregate_cost_column(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], 'Monto_Salarial', start_ts_p1, end_ts_p1, TABLE_COLUMNS.get(TABLE_COSTOS_SALARIAL, {}))
        costo_fijos_p1 = aggregate_cost_column(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], 'Monto_Gasto_Fijo', start_ts_p1, end_ts_p1, TABLE_COLUMNS.get(TABLE_GASTOS_FIJOS, {}))
        costo_mantenimiento_p1 = aggregate_cost_column(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], 'Monto_Mantenimiento', start_ts_p1, end_ts_p1, TABLE_COLUMNS.get(TABLE_GASTOS_MANTENIMIENTO, {}))
        total_costo_p1 = costo_combustible_p1 + costo_salarial_p1 + costo_fijos_p1 + costo_mantenimiento_p1
        consumo_p2_filtered_dt = filter_df_by_date(st.session_state.df_consumo, DATETIME_COLUMNS[TABLE_CONSUMO], start_ts_p2, end_ts_p2, TABLE_COLUMNS.get(TABLE_CONSUMO, {}))
        precios_p2_filtered_dt = filter_df_by_date(st.session_state.df_precios_combustible, DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE], start_ts_p2, end_ts_p2, TABLE_COLUMNS.get(TABLE_PRECIOS_COMBUSTIBLE, {}))
        costo_combustible_p2 = 0
        date_col_name_consumo = DATETIME_COLUMNS[TABLE_CONSUMO]
        date_col_name_precio = DATETIME_COLUMNS[TABLE_PRECIOS_COMBUSTIBLE]
        if not consumo_p2_filtered_dt.empty and not precios_p2_filtered_dt.empty and date_col_name_consumo in consumo_p2_filtered_dt.columns and date_col_name_precio in precios_p2_filtered_dt.columns and 'Consumo_Litros' in consumo_p2_filtered_dt.columns and 'Precio_Litro' in precios_p2_filtered_dt.columns:
             consumo_p2_filtered_dt[date_col_name_consumo] = pd.to_datetime(consumo_p2_filtered_dt[date_col_name_consumo], errors='coerce')
             precios_p2_filtered_dt[date_col_name_precio] = pd.to_datetime(precios_p2_filtered_dt[date_col_name_precio], errors='coerce')
             consumo_p2_sorted = consumo_p2_filtered_dt.dropna(subset=[date_col_name_consumo]).sort_values(date_col_name_consumo).copy()
             precios_p2_sorted = precios_p2_filtered_dt.dropna(subset=[date_col_name_precio, 'Precio_Litro']).drop_duplicates(subset=[date_col_name_precio]).sort_values(date_col_name_precio).copy()
             if not consumo_p2_sorted.empty and not precios_p2_sorted.empty:
                  consumo_merged = pd.merge_asof(consumo_p2_sorted, precios_p2_sorted[[date_col_name_precio, 'Precio_Litro']], left_on=date_col_name_consumo, right_on=date_col_name_precio, direction='backward', suffixes=('_consumo', '_precio'))
                  price_col_after_merge = 'Precio_Litro_precio' if 'Precio_Litro_precio' in consumo_merged.columns else 'Precio_Litro'
                  if price_col_after_merge in consumo_merged.columns and 'Consumo_Litros' in consumo_merged.columns:
                       consumo_merged['Consumo_Litros'] = pd.to_numeric(consumo_merged['Consumo_Litros'], errors='coerce').fillna(0.0)
                       consumo_merged[price_col_after_merge] = pd.to_numeric(consumo_merged[price_col_after_merge], errors='coerce').fillna(0.0)
                       costo_combustible_p2 = (consumo_merged['Consumo_Litros'] * consumo_merged[price_col_after_merge]).sum()
                  consumo_merged = consumo_merged.loc[:,~consumo_merged.columns.duplicated()].copy()
        costo_salarial_p2 = aggregate_cost_column(st.session_state.df_costos_salarial, DATETIME_COLUMNS[TABLE_COSTOS_SALARIAL], 'Monto_Salarial', start_ts_p2, end_ts_p2, TABLE_COLUMNS.get(TABLE_COSTOS_SALARIAL, {}))
        costo_fijos_p2 = aggregate_cost_column(st.session_state.df_gastos_fijos, DATETIME_COLUMNS[TABLE_GASTOS_FIJOS], 'Monto_Gasto_Fijo', start_ts_p2, end_ts_p2, TABLE_COLUMNS.get(TABLE_GASTOS_FIJOS, {}))
        costo_mantenimiento_p2 = aggregate_cost_column(st.session_state.df_gastos_mantenimiento, DATETIME_COLUMNS[TABLE_GASTOS_MANTENIMIENTO], 'Monto_Mantenimiento', start_ts_p2, end_ts_p2, TABLE_COLUMNS.get(TABLE_GASTOS_MANTENIMIENTO, {}))
        total_costo_p2 = costo_combustible_p2 + costo_salarial_p2 + costo_fijos_p2 + costo_mantenimiento_p2
        labels = [f'Total Costo<br>P1<br>({fecha_inicio_p1.strftime("%Y-%m-%d")} a {fecha_fin_p1.strftime("%Y-%m-%d")})']
        measures = ['absolute']
        values = [total_costo_p1]
        texts = [f"${total_costo_p1:,.2f}"]
        variacion_combustible = costo_combustible_p2 - costo_combustible_p1
        variacion_salarial = costo_salarial_p2 - costo_salarial_p1
        variacion_fijos = costo_fijos_p2 - costo_fijos_p1
        variacion_mantenimiento = costo_mantenimiento_p2 - costo_mantenimiento_p1
        variacion_total = total_costo_p2 - total_costo_p1
        variation_threshold = 0.01
        variation_data = []
        if 'Consumo_Litros' in TABLE_COLUMNS.get(TABLE_CONSUMO, {}) and 'Precio_Litro' in TABLE_COLUMNS.get(TABLE_PRECIOS_COMBUSTIBLE, {}) and abs(variacion_combustible) >= variation_threshold: variation_data.append({'label': 'Var. Combustible', 'value': variacion_combustible})
        if 'Monto_Salarial' in TABLE_COLUMNS.get(TABLE_COSTOS_SALARIAL, {}) and abs(variacion_salarial) >= variation_threshold: variation_data.append({'label': 'Var. Salarial', 'value': variacion_salarial})
        if 'Monto_Gasto_Fijo' in TABLE_COLUMNS.get(TABLE_GASTOS_FIJOS, {}) and abs(variacion_fijos) >= variation_threshold: variation_data.append({'label': 'Var. Fijos', 'value': variacion_fijos})
        if 'Monto_Mantenimiento' in TABLE_COLUMNS.get(TABLE_GASTOS_MANTENIMIENTO, {}) and abs(variacion_mantenimiento) >= variation_threshold: variation_data.append({'label': 'Var. Mantenimiento', 'value': variacion_mantenimiento})
        variation_data.sort(key=lambda x: x['value'], reverse=True)
        for item in variation_data:
            labels.append(item['label'])
            measures.append('relative')
            values.append(item['value'])
            texts.append(f"${item['value']:,.2f}")
        labels.append(f'Total Costo<br>P2<br>({fecha_inicio_p2.strftime("%Y-%m-%d")} a {fecha_fin_p2.strftime("%Y-%m-%d")})')
        measures.append('total')
        values.append(total_costo_p2)
        texts.append(f"${total_costo_p2:,.2f}")
        if (len(labels) > 2) or (len(labels) == 2 and abs(values[0] - values[1]) >= variation_threshold) or (len(labels) == 2 and abs(values[0]) >= variation_threshold):
             fig = go.Figure(go.Waterfall(
                 name = "Variación de Costos", orientation = "v", measure = measures, x = labels,
                 textposition = "outside", text = texts, y = values, connector = {"line":{"color":"rgb(63, 63, 63)"}},
                 increasing = {"marker":{"color":"#FF4136"}}, decreasing = {"marker":{"color":"#3D9970"}},
                 totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
             ))
             fig.update_layout(
                 title = f'Variación de Costos de Flota: {fecha_inicio_p1.strftime("%Y-%m-%d")} a {fecha_fin_p1.strftime("%Y-%m-%d")} vs {fecha_inicio_p2.strftime("%Y-%m-%d")} a {fecha_fin_p2.strftime("%Y-%m-%d")}',
                 showlegend = False, yaxis_title="Monto ($)", margin=dict(l=20, r=20, t=100, b=20), height=600
             )
             st.plotly_chart(fig, use_container_width=True)
        elif abs(total_costo_p1) < variation_threshold and abs(total_costo_p2) < variation_threshold:
             st.info("Los costos totales para ambos períodos son cero o insignificantes.")
        elif abs(total_costo_p1) >= variation_threshold and abs(total_costo_p2 - total_costo_p1) < variation_threshold:
             st.info("El costo total del Período 2 es igual al Período 1 o la variación es insignificante.")
        else:
             st.info("No hay datos de costos suficientes para mostrar el gráfico.")
        st.subheader("Detalle de Costos por Período")
        col_p_1, col_p_2 = st.columns(2)
        with col_p_1:
            st.write(f"**Periodo 1: {fecha_inicio_p1.strftime('%Y-%m-%d')} a {fecha_fin_p1.strftime('%Y-%m-%d')}**")
            st.write(f"- Combustible: ${costo_combustible_p1:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p1:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p1:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p1:,.2f}")
            st.write(f"**Total Periodo 1: ${total_costo_p1:,.2f}**")
        with col_p_2:
            st.write(f"**Periodo 2: {fecha_inicio_p2.strftime('%Y-%m-%d')} a {fecha_fin_p2.strftime('%Y-%m-%d')}**")
            st.write(f"- Combustible: ${costo_combustible_p2:,.2f}")
            st.write(f"- Salarial: ${costo_salarial_p2:,.2f}")
            st.write(f"- Fijos: ${costo_fijos_p2:,.2f}")
            st.write(f"- Mantenimiento: ${costo_mantenimiento_p2:,.2f}")
            st.write(f"**Total Periodo 2: ${total_costo_p2:,.2f}**")
        if abs(variacion_total) >= variation_threshold or variation_data:
            st.subheader("Variaciones por Categoría")
            st.write(f"- Combustible: ${variacion_combustible:,.2f}")
            st.write(f"- Salarial: ${variacion_salarial:,.2f}")
            st.write(f"- Fijos: ${variacion_fijos:,.2f}")
            st.write(f"- Mantenimiento: ${variacion_mantenimiento:,.2f}")
            st.write(f"**Variación Total: ${variacion_total:,.2f}**")

def page_gestion_obras():
    st.title("Gestión de Obras")
    # This page uses st.data_editor for budget, which has its own NumberColumn.
    # No direct st.number_input with 'required' in the forms of this page.
    # For brevity, skipping full paste unless strictly needed.
    # It was included in the previous response, so I'll include it again.
    st.write("Aquí puedes crear y gestionar proyectos de obra, así como su presupuesto de materiales.")
    st.subheader("Crear Nueva Obra")
    with st.form("form_add_obra", clear_on_submit=True):
        nombre_obra = st.text_input("Nombre de la Obra").strip()
        responsable = st.text_input("Responsable de Seguimiento").strip()
        submitted = st.form_submit_button("Crear Obra")
        if submitted:
            if not nombre_obra or not responsable:
                st.warning("Por favor, complete Nombre de Obra y Responsable.")
            elif nombre_obra.lower() in st.session_state.df_proyectos['Nombre_Obra'].astype(str).str.strip().str.lower().tolist():
                st.warning(f"La obra '{nombre_obra}' ya existe.")
            else:
                existing_ids = set(st.session_state.df_proyectos['ID_Obra'].astype(str).tolist())
                base_id = f"OBRA_{int(time.time() * 1e6)}"
                id_obra = base_id
                counter = 0
                while id_obra in existing_ids:
                    counter += 1
                    id_obra = f"{base_id}_{counter}"
                new_obra_data = {'ID_Obra': id_obra, 'Nombre_Obra': nombre_obra, 'Responsable': responsable}
                new_obra_df = pd.DataFrame([new_obra_data])
                expected_cols_proyectos = list(TABLE_COLUMNS[TABLE_PROYECTOS].keys())
                new_obra_df = new_obra_df.reindex(columns=expected_cols_proyectos)
                for col, dtype in TABLE_COLUMNS[TABLE_PROYECTOS].items():
                    if col in new_obra_df.columns:
                         try:
                              if dtype == 'object':
                                   new_obra_df[col] = new_obra_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                   new_obra_df[col] = new_obra_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                              elif 'float' in dtype: new_obra_df[col] = pd.to_numeric(new_obra_df[col], errors='coerce').astype(float).fillna(0.0)
                              elif 'int' in dtype:
                                   if hasattr(pd, 'Int64Dtype'): new_obra_df[col] = pd.to_numeric(new_obra_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                   else: new_obra_df[col] = pd.to_numeric(new_obra_df[col], errors='coerce').astype(float).fillna(0.0)
                         except Exception as dtype_e:
                              st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                df_current_proyectos_reindexed = st.session_state.df_proyectos.reindex(columns=expected_cols_proyectos)
                st.session_state.df_proyectos = pd.concat([df_current_proyectos_reindexed, new_obra_df], ignore_index=True)
                save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS)
                st.success(f"Obra '{nombre_obra}' creada con ID: {id_obra}")
                st.experimental_rerun()

    st.subheader("Lista de Obras")
    obras_disponibles_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_list = [str(id).strip() for id in obras_disponibles_list if pd.notna(id) and str(id).strip() != '']
    obras_disponibles_list.sort()
    if not obras_disponibles_list:
        st.info("No hay obras creadas aún.")
        if "select_obra_gestion_selectbox_persistent" in st.session_state:
             del st.session_state["select_obra_gestion_selectbox_persistent"]
    else:
         st.info("Edite la tabla siguiente para modificar o eliminar obras.")
         df_proyectos_editable = st.session_state.df_proyectos.copy()
         expected_cols_proyectos = list(TABLE_COLUMNS[TABLE_PROYECTOS].keys())
         df_proyectos_editable = df_proyectos_editable.reindex(columns=expected_cols_proyectos)
         if 'ID_Obra' in df_proyectos_editable.columns:
              df_proyectos_editable['ID_Obra'] = df_proyectos_editable['ID_Obra'].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
         df_proyectos_edited = st.data_editor(
              df_proyectos_editable, key="data_editor_proyectos", num_rows="dynamic",
              column_config={
                   "ID_Obra": st.column_config.TextColumn("ID Obra", disabled=True),
                   "Nombre_Obra": st.column_config.TextColumn("Nombre Obra", required=True),
                   "Responsable": st.column_config.TextColumn("Responsable", required=True)
              }
         )
         df_proyectos_edited_processed = df_proyectos_edited.copy()
         df_proyectos_edited_processed = df_proyectos_edited_processed.reindex(columns=expected_cols_proyectos)
         new_row_mask = df_proyectos_edited_processed['ID_Obra'].isna() | (df_proyectos_edited_processed['ID_Obra'].astype(str).str.strip() == '')
         if new_row_mask.any():
              existing_ids = set(st.session_state.df_proyectos['ID_Obra'].astype(str).tolist())
              new_ids_batch = []
              for i in range(new_row_mask.sum()):
                  base_id = f"OBRA_EDIT_{int(time.time() * 1e6)}_{i}"
                  unique_id = base_id
                  counter = 0
                  while unique_id in existing_ids or unique_id in new_ids_batch:
                      counter += 1
                      unique_id = f"{base_id}_{counter}"
                  new_ids_batch.append(unique_id)
              df_proyectos_edited_processed.loc[new_row_mask, 'ID_Obra'] = new_ids_batch
         for col in ['Nombre_Obra', 'Responsable']:
            if col in df_proyectos_edited_processed.columns:
                 df_proyectos_edited_processed[col] = df_proyectos_edited_processed[col].astype(str).str.strip().replace({'': None}).mask(df_proyectos_edited_processed[col].isna(), None)
         df_proyectos_original_compare = st.session_state.df_proyectos.reindex(columns=expected_cols_proyectos).sort_values(by=expected_cols_proyectos).reset_index(drop=True)
         df_proyectos_edited_compare = df_proyectos_edited_processed.reindex(columns=expected_cols_proyectos).sort_values(by=expected_cols_proyectos).reset_index(drop=True)
         if not df_proyectos_edited_compare.equals(df_proyectos_original_compare):
              if st.button("Guardar Cambios en Lista de Obras", key="save_proyectos_button"):
                   df_to_save = df_proyectos_edited_processed.copy()
                   df_to_save = df_to_save[(df_to_save['Nombre_Obra'].notna()) & (df_to_save['Responsable'].notna())].copy()
                   if df_to_save.empty and not df_proyectos_edited_processed.empty:
                        st.error("Error: Ninguna fila válida. Complete Nombre Obra y Responsable.")
                   elif df_to_save['Nombre_Obra'].astype(str).str.strip().str.lower().duplicated().any():
                        st.error("Error: Nombres de obras duplicados.")
                   elif df_to_save['ID_Obra'].astype(str).str.strip().duplicated().any():
                       st.error("Error: IDs de obra duplicados.")
                   else:
                       if 'ID_Obra' in df_to_save.columns:
                           df_to_save['ID_Obra'] = df_to_save['ID_Obra'].astype(str).str.strip().replace({'': None}).mask(df_to_save['ID_Obra'].isna(), None)
                       st.session_state.df_proyectos = df_to_save
                       save_table(st.session_state.df_proyectos, DATABASE_FILE, TABLE_PROYECTOS)
                       st.success("Cambios en obras guardados.")
                       st.experimental_rerun()
              else:
                  st.info("Hay cambios sin guardar en la lista de obras.")
    obras_disponibles_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_list = [str(id).strip() for id in obras_disponibles_list if pd.notna(id) and str(id).strip() != '']
    obras_disponibles_list.sort()
    st.markdown("---")
    st.subheader("Gestionar Presupuesto por Obra")
    if not obras_disponibles_list:
         st.info("No hay obras disponibles para gestionar presupuesto.")
         if "select_obra_gestion_selectbox_persistent" in st.session_state:
              del st.session_state["select_obra_gestion_selectbox_persistent"]
         return
    obra_options_gestion_filtered_df = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str).isin(obras_disponibles_list)].copy()
    obra_options_gestion_list = [(f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})", o['ID_Obra']) for index, o in obra_options_gestion_filtered_df.iterrows() if pd.notna(o['ID_Obra'])]
    obra_options_gestion_list.sort(key=lambda x: x[0])
    obra_gestion_labels = [item[0] for item in obra_options_gestion_list]
    obra_gestion_label_to_id = dict(obra_options_gestion_list)
    if not obra_gestion_labels:
         st.info("No hay obras disponibles para gestionar presupuesto.")
         if "select_obra_gestion_selectbox_persistent" in st.session_state: del st.session_state["select_obra_gestion_selectbox_persistent"]
         st.experimental_rerun()
         return
    default_obra_index = 0
    if "select_obra_gestion_selectbox_persistent" in st.session_state and st.session_state.select_obra_gestion_selectbox_persistent in obra_gestion_labels:
         default_obra_index = obra_gestion_labels.index(st.session_state.select_obra_gestion_selectbox_persistent)
    elif "select_obra_gestion_selectbox_persistent" in st.session_state:
         del st.session_state["select_obra_gestion_selectbox_persistent"]
    selected_obra_label_gestion = st.selectbox(
        "Seleccione una Obra:", options=obra_gestion_labels, index=default_obra_index, key="select_obra_gestion_selectbox_persistent"
    )
    obra_seleccionada_id = obra_gestion_label_to_id.get(selected_obra_label_gestion)
    if obra_seleccionada_id is None or str(obra_seleccionada_id) not in obras_disponibles_list:
         st.warning(f"La obra '{selected_obra_label_gestion}' ya no es válida.")
         if "select_obra_gestion_selectbox_persistent" in st.session_state: del st.session_state["select_obra_gestion_selectbox_persistent"]
         st.experimental_rerun()
         return
    obra_name_row = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str) == str(obra_seleccionada_id)].iloc[0] if not st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str) == str(obra_seleccionada_id)].empty else None
    obra_nombre = obra_name_row['Nombre_Obra'] if obra_name_row is not None and 'Nombre_Obra' in obra_name_row and pd.notna(obra_name_row['Nombre_Obra']) else f"Obra ID: {obra_seleccionada_id}"
    st.markdown(f"#### Presupuesto de Materiales para '{obra_nombre}'")
    df_presupuesto_materiales_temp = st.session_state.df_presupuesto_materiales.copy()
    if 'ID_Obra' in df_presupuesto_materiales_temp.columns:
         df_presupuesto_materiales_temp['ID_Obra_clean'] = df_presupuesto_materiales_temp['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_presupuesto_materiales_temp['ID_Obra'].isna(), None)
    else:
         df_presupuesto_materiales_temp['ID_Obra_clean'] = None
    df_presupuesto_obra = df_presupuesto_materiales_temp[
        df_presupuesto_materiales_temp['ID_Obra_clean'] == str(obra_seleccionada_id)
    ].copy()
    df_presupuesto_obra = df_presupuesto_obra.drop(columns=['ID_Obra_clean'], errors='ignore')
    st.info("Edite la tabla siguiente para añadir, modificar o eliminar items del presupuesto.")
    df_presupuesto_obra_display = df_presupuesto_obra.copy()
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
        if col not in df_presupuesto_obra_display.columns: df_presupuesto_obra_display[col] = 0.0
        df_presupuesto_obra_display[col] = pd.to_numeric(df_presupuesto_obra_display[col], errors='coerce').fillna(0.0)
    df_presupuesto_obra_display = calcular_costo_presupuestado(df_presupuesto_obra_display)
    expected_cols_presupuesto = list(TABLE_COLUMNS[TABLE_PRESUPUESTO_MATERIALES].keys())
    df_presupuesto_obra_display = df_presupuesto_obra_display.reindex(columns=expected_cols_presupuesto)
    for col in ['ID_Obra', 'Material']:
        if col in df_presupuesto_obra_display.columns:
             df_presupuesto_obra_display[col] = df_presupuesto_obra_display[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
    df_presupuesto_obra_edited = st.data_editor(
        df_presupuesto_obra_display, key=f"data_editor_presupuesto_{obra_seleccionada_id}", num_rows="dynamic",
        column_config={
            "ID_Obra": st.column_config.TextColumn("ID Obra", disabled=True),
            "Material": st.column_config.TextColumn("Material", required=True),
            "Cantidad_Presupuestada": st.column_config.NumberColumn("Cantidad Presupuestada", min_value=0.0, format="%.2f", required=True),
            "Precio_Unitario_Presupuestado": st.column_config.NumberColumn("Precio Unitario Presupuestado", min_value=0.0, format="%.2f", required=True),
            "Costo_Presupuestado": st.column_config.NumberColumn("Costo Presupuestado", disabled=True, format="%.2f")
        }
    )
    df_presupuesto_obra_edited_processed = df_presupuesto_obra_edited.copy()
    df_presupuesto_obra_edited_processed = df_presupuesto_obra_edited_processed.reindex(columns=expected_cols_presupuesto)
    df_presupuesto_obra_edited_processed['ID_Obra'] = str(obra_seleccionada_id)
    if 'Material' in df_presupuesto_obra_edited_processed.columns:
        df_presupuesto_obra_edited_processed['Material'] = df_presupuesto_obra_edited_processed['Material'].astype(str).str.strip().replace({'': None}).mask(df_presupuesto_obra_edited_processed['Material'].isna(), None)
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
         if col not in df_presupuesto_obra_edited_processed.columns: df_presupuesto_obra_edited_processed[col] = 0.0
         df_presupuesto_obra_edited_processed[col] = pd.to_numeric(df_presupuesto_obra_edited_processed[col], errors='coerce').fillna(0.0)
    df_presupuesto_obra_edited_processed = calcular_costo_presupuestado(df_presupuesto_obra_edited_processed)
    df_presupuesto_obra_original_filtered = st.session_state.df_presupuesto_materiales.copy()
    if 'ID_Obra' in df_presupuesto_obra_original_filtered.columns:
         df_presupuesto_obra_original_filtered['ID_Obra_clean'] = df_presupuesto_obra_original_filtered['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_presupuesto_obra_original_filtered['ID_Obra'].isna(), None)
    else:
         df_presupuesto_obra_original_filtered['ID_Obra_clean'] = None
    df_presupuesto_obra_original_filtered = df_presupuesto_obra_original_filtered[
        df_presupuesto_obra_original_filtered['ID_Obra_clean'] == str(obra_seleccionada_id)
    ].copy()
    df_presupuesto_obra_original_filtered = df_presupuesto_obra_original_filtered.drop(columns=['ID_Obra_clean'], errors='ignore')
    df_presupuesto_obra_original_filtered = df_presupuesto_obra_original_filtered.reindex(columns=expected_cols_presupuesto)
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
         if col not in df_presupuesto_obra_original_filtered.columns: df_presupuesto_obra_original_filtered[col] = 0.0
         df_presupuesto_obra_original_filtered[col] = pd.to_numeric(df_presupuesto_obra_original_filtered[col], errors='coerce').fillna(0.0)
    df_presupuesto_obra_original_filtered = calcular_costo_presupuestado(df_presupuesto_obra_original_filtered)
    df_presupuesto_obra_original_compare = df_presupuesto_obra_original_filtered.sort_values(by=expected_cols_presupuesto).reset_index(drop=True)
    df_presupuesto_obra_edited_compare = df_presupuesto_obra_edited_processed.sort_values(by=expected_cols_presupuesto).reset_index(drop=True)
    if not df_presupuesto_obra_edited_compare.equals(df_presupuesto_obra_original_compare):
         if st.button(f"Guardar Cambios en Presupuesto de '{obra_nombre}'", key=f"save_presupuesto_{obra_seleccionada_id}_button"):
             df_to_save_obra = df_presupuesto_obra_edited_processed.copy()
             df_to_save_obra = df_to_save_obra[(df_to_save_obra['Material'].notna()) &
                                               (df_to_save_obra['Cantidad_Presupuestada'].notna()) &
                                               (df_to_save_obra['Precio_Unitario_Presupuestado'].notna())].copy()
             if df_to_save_obra.empty and not df_presupuesto_obra_edited_processed.empty:
                  st.error("Error: Ninguna fila válida. Complete campos obligatorios.")
             elif 'Material' in df_to_save_obra.columns and df_to_save_obra['Material'].astype(str).str.strip().str.lower().duplicated().any():
                  st.error("Error: Materiales duplicados para esta obra.")
             else:
                 df_rest_presupuesto = st.session_state.df_presupuesto_materiales.copy()
                 if 'ID_Obra' in df_rest_presupuesto.columns:
                      df_rest_presupuesto['ID_Obra_clean'] = df_rest_presupuesto['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_rest_presupuesto['ID_Obra'].isna(), None)
                 else:
                      df_rest_presupuesto['ID_Obra_clean'] = 'ID Desconocida'
                 df_rest_presupuesto = df_rest_presupuesto[
                     df_rest_presupuesto['ID_Obra_clean'] != str(obra_seleccionada_id)
                 ].copy()
                 df_rest_presupuesto = df_rest_presupuesto.drop(columns=['ID_Obra_clean'], errors='ignore')
                 df_rest_presupuesto = df_rest_presupuesto.reindex(columns=expected_cols_presupuesto)
                 st.session_state.df_presupuesto_materiales = pd.concat([df_rest_presupuesto, df_to_save_obra.reindex(columns=expected_cols_presupuesto)], ignore_index=True)
                 save_table(st.session_state.df_presupuesto_materiales, DATABASE_FILE, TABLE_PRESUPUESTO_MATERIALES)
                 st.success(f"Presupuesto de '{obra_nombre}' guardado.")
                 st.experimental_rerun()
         else:
             st.info(f"Hay cambios sin guardar en el presupuesto de '{obra_nombre}'.")
    st.markdown(f"#### Reporte de Presupuesto para '{obra_nombre}'")
    df_presupuesto_obra_current = st.session_state.df_presupuesto_materiales.copy()
    if 'ID_Obra' in df_presupuesto_obra_current.columns:
         df_presupuesto_obra_current['ID_Obra_clean'] = df_presupuesto_obra_current['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_presupuesto_obra_current['ID_Obra'].isna(), None)
    else:
         df_presupuesto_obra_current['ID_Obra_clean'] = None
    df_presupuesto_obra_current = df_presupuesto_obra_current[
         df_presupuesto_obra_current['ID_Obra_clean'] == str(obra_seleccionada_id)
    ].copy()
    df_presupuesto_obra_current = df_presupuesto_obra_current.drop(columns=['ID_Obra_clean'], errors='ignore')
    if df_presupuesto_obra_current.empty:
        st.info("No hay presupuesto de materiales registrado para esta obra.")
    else:
        st.subheader("Detalle del Presupuesto")
        for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
             if col not in df_presupuesto_obra_current.columns: df_presupuesto_obra_current[col] = 0.0
             df_presupuesto_obra_current[col] = pd.to_numeric(df_presupuesto_obra_current[col], errors='coerce').fillna(0.0)
        df_presupuesto_obra_with_cost = calcular_costo_presupuestado(df_presupuesto_obra_current)
        report_cols_presupuesto = ['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']
        report_cols_presupuesto_present = [col for col in report_cols_presupuesto if col in df_presupuesto_obra_with_cost.columns]
        if report_cols_presupuesto_present:
             st.dataframe(df_presupuesto_obra_with_cost[report_cols_presupuesto_present].round(2))
        else:
             st.warning("No se pudieron mostrar detalles del presupuesto.")
        cantidad_presupuestada_sum = pd.to_numeric(df_presupuesto_obra_with_cost.get('Cantidad_Presupuestada', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        costo_presupuestado_sum = pd.to_numeric(df_presupuesto_obra_with_cost.get('Costo_Presupuestado', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        st.subheader("Resumen del Presupuesto")
        st.write(f"**Cantidad Total Presupuestada:** {cantidad_presupuestada_sum:,.2f}")
        st.write(f"**Costo Total Presupuestado:** ${costo_presupuestado_sum:,.2f}")
    st.markdown(f"#### Variación Materiales para '{obra_nombre}' (Presupuesto vs Asignado)")
    df_asignacion_materiales_temp = st.session_state.df_asignacion_materiales.copy()
    if 'ID_Obra' in df_asignacion_materiales_temp.columns:
         df_asignacion_materiales_temp['ID_Obra_clean'] = df_asignacion_materiales_temp['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_asignacion_materiales_temp['ID_Obra'].isna(), None)
    else:
         df_asignacion_materiales_temp['ID_Obra_clean'] = None
    df_asignacion_obra_current = df_asignacion_materiales_temp[
         df_asignacion_materiales_temp['ID_Obra_clean'] == str(obra_seleccionada_id)
    ].copy()
    df_asignacion_obra_current = df_asignacion_obra_current.drop(columns=['ID_Obra_clean'], errors='ignore')
    for col in ['Cantidad_Asignada', 'Precio_Unitario_Asignado']:
         if col not in df_asignacion_obra_current.columns: df_asignacion_obra_current[col] = 0.0
         df_asignacion_obra_current[col] = pd.to_numeric(df_asignacion_obra_current[col], errors='coerce').fillna(0.0)
    df_asignacion_obra_current = calcular_costo_asignado(df_asignacion_obra_current)
    if df_presupuesto_obra_current.empty and df_asignacion_obra_current.empty:
        st.info("No hay presupuesto ni materiales asignados para esta obra.")
    else:
       presupuesto_agg = pd.DataFrame(columns=['Material', 'Cantidad_Presupuestada', 'Costo_Presupuestado'])
       if not df_presupuesto_obra_current.empty and 'Material' in df_presupuesto_obra_current.columns and 'Cantidad_Presupuestada' in df_presupuesto_obra_current.columns and 'Costo_Presupuestado' in df_presupuesto_obra_current.columns:
           df_presupuesto_obra_current_clean = df_presupuesto_obra_current.copy()
           df_presupuesto_obra_current_clean['Material'] = df_presupuesto_obra_current_clean['Material'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None})
           df_presupuesto_obra_current_clean['Cantidad_Presupuestada'] = pd.to_numeric(df_presupuesto_obra_current_clean['Cantidad_Presupuestada'], errors='coerce').fillna(0.0)
           df_presupuesto_obra_current_clean['Costo_Presupuestado'] = pd.to_numeric(df_presupuesto_obra_current_clean['Costo_Presupuestado'], errors='coerce').fillna(0.0)
           presupuesto_agg = df_presupuesto_obra_current_clean.dropna(subset=['Material']).groupby('Material', dropna=True).agg(
               Cantidad_Presupuestada=('Cantidad_Presupuestada', 'sum'), Costo_Presupuestado=('Costo_Presupuestado', 'sum')
           ).reset_index()
       asignacion_agg = pd.DataFrame(columns=['Material', 'Cantidad_Asignada', 'Costo_Asignado'])
       if not df_asignacion_obra_current.empty and 'Material' in df_asignacion_obra_current.columns and 'Cantidad_Asignada' in df_asignacion_obra_current.columns and 'Costo_Asignado' in df_asignacion_obra_current.columns:
           df_asignacion_obra_current_clean = df_asignacion_obra_current.copy()
           df_asignacion_obra_current_clean['Material'] = df_asignacion_obra_current_clean['Material'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None})
           df_asignacion_obra_current_clean['Cantidad_Asignada'] = pd.to_numeric(df_asignacion_obra_current_clean['Cantidad_Asignada'], errors='coerce').fillna(0.0)
           df_asignacion_obra_current_clean['Costo_Asignado'] = pd.to_numeric(df_asignacion_obra_current_clean['Costo_Asignado'], errors='coerce').fillna(0.0)
           asignacion_agg = df_asignacion_obra_current_clean.dropna(subset=['Material']).groupby('Material', dropna=True).agg(
               Cantidad_Asignada=('Cantidad_Asignada', 'sum'), Costo_Asignado=('Costo_Asignado', 'sum')
           ).reset_index()
       presupuesto_agg['Material'] = presupuesto_agg['Material'].astype(str)
       asignacion_agg['Material'] = asignacion_agg['Material'].astype(str)
       variacion_obra = pd.merge(presupuesto_agg, asignacion_agg, on='Material', how='outer').fillna(0)
       cost_cols = ['Costo_Presupuestado', 'Costo_Asignado']
       qty_cols = ['Cantidad_Presupuestada', 'Cantidad_Asignada']
       for col in cost_cols + qty_cols:
           if col not in variacion_obra.columns: variacion_obra[col] = 0.0
           else: variacion_obra[col] = pd.to_numeric(variacion_obra[col], errors='coerce').fillna(0.0)
       variacion_obra['Cantidad_Variacion'] = variacion_obra['Cantidad_Asignada'] - variacion_obra['Cantidad_Presupuestada']
       variacion_obra['Costo_Variacion'] = variacion_obra['Costo_Asignado'] - variacion_obra['Costo_Presupuestado']
       st.subheader("Reporte de Variación por Material")
       if variacion_obra.empty:
            st.info("No hay datos de variación de materiales para esta obra.")
       else:
           report_cols_variacion = ['Material', 'Cantidad_Presupuestada', 'Cantidad_Asignada', 'Cantidad_Variacion', 'Costo_Presupuestado', 'Costo_Asignado', 'Costo_Variacion']
           for col in report_cols_variacion:
                if col not in variacion_obra.columns: variacion_obra[col] = pd.NA
           display_cols_present = [col for col in report_cols_variacion if col in variacion_obra.columns]
           if display_cols_present: st.dataframe(variacion_obra[display_cols_present].round(2))
           else: st.warning("No se pudo mostrar el reporte de variación.")
           total_costo_presupuestado_obra = pd.to_numeric(variacion_obra.get('Costo_Presupuestado', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
           total_costo_asignado_obra = pd.to_numeric(variacion_obra.get('Costo_Asignado', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
           total_variacion_costo_obra = total_costo_asignado_obra - total_costo_presupuestado_obra
           st.subheader("Resumen de Variación de Costo Total")
           st.write(f"Costo Presupuestado Total: ${total_costo_presupuestado_obra:,.2f}")
           st.write(f"Costo Asignado (Real) Total: ${total_costo_asignado_obra:,.2f}")
           st.write(f"Variación Total: ${total_variacion_costo_obra:,.2f}")
           variation_threshold_obra = 0.01
           if abs(total_variacion_costo_obra) >= variation_threshold_obra or abs(total_costo_presupuestado_obra) >= variation_threshold_obra or abs(total_costo_asignado_obra) >= variation_threshold_obra:
                st.subheader("Gráfico de Variación de Costo por Obra")
                labels_obra_cascada = [f'Presupuesto<br>{obra_nombre}']
                values_obra_cascada = [total_costo_presupuestado_obra]
                measures_obra_cascada = ['absolute']
                texts_obra_cascada = [f"${total_costo_presupuestado_obra:,.2f}"]
                if abs(total_variacion_costo_obra) >= variation_threshold_obra:
                     labels_obra_cascada.append('Variación Total')
                     values_obra_cascada.append(total_variacion_costo_obra)
                     measures_obra_cascada.append('relative')
                     texts_obra_cascada.append(f"${total_variacion_costo_obra:,.2f}")
                labels_obra_cascada.append(f'Asignado<br>{obra_nombre}')
                values_obra_cascada.append(total_costo_asignado_obra)
                measures_obra_cascada.append('total')
                texts_obra_cascada.append(f"${total_costo_asignado_obra:,.2f}") # Corrected texts here
                if (len(labels_obra_cascada) > 2) or (len(labels_obra_cascada) == 2 and abs(values_obra_cascada[0] - values_obra_cascada[1]) >= variation_threshold_obra) or (len(labels_obra_cascada) == 2 and abs(values_obra_cascada[0]) >= variation_threshold_obra):
                     fig_obra_variacion = go.Figure(go.Waterfall(
                        name = f"Variación Obra: {obra_nombre}", orientation = "v", measure = measures_obra_cascada,
                        x = labels_obra_cascada, textposition = "outside", text = texts_obra_cascada, y = values_obra_cascada,
                        connector = {"line":{"color":"rgb(63, 63, 63)"}}, increasing = {"marker":{"color":"#FF4136"}},
                        decreasing = {"marker":{"color":"#3D9970"}}, totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
                     ))
                     fig_obra_variacion.update_layout(
                         title = f'Variación Costo Materiales Obra: {obra_nombre}', showlegend = False,
                         yaxis_title="Monto ($)", margin=dict(l=20, r=20, t=60, b=20), height=400
                     )
                     st.plotly_chart(fig_obra_variacion, use_container_width=True)
                elif abs(total_costo_presupuestado_obra) < variation_threshold_obra and abs(total_costo_asignado_obra) < variation_threshold_obra:
                      st.info("El presupuesto y costo asignado son cero o insignificantes.")
                elif abs(total_costo_presupuestado_obra) >= variation_threshold_obra and abs(total_costo_asignado_obra - total_costo_presupuestado_obra) < variation_threshold_obra:
                      st.info("El costo asignado es igual al presupuestado o la variación es insignificante.")
                else:
                     st.info("No hay datos de costos suficientes para mostrar el gráfico.")
           else:
                st.info("No hay costo presupuestado ni asignado total para mostrar el gráfico.")

def page_reporte_presupuesto_total_obras():
    st.title("Reporte de Presupuesto Total por Obras")
    # This page does calculations and displays a dataframe/metrics. No direct st.number_input with 'required'.
    # For brevity, skipping. It was included in the previous response.
    # Pasting it again for completeness.
    st.write("Suma el presupuesto total de materiales de todas las obras.")
    if st.session_state.df_presupuesto_materiales.empty:
        st.info("No hay presupuesto de materiales registrado para ninguna obra.")
        return
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
        if col not in df_presupuesto.columns: df_presupuesto[col] = 0.0
        df_presupuesto[col] = pd.to_numeric(df_presupuesto[col], errors='coerce').fillna(0.0)
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto)
    if 'ID_Obra' in df_presupuesto.columns:
        df_presupuesto['ID_Obra_clean'] = df_presupuesto['ID_Obra'].astype(str).str.strip().replace({'': 'ID Desconocida', 'nan': 'ID Desconocida', 'None': 'ID Desconocida'})
    else:
         df_presupuesto['ID_Obra_clean'] = 'ID Desconocida'
    if 'Cantidad_Presupuestada' not in df_presupuesto.columns: df_presupuesto['Cantidad_Presupuestada'] = 0.0
    if 'Costo_Presupuestado' not in df_presupuesto.columns: df_presupuesto['Costo_Presupuestado'] = 0.0
    df_presupuesto['Cantidad_Presupuestada'] = pd.to_numeric(df_presupuesto['Cantidad_Presupuestada'], errors='coerce').fillna(0.0)
    df_presupuesto['Costo_Presupuestado'] = pd.to_numeric(df_presupuesto['Costo_Presupuestado'], errors='coerce').fillna(0.0)
    if not df_presupuesto.empty:
        reporte_por_obra = df_presupuesto.groupby('ID_Obra_clean', dropna=False).agg(
            Cantidad_Total_Presupuestada=('Cantidad_Presupuestada', 'sum'),
            Costo_Total_Presupuestado=('Costo_Presupuestado', 'sum')
        ).reset_index()
    else:
         reporte_por_obra = pd.DataFrame(columns=['ID_Obra_clean', 'Cantidad_Total_Presupuestada', 'Costo_Total_Presupuestado'])
    df_proyectos_temp = st.session_state.df_proyectos.copy()
    if 'ID_Obra' in df_proyectos_temp.columns:
         df_proyectos_temp['ID_Obra_clean_for_merge'] = df_proyectos_temp['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_proyectos_temp['ID_Obra'].isna(), None)
         reporte_por_obra = reporte_por_obra.merge(df_proyectos_temp[['ID_Obra_clean_for_merge', 'Nombre_Obra']], left_on='ID_Obra_clean', right_on='ID_Obra_clean_for_merge', how='left')
         reporte_por_obra['Nombre_Obra'] = reporte_por_obra.apply(
             lambda row: row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else f"Obra ID: {row['ID_Obra_clean']}" if row['ID_Obra_clean'] != 'ID Desconocida' else 'ID Desconocida', axis=1
         )
         reporte_por_obra = reporte_por_obra.drop(columns=['ID_Obra_clean_for_merge'], errors='ignore')
    else:
         reporte_por_obra['Nombre_Obra'] = reporte_por_obra['ID_Obra_clean'].apply(lambda x: f"Obra ID: {x}" if x != 'ID Desconocida' else 'ID Desconocida')
    reporte_por_obra = reporte_por_obra.rename(columns={'ID_Obra_clean': 'ID_Obra'})
    sort_cols = []
    if 'Nombre_Obra' in reporte_por_obra.columns: sort_cols.append('Nombre_Obra')
    if 'ID_Obra' in reporte_por_obra.columns: sort_cols.append('ID_Obra')
    if sort_cols:
         reporte_por_obra = reporte_por_obra.sort_values(by=sort_cols).reset_index(drop=True)
    st.subheader("Presupuesto Total por Obra")
    if reporte_por_obra.empty:
         st.info("No hay presupuesto total calculado.")
    else:
         display_cols = ['Nombre_Obra', 'ID_Obra', 'Cantidad_Total_Presupuestada', 'Costo_Total_Presupuestado']
         for col in display_cols:
              if col not in reporte_por_obra.columns:
                   reporte_por_obra[col] = pd.NA
         display_cols_present = [col for col in display_cols if col in reporte_por_obra.columns]
         if display_cols_present: st.dataframe(reporte_por_obra[display_cols_present].round(2))
         else: st.warning("No se pudo mostrar el reporte.")
         cantidad_gran_total = pd.to_numeric(reporte_por_obra.get('Cantidad_Total_Presupuestada', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
         costo_gran_total = pd.to_numeric(reporte_por_obra.get('Costo_Total_Presupuestado', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
         st.subheader("Gran Total Presupuestado (Todas las Obras)")
         st.write(f"**Cantidad Gran Total Presupuestada:** {cantidad_gran_total:,.2f}")
         st.write(f"**Costo Gran Total Presupuestado:** ${costo_gran_total:,.2f}")

def page_compras_asignacion():
    st.title("Gestión de Compras y Asignación de Materiales")
    st.write("Registra las compras y asigna materiales a las obras.")

    st.subheader("Registrar Compra de Materiales")
    with st.form("form_add_compra", clear_on_submit=True):
        fecha_compra = st.date_input("Fecha de Compra", value=datetime.date.today(), key="compra_fecha_add")
        material_compra = st.text_input("Nombre del Material Comprado", key="compra_material_add").strip()
        cantidad_comprada = st.number_input("Cantidad Comprada", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="compra_cantidad_add") # Removed required
        precio_unitario_comprado = st.number_input("Precio Unitario de Compra", min_value=0.0, value=0.0, step=0.01, format="%.2f", key="compra_precio_add") # Removed required
        submitted = st.form_submit_button("Registrar Compra")
        if submitted:
            if not fecha_compra:
                st.warning("Por favor, complete la fecha de compra.")
            elif not material_compra:
                st.warning("Por favor, complete el nombre del material.")
            elif cantidad_comprada is None or precio_unitario_comprado is None: # Added None check
                 st.warning("Por favor, complete los campos de cantidad y precio unitario.")
            elif (cantidad_comprada < 0 or precio_unitario_comprado < 0):
                 st.warning("Cantidad y Precio Unitario deben ser >= 0.")
            elif cantidad_comprada == 0 and precio_unitario_comprado == 0:
                st.warning("Cantidad y precio no pueden ser ambos cero.")
            else:
                existing_ids = set(st.session_state.df_compras_materiales['ID_Compra'].astype(str).tolist())
                base_id = f"COMPRA_{int(time.time() * 1e6)}"
                id_compra = base_id
                counter = 0
                while id_compra in existing_ids:
                    counter += 1
                    id_compra = f"{base_id}_{counter}"
                new_compra_data = {
                    'ID_Compra': id_compra, 'Fecha_Compra': fecha_compra, 'Material': material_compra,
                    'Cantidad_Comprada': float(cantidad_comprada if cantidad_comprada is not None else 0.0), # Handle None
                    'Precio_Unitario_Comprado': float(precio_unitario_comprado if precio_unitario_comprado is not None else 0.0) # Handle None
                }
                new_compra_df = pd.DataFrame([new_compra_data])
                new_compra_df = calcular_costo_compra(new_compra_df)
                expected_cols_compras = list(TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].keys())
                new_compra_df = new_compra_df.reindex(columns=expected_cols_compras)
                date_col_name_compra = DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]
                if date_col_name_compra in new_compra_df.columns:
                     new_compra_df[date_col_name_compra] = pd.to_datetime(new_compra_df[date_col_name_compra], errors='coerce')
                for col, dtype in TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].items():
                     if col in new_compra_df.columns:
                           try:
                                if dtype == 'object':
                                     new_compra_df[col] = new_compra_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                     new_compra_df[col] = new_compra_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                elif 'float' in dtype: new_compra_df[col] = pd.to_numeric(new_compra_df[col], errors='coerce').astype(float).fillna(0.0)
                                elif 'int' in dtype:
                                     if hasattr(pd, 'Int64Dtype'): new_compra_df[col] = pd.to_numeric(new_compra_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                     else: new_compra_df[col] = pd.to_numeric(new_compra_df[col], errors='coerce').astype(float).fillna(0.0)
                           except Exception as dtype_e:
                                st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                df_current_compras_reindexed = st.session_state.df_compras_materiales.reindex(columns=expected_cols_compras)
                st.session_state.df_compras_materiales = pd.concat([df_current_compras_reindexed, new_compra_df], ignore_index=True)
                save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES)
                st.success(f"Compra de '{material_compra}' registrada con ID: {id_compra}")
                st.experimental_rerun()

    st.subheader("Historial de Compras")
    if st.session_state.df_compras_materiales.empty:
        st.info("No hay compras registradas aún.")
    else:
         st.info("Edite la tabla siguiente para modificar o eliminar compras.")
         df_compras_editable = st.session_state.df_compras_materiales.copy()
         date_col_name_compra = DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]
         if date_col_name_compra in df_compras_editable.columns:
              df_compras_editable[date_col_name_compra] = pd.to_datetime(df_compras_editable[date_col_name_compra], errors='coerce')
         else:
              df_compras_editable[date_col_name_compra] = pd.Series(dtype='datetime64[ns]', index=df_compras_editable.index)
         for col in ['Cantidad_Comprada', 'Precio_Unitario_Comprado']:
             if col not in df_compras_editable.columns: df_compras_editable[col] = 0.0
             df_compras_editable[col] = pd.to_numeric(df_compras_editable[col], errors='coerce').fillna(0.0)
         df_compras_editable = calcular_costo_compra(df_compras_editable)
         expected_cols_compras = list(TABLE_COLUMNS[TABLE_COMPRAS_MATERIALES].keys())
         df_compras_editable = df_compras_editable.reindex(columns=expected_cols_compras)
         for col in ['ID_Compra', 'Material']:
             if col in df_compras_editable.columns:
                 df_compras_editable[col] = df_compras_editable[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
         df_compras_edited = st.data_editor(
             df_compras_editable, key="data_editor_compras", num_rows="dynamic",
             column_config={
                 "ID_Compra": st.column_config.TextColumn("ID Compra", disabled=True),
                 date_col_name_compra: st.column_config.DateColumn("Fecha Compra", required=True),
                 "Material": st.column_config.TextColumn("Material", required=True),
                 "Cantidad_Comprada": st.column_config.NumberColumn("Cantidad Comprada", min_value=0.0, format="%.2f", required=True),
                 "Precio_Unitario_Comprado": st.column_config.NumberColumn("Precio Unitario Compra", min_value=0.0, format="%.2f", required=True),
                 "Costo_Compra": st.column_config.NumberColumn("Costo Compra", disabled=True, format="%.2f")
             }
         )
         df_compras_edited_processed = df_compras_edited.copy()
         df_compras_edited_processed = df_compras_edited_processed.reindex(columns=expected_cols_compras)
         for col in ['ID_Compra', 'Material']:
            if col in df_compras_edited_processed.columns:
                 df_compras_edited_processed[col] = df_compras_edited_processed[col].astype(str).str.strip().replace({'': None}).mask(df_compras_edited_processed[col].isna(), None)
         for col in ['Cantidad_Comprada', 'Precio_Unitario_Comprado']:
             if col not in df_compras_edited_processed.columns: df_compras_edited_processed[col] = 0.0
             df_compras_edited_processed[col] = pd.to_numeric(df_compras_edited_processed[col], errors='coerce').fillna(0.0)
         df_compras_edited_processed = calcular_costo_compra(df_compras_edited_processed)
         new_row_mask = df_compras_edited_processed['ID_Compra'].isna()
         if new_row_mask.any():
              existing_ids = set(st.session_state.df_compras_materiales['ID_Compra'].astype(str).tolist())
              new_ids_batch = []
              for i in range(new_row_mask.sum()):
                   base_id = f"COMPRA_EDIT_{int(time.time() * 1e6)}_{i}"
                   unique_id = base_id
                   counter = 0
                   while unique_id in existing_ids or unique_id in new_ids_batch:
                       counter += 1
                       unique_id = f"{base_id}_{counter}"
                   new_ids_batch.append(unique_id)
              df_compras_edited_processed.loc[new_row_mask, 'ID_Compra'] = new_ids_batch
         df_compras_original_compare = st.session_state.df_compras_materiales.reindex(columns=expected_cols_compras).sort_values(by=expected_cols_compras).reset_index(drop=True)
         df_compras_edited_compare = df_compras_edited_processed.reindex(columns=expected_cols_compras).sort_values(by=expected_cols_compras).reset_index(drop=True)
         if not df_compras_edited_compare.equals(df_compras_original_compare):
              if st.button("Guardar Cambios en Historial de Compras", key="save_compras_button"):
                 df_to_save = df_compras_edited_processed.copy()
                 date_col_name_compra = DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]
                 df_to_save = df_to_save[(df_to_save['ID_Compra'].notna()) &
                                         (df_to_save[date_col_name_compra].notna()) &
                                         (df_to_save['Material'].notna()) &
                                         (df_to_save['Cantidad_Comprada'].notna()) &
                                         (df_to_save['Precio_Unitario_Comprado'].notna())
                                        ].copy()
                 if df_to_save.empty and not df_compras_edited_processed.empty:
                      st.error("Error: Ninguna fila válida. Complete campos obligatorios.")
                 elif ((pd.to_numeric(df_to_save['Cantidad_Comprada'], errors='coerce').fillna(0) == 0) &
                       (pd.to_numeric(df_to_save['Precio_Unitario_Comprado'], errors='coerce').fillna(0) == 0)).any():
                      st.warning("Advertencia: Algunas compras tienen Cantidad y Precio Unitario ambos cero.")
                 elif df_to_save['ID_Compra'].astype(str).str.strip().duplicated().any():
                     st.error("Error: IDs de compra duplicados.")
                 else:
                      st.session_state.df_compras_materiales = df_to_save
                      save_table(st.session_state.df_compras_materiales, DATABASE_FILE, TABLE_COMPRAS_MATERIALES)
                      st.success("Cambios en historial de compras guardados.")
                      st.experimental_rerun()
              else:
                 st.info("Hay cambios sin guardar en el historial de compras.")

    st.markdown("---")
    st.subheader("Asignar Materiales a Obra")
    obras_disponibles_assign_list = st.session_state.df_proyectos['ID_Obra'].unique().tolist()
    obras_disponibles_assign_list = [str(id).strip() for id in obras_disponibles_assign_list if pd.notna(id) and str(id).strip() != '']
    obras_disponibles_assign_list.sort()

    if not obras_disponibles_assign_list:
        st.warning("No hay obras creadas. No se pueden asignar materiales.")
        if "asig_obra_selectbox_persistent" in st.session_state: del st.session_state["asig_obra_selectbox_persistent"]
        return

    obra_options_assign_filtered_df = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str).isin(obras_disponibles_assign_list)].copy()
    obra_options_assign_list = [(f"{o['Nombre_Obra']} (ID: {o['ID_Obra']})", o['ID_Obra']) for index, o in obra_options_assign_filtered_df.iterrows() if pd.notna(o['ID_Obra'])]
    obra_options_assign_list.sort(key=lambda x: x[0])
    obra_assign_labels = [item[0] for item in obra_options_assign_list]
    obra_assign_label_to_id = dict(obra_options_assign_list)
    if not obra_assign_labels:
        st.warning("No hay obras disponibles para asignar materiales.")
        if "asig_obra_selectbox_persistent" in st.session_state: del st.session_state["asig_obra_selectbox_persistent"]
        st.experimental_rerun()
        return
    default_obra_assign_index = 0
    if "asig_obra_selectbox_persistent" in st.session_state and st.session_state.asig_obra_selectbox_persistent in obra_assign_labels:
         default_obra_assign_index = obra_assign_labels.index(st.session_state.asig_obra_selectbox_persistent)
    elif "asig_obra_selectbox_persistent" in st.session_state:
         del st.session_state["asig_obra_selectbox_persistent"]
    materiales_comprados_unicos = st.session_state.df_compras_materiales['Material'].unique().tolist()
    materiales_comprados_unicos = [str(m).strip() for m in materiales_comprados_unicos if pd.notna(m) and str(m).strip() != '']
    materiales_comprados_unicos.sort()
    material_options_select = ["Seleccionar material..."] + materiales_comprados_unicos if materiales_comprados_unicos else []

    with st.form("form_asignar_material", clear_on_submit=True):
        fecha_asignacion = st.date_input("Fecha de Asignación", value=datetime.date.today(), key="asig_fecha")
        selected_obra_label_assign = st.selectbox(
            "Seleccione Obra de Destino:", options=obra_assign_labels, index=default_obra_assign_index, key="asig_obra_selectbox_persistent"
        )
        obra_destino_id = obra_assign_label_to_id.get(selected_obra_label_assign)
        if obra_destino_id is None or str(obra_destino_id) not in obras_disponibles_assign_list:
             st.warning(f"La obra '{selected_obra_label_assign}' no es válida.")
             if "asig_obra_selectbox_persistent" in st.session_state: del st.session_state["asig_obra_selectbox_persistent"]
             st.experimental_rerun()
             return
        material_input_method = st.radio("¿Cómo seleccionar material?", ["Seleccionar de compras", "Escribir manualmente"], key="material_input_method_radio")
        material_asignado = None
        suggested_price = 0.0
        if material_input_method == "Seleccionar de compras":
             if material_options_select:
                  material_asignado = st.selectbox("Material a Asignar:", material_options_select, key="asig_material_select")
                  if material_asignado and material_asignado != '' and material_asignado != "Seleccionar material...":
                      last_purchase = st.session_state.df_compras_materiales[
                           st.session_state.df_compras_materiales['Material'].astype(str).str.strip().str.lower() == material_asignado.lower().strip()
                       ].copy()
                      date_col_name_compra = DATETIME_COLUMNS[TABLE_COMPRAS_MATERIALES]
                      if date_col_name_compra in last_purchase.columns:
                           last_purchase[date_col_name_compra] = pd.to_datetime(last_purchase[date_col_name_compra], errors='coerce')
                           last_purchase = last_purchase.sort_values(date_col_name_compra, ascending=False)
                      if not last_purchase.empty and 'Precio_Unitario_Comprado' in last_purchase.columns:
                          last_purchase['Precio_Unitario_Comprado'] = pd.to_numeric(last_purchase['Precio_Unitario_Comprado'], errors='coerce')
                          suggested_price_series = last_purchase['Precio_Unitario_Comprado'].dropna()
                          suggested_price = suggested_price_series.iloc[0] if not suggested_price_series.empty else 0.0
                      else: suggested_price = 0.0
                  else: suggested_price = 0.0
             else:
                  st.info("No hay materiales en compras. Use 'Escribir manualmente'.")
                  material_input_method = "Escribir manualmente"
                  st.session_state.material_input_method_radio = "Escribir manualmente"
                  st.experimental_rerun()
                  return
        if material_input_method == "Escribir manualmente":
             material_asignado = st.text_input("Nombre del Material a Asignar", key="asig_material_manual").strip()
             suggested_price = 0.0
        price_input_key = "asig_precio"
        price_edited_flag_key = f"{price_input_key}_user_edited"
        material_selection_changed = False
        if material_input_method == "Seleccionar de compras":
             last_selected_material = st.session_state.get("last_selected_asig_material_select")
             if last_selected_material != material_asignado: material_selection_changed = True
             st.session_state.last_selected_asig_material_select = material_asignado
        else:
             if st.session_state.get("last_material_input_method") == "Seleccionar de compras" and material_input_method == "Escribir manualmente": material_selection_changed = True
             if "last_selected_asig_material_select" in st.session_state: del st.session_state["last_selected_asig_material_select"]
        st.session_state.last_material_input_method = material_input_method
        if "submitted_form_asignar_material" not in st.session_state: st.session_state.submitted_form_asignar_material = False # Initialize
        if st.session_state.submitted_form_asignar_material or material_selection_changed: # If form was submitted or material changed
             st.session_state[price_edited_flag_key] = False
             st.session_state.submitted_form_asignar_material = False # Reset submission flag

        if not st.session_state.get(price_edited_flag_key, False):
             st.session_state.current_asig_price_suggestion = suggested_price

        cantidad_asignada = st.number_input("Cantidad a Asignar", min_value=0.0, value=st.session_state.get("asig_cantidad", 0.0), step=0.01, format="%.2f", key="asig_cantidad") # Removed required
        precio_unitario_asignado = st.number_input(
            "Precio Unitario Asignado (Costo Real)", min_value=0.0, format="%.2f", key=price_input_key,
            value=st.session_state.get(price_input_key, st.session_state.get('current_asig_price_suggestion', 0.0)),
            on_change=lambda: st.session_state.update({price_edited_flag_key: True})
        ) # Removed required

        submitted_assign = st.form_submit_button("Asignar Material")
        if submitted_assign:
             st.session_state.submitted_form_asignar_material = True # Set submission flag
             if 'current_asig_price_suggestion' in st.session_state: del st.session_state['current_asig_price_suggestion']
             if price_edited_flag_key in st.session_state: del st.session_state[price_edited_flag_key]
             if "last_selected_asig_material_select" in st.session_state: del st.session_state["last_selected_asig_material_select"]
             if "last_material_input_method" in st.session_state: del st.session_state["last_material_input_method"]

             if obra_destino_id is None or str(obra_destino_id).strip() == '':
                  st.warning("Por favor, seleccione una obra de destino válida.")
             elif not fecha_asignacion:
                 st.warning("Por favor, complete la fecha de asignación.")
             elif not material_asignado or str(material_asignado).strip() == '' or material_asignado == "Seleccionar material...":
                  st.warning("Por favor, complete el nombre del material.")
             elif cantidad_asignada is None or precio_unitario_asignado is None: # Added None check
                  st.warning("Por favor, complete cantidad y precio unitario asignado.")
             elif (cantidad_asignada < 0 or precio_unitario_asignado < 0):
                  st.warning("Cantidad y Precio Unitario deben ser >= 0.")
             elif cantidad_asignada == 0 and precio_unitario_asignado == 0:
                  st.warning("Cantidad y precio no pueden ser ambos cero.")
             else:
                  existing_ids = set(st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str).tolist())
                  base_id = f"ASIG_{int(time.time() * 1e6)}"
                  id_asignacion = base_id
                  counter = 0
                  while id_asignacion in existing_ids:
                      counter += 1
                      id_asignacion = f"{base_id}_{counter}"
                  new_asignacion_data = {
                      'ID_Asignacion': id_asignacion, 'Fecha_Asignacion': fecha_asignacion, 'ID_Obra': obra_destino_id,
                      'Material': str(material_asignado).strip(),
                      'Cantidad_Asignada': float(cantidad_asignada if cantidad_asignada is not None else 0.0), # Handle None
                      'Precio_Unitario_Asignado': float(precio_unitario_asignado if precio_unitario_asignado is not None else 0.0) # Handle None
                  }
                  new_asignacion_df = pd.DataFrame([new_asignacion_data])
                  new_asignacion_df = calcular_costo_asignado(new_asignacion_df)
                  expected_cols_asignacion = list(TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].keys())
                  new_asignacion_df = new_asignacion_df.reindex(columns=expected_cols_asignacion)
                  date_col_name_asignacion = DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]
                  if date_col_name_asignacion in new_asignacion_df.columns:
                       new_asignacion_df[date_col_name_asignacion] = pd.to_datetime(new_asignacion_df[date_col_name_asignacion], errors='coerce')
                  for col, dtype in TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].items():
                      if col in new_asignacion_df.columns:
                            try:
                                 if dtype == 'object':
                                      new_asignacion_df[col] = new_asignacion_df[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object)
                                      new_asignacion_df[col] = new_asignacion_df[col].replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
                                 elif 'float' in dtype:
                                      new_asignacion_df[col] = pd.to_numeric(new_asignacion_df[col], errors='coerce').astype(float).fillna(0.0)
                                 elif 'int' in dtype:
                                      if hasattr(pd, 'Int64Dtype'): new_asignacion_df[col] = pd.to_numeric(new_asignacion_df[col], errors='coerce').astype(pd.Int64Dtype()).fillna(pd.NA)
                                      else: new_asignacion_df[col] = pd.to_numeric(new_asignacion_df[col], errors='coerce').astype(float).fillna(0.0)
                            except Exception as dtype_e:
                                st.warning(f"No se pudo convertir la nueva columna '{col}' a dtype '{dtype}': {dtype_e}")
                  df_current_asignacion_reindexed = st.session_state.df_asignacion_materiales.reindex(columns=expected_cols_asignacion)
                  st.session_state.df_asignacion_materiales = pd.concat([df_current_asignacion_reindexed, new_asignacion_df], ignore_index=True)
                  save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)
                  obra_name_row = st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'].astype(str) == str(obra_destino_id)].iloc[0] if str(obra_destino_id) in st.session_state.df_proyectos['ID_Obra'].astype(str).tolist() else None
                  obra_name_for_success = obra_name_row['Nombre_Obra'] if obra_name_row is not None and 'Nombre_Obra' in obra_name_row and pd.notna(obra_name_row['Nombre_Obra']) else f"Obra ID: {obra_destino_id}"
                  st.success(f"Material '{material_asignado}' ({cantidad_asignada:.2f} unidades) asignado a obra '{obra_name_for_success}'.")
                  st.experimental_rerun()

    st.subheader("Historial de Asignaciones")
    # ... (rest of page_compras_asignacion, data_editor and delete logic)
    # The data_editor here uses NumberColumn which is fine.
    # For brevity, skipping this large section. It was included in the previous response.
    # Pasting it again for completeness.
    if st.session_state.df_asignacion_materiales.empty:
        st.info("No hay materiales asignados aún.")
    else:
        st.info("Edite la tabla siguiente para modificar o eliminar asignaciones.")
        df_asignaciones_editable = st.session_state.df_asignacion_materiales.copy()
        date_col_name_asignacion = DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]
        if date_col_name_asignacion in df_asignaciones_editable.columns:
             df_asignaciones_editable[date_col_name_asignacion] = pd.to_datetime(df_asignaciones_editable[date_col_name_asignacion], errors='coerce')
        else:
             df_asignaciones_editable[date_col_name_asignacion] = pd.Series(dtype='datetime64[ns]', index=df_asignaciones_editable.index)
        for col in ['Cantidad_Asignada', 'Precio_Unitario_Asignado']:
             if col not in df_asignaciones_editable.columns: df_asignaciones_editable[col] = 0.0
             df_asignaciones_editable[col] = pd.to_numeric(df_asignaciones_editable[col], errors='coerce').fillna(0.0)
        df_asignaciones_editable = calcular_costo_asignado(df_asignaciones_editable)
        obra_ids_for_editor = obras_disponibles_assign_list
        expected_cols_asignacion = list(TABLE_COLUMNS[TABLE_ASIGNACION_MATERIALES].keys())
        df_asignaciones_editable = df_asignaciones_editable.reindex(columns=expected_cols_asignacion)
        for col in ['ID_Asignacion', 'ID_Obra', 'Material']:
             if col in df_asignaciones_editable.columns:
                  df_asignaciones_editable[col] = df_asignaciones_editable[col].astype(pd.StringDtype() if hasattr(pd, 'StringDtype') else object).replace({np.nan: pd.NA, None: pd.NA, '': pd.NA})
        if not obra_ids_for_editor:
             st.warning("No hay obras válidas. Tabla de asignaciones se mostrará sin opción de editar Obra.")
             display_cols_asig_non_editable = [col for col in expected_cols_asignacion if col != 'ID_Obra']
             st.dataframe(df_asignaciones_editable[display_cols_asig_non_editable])
             return
        default_obra_editor_value = obra_ids_for_editor[0] if obra_ids_for_editor else None
        if default_obra_editor_value is not None:
             df_asignaciones_editable['ID_Obra'] = df_asignaciones_editable['ID_Obra'].apply(
                  lambda x: default_obra_editor_value if pd.isna(x) else x
             )
        df_asignaciones_edited = st.data_editor(
            df_asignaciones_editable, key="data_editor_asignaciones", num_rows="dynamic",
             column_config={
                 "ID_Asignacion": st.column_config.TextColumn("ID Asignación", disabled=True),
                 date_col_name_asignacion: st.column_config.DateColumn("Fecha Asignación", required=True),
                 "ID_Obra": st.column_config.SelectboxColumn("ID Obra", options=obra_ids_for_editor, required=True),
                 "Material": st.column_config.TextColumn("Material", required=True),
                 "Cantidad_Asignada": st.column_config.NumberColumn("Cantidad Asignada", min_value=0.0, format="%.2f", required=True),
                 "Precio_Unitario_Asignado": st.column_config.NumberColumn("Precio Unitario Asignado", min_value=0.0, format="%.2f", required=True),
                 "Costo_Asignado": st.column_config.NumberColumn("Costo Asignado", disabled=True, format="%.2f")
             }
         )
        df_asignaciones_edited_processed = df_asignaciones_edited.copy()
        df_asignaciones_edited_processed = df_asignaciones_edited_processed.reindex(columns=expected_cols_asignacion)
        for col in ['ID_Asignacion', 'ID_Obra', 'Material']:
            if col in df_asignaciones_edited_processed.columns:
                df_asignaciones_edited_processed[col] = df_asignaciones_edited_processed[col].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_asignaciones_edited_processed[col].isna(), None)
        for col in ['Cantidad_Asignada', 'Precio_Unitario_Asignado']:
            if col not in df_asignaciones_edited_processed.columns: df_asignaciones_edited_processed[col] = 0.0
            df_asignaciones_edited_processed[col] = pd.to_numeric(df_asignaciones_edited_processed[col], errors='coerce').fillna(0.0)
        df_asignaciones_edited_processed = calcular_costo_asignado(df_asignaciones_edited_processed)
        new_row_mask = df_asignaciones_edited_processed['ID_Asignacion'].isna()
        if new_row_mask.any():
            existing_ids = set(st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str).tolist())
            new_ids_batch = []
            for i in range(new_row_mask.sum()):
                base_id = f"ASIG_EDIT_{int(time.time() * 1e6)}_{i}"
                unique_id = base_id
                counter = 0
                while unique_id in existing_ids or unique_id in new_ids_batch:
                    counter += 1
                    unique_id = f"{base_id}_{counter}"
                new_ids_batch.append(unique_id)
            df_asignaciones_edited_processed.loc[new_row_mask, 'ID_Asignacion'] = new_ids_batch
        df_asignaciones_original_compare = st.session_state.df_asignacion_materiales.reindex(columns=expected_cols_asignacion).sort_values(by=expected_cols_asignacion).reset_index(drop=True)
        df_asignaciones_edited_compare = df_asignaciones_edited_processed.reindex(columns=expected_cols_asignacion).sort_values(by=expected_cols_asignacion).reset_index(drop=True)
        if not df_asignaciones_edited_compare.equals(df_asignaciones_original_compare):
            if st.button("Guardar Cambios en Historial de Asignaciones", key="save_asignaciones_button"):
                df_to_save = df_asignaciones_edited_processed.copy()
                date_col_name_asignacion = DATETIME_COLUMNS[TABLE_ASIGNACION_MATERIALES]
                df_to_save = df_to_save[(df_to_save['ID_Asignacion'].notna()) &
                                        (df_to_save[date_col_name_asignacion].notna()) &
                                        (df_to_save['ID_Obra'].notna()) &
                                        (df_to_save['Material'].notna()) &
                                        (df_to_save['Cantidad_Asignada'].notna()) &
                                        (df_to_save['Precio_Unitario_Asignado'].notna())
                                        ].copy()
                if df_to_save.empty and not df_asignaciones_edited_processed.empty:
                    st.error("Error: Ninguna fila válida. Complete campos obligatorios.")
                elif ((pd.to_numeric(df_to_save['Cantidad_Asignada'], errors='coerce').fillna(0) == 0) &
                    (pd.to_numeric(df_to_save['Precio_Unitario_Asignado'], errors='coerce').fillna(0) == 0)).any():
                    st.warning("Advertencia: Algunas asignaciones tienen Cantidad y Precio Unitario ambos cero.")
                elif df_to_save['ID_Asignacion'].astype(str).str.strip().duplicated().any():
                    st.error("Error: IDs de asignación duplicados.")
                else:
                    st.session_state.df_asignacion_materiales = df_to_save
                    save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)
                    st.success("Cambios en historial de asignaciones guardados.")
                    st.experimental_rerun()
            else:
                st.info("Hay cambios sin guardar en el historial de asignaciones.")
        st.markdown("---")
        st.subheader("Eliminar Asignación por ID")
        asignaciones_disponibles_list_current = st.session_state.df_asignacion_materiales['ID_Asignacion'].unique().tolist()
        asignaciones_disponibles_list_current = [str(id).strip() for id in asignaciones_disponibles_list_current if pd.notna(id) and str(id).strip() != '']
        asignaciones_disponibles_list_current.sort()
        if not asignaciones_disponibles_list_current:
            st.info("No hay asignaciones para eliminar por ID.")
        else:
            info_cols = ['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada']
            info_cols_present = [col for col in info_cols if col in st.session_state.df_asignacion_materiales.columns]
            df_asig_info = st.session_state.df_asignacion_materiales[info_cols_present].copy()
            if 'ID_Asignacion' in df_asig_info.columns:
                 df_asig_info['ID_Asignacion_clean'] = df_asig_info['ID_Asignacion'].astype(str).str.strip().replace({'': 'ID Desconocida', 'nan': 'ID Desconocida', 'None': 'ID Desconocida'})
                 df_asig_info = df_asig_info[df_asig_info['ID_Asignacion_clean'].isin(asignaciones_disponibles_list_current)].copy()
            else:
                 st.warning("La tabla de asignaciones no contiene 'ID_Asignacion'.")
                 df_asig_info = pd.DataFrame({'ID_Asignacion_clean': asignaciones_disponibles_list_current})
                 for col in info_cols[1:]: df_asig_info[col] = 'No Disp.'
            if not df_asig_info.empty:
                 if 'Fecha_Asignacion' in df_asig_info.columns:
                     df_asig_info['Fecha_Asignacion_str'] = pd.to_datetime(df_asig_info['Fecha_Asignacion'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('Fecha Inválida')
                 else: df_asig_info['Fecha_Asignacion_str'] = 'Fecha No Disp.'
                 for col in ['ID_Obra', 'Material']:
                      if col in df_asig_info.columns: df_asig_info[col] = df_asig_info[col].astype(str).str.strip().replace({'': 'N/A', 'nan': 'N/A', 'None': 'N/A'})
                      else: df_asig_info[col] = 'No Disp.'
                 if 'Cantidad_Asignada' in df_asig_info.columns:
                      df_asig_info['Cantidad_Asignada'] = pd.to_numeric(df_as_ig_info['Cantidad_Asignada'], errors='coerce').fillna(0.0).round(2)
                      df_asig_info['Cantidad_Asignada_str'] = df_asig_info['Cantidad_Asignada'].astype(str)
                 else: df_asig_info['Cantidad_Asignada_str'] = 'No Disp.'
                 asig_options_dict = df_asig_info.set_index('ID_Asignacion_clean').to_dict('index')
            else: asig_options_dict = {}
            def format_assignment_option_display(asig_id):
                asig_id_clean = str(asig_id).strip()
                info = asig_options_dict.get(asig_id_clean, {})
                fecha_str = info.get('Fecha_Asignacion_str', 'Fecha No Disp.')
                obra_id = info.get('ID_Obra', 'Obra No Disp.')
                material = info.get('Material', 'Material No Disp.')
                cantidad = info.get('Cantidad_Asignada_str', 'Cant. No Disp.')
                info_parts_for_display = []
                if fecha_str != 'Fecha No Disp.' and fecha_str != 'Fecha Inválida': info_parts_for_display.append(fecha_str)
                if obra_id != 'Obra No Disp.' and obra_id != 'N/A': info_parts_for_display.append(f"Obra: {obra_id}")
                if material != 'Material No Disp.' and material != 'N/A': info_parts_for_display.append(f"Mat: {material}")
                if cantidad != 'Cant. No Disp.' and cantidad != '0.0': info_parts_for_display.append(f"Cant: {cantidad}")
                if info_parts_for_display: return f"{asig_id_clean} ({' | '.join(info_parts_for_display)})"
                else: return f"{asig_id_clean} (Detalles No Disponibles)"
            id_asignacion_eliminar = st.selectbox(
                "Seleccione ID de Asignación a eliminar:", options=asignaciones_disponibles_list_current,
                format_func=format_assignment_option_display, key="eliminar_asig_select"
            )
            if st.button(f"Eliminar Asignación Seleccionada", key="eliminar_asig_button"):
                 selected_id_clean = str(id_asignacion_eliminar).strip()
                 df_filtered = st.session_state.df_asignacion_materiales[
                     st.session_state.df_asignacion_materiales['ID_Asignacion'].astype(str).str.strip() != selected_id_clean
                 ].copy()
                 if len(df_filtered) < len(st.session_state.df_asignacion_materiales):
                     st.session_state.df_asignacion_materiales = df_filtered
                     save_table(st.session_state.df_asignacion_materiales, DATABASE_FILE, TABLE_ASIGNACION_MATERIALES)
                     st.success(f"Asignación {id_asignacion_eliminar} eliminada.")
                     st.experimental_rerun()
                 else:
                     st.warning(f"No se encontró la asignación con ID {id_asignacion_eliminar}.")

def page_reporte_variacion_total_obras():
    st.title("Reporte de Variación Total Obras (Presupuesto vs Real)")
    # This page does calculations and displays dataframes/charts. No direct st.number_input with 'required'.
    # For brevity, skipping. It was included in the previous response.
    # Pasting it again for completeness.
    st.write("Compara el costo total presupuestado vs el costo total real (asignado) para cada obra.")
    if st.session_state.df_presupuesto_materiales.empty and st.session_state.df_asignacion_materiales.empty:
        st.info("No hay datos de presupuesto ni de asignación para generar el reporte.")
        return
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
        if col not in df_presupuesto.columns: df_presupuesto[col] = 0.0
        df_presupuesto[col] = pd.to_numeric(df_presupuesto[col], errors='coerce').fillna(0.0)
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto)
    if 'ID_Obra' in df_presupuesto.columns:
        df_presupuesto['ID_Obra_clean'] = df_presupuesto['ID_Obra'].astype(str).str.strip().replace({'': 'ID Desconocida', 'nan': 'ID Desconocida', 'None': 'ID Desconocida'})
    else: df_presupuesto['ID_Obra_clean'] = 'ID Desconocida'
    if 'Cantidad_Presupuestada' not in df_presupuesto.columns: df_presupuesto['Cantidad_Presupuestada'] = 0.0
    if 'Costo_Presupuestado' not in df_presupuesto.columns: df_presupuesto['Costo_Presupuestado'] = 0.0
    df_presupuesto['Cantidad_Presupuestada'] = pd.to_numeric(df_presupuesto['Cantidad_Presupuestada'], errors='coerce').fillna(0.0)
    df_presupuesto['Costo_Presupuestado'] = pd.to_numeric(df_presupuesto['Costo_Presupuestado'], errors='coerce').fillna(0.0)
    if not df_presupuesto.empty:
        presupuesto_total_obra = df_presupuesto.groupby('ID_Obra_clean', dropna=False).agg(
            Cantidad_Total_Presupuestada=('Cantidad_Presupuestada', 'sum'),
            Costo_Total_Presupuestado=('Costo_Presupuestado', 'sum')
        ).reset_index()
    else:
         presupuesto_total_obra = pd.DataFrame(columns=['ID_Obra_clean', 'Cantidad_Presupuestada_Total', 'Costo_Presupuestado_Total'])
    df_asignacion = st.session_state.df_asignacion_materiales.copy()
    for col in ['Cantidad_Asignada', 'Precio_Unitario_Asignado']:
        if col not in df_asignacion.columns: df_asignacion[col] = 0.0
        df_asignacion[col] = pd.to_numeric(df_asignacion[col], errors='coerce').fillna(0.0)
    df_asignacion = calcular_costo_asignado(df_asignacion)
    if 'ID_Obra' in df_asignacion.columns:
         df_asignacion['ID_Obra_clean'] = df_asignacion['ID_Obra'].astype(str).str.strip().replace({'': 'ID Desconocida', 'nan': 'ID Desconocida', 'None': 'ID Desconocida'})
    else: df_asignacion['ID_Obra_clean'] = 'ID Desconocida'
    if 'Costo_Asignado' not in df_asignacion.columns: df_asignacion['Costo_Asignado'] = 0.0
    df_asignacion['Costo_Asignado'] = pd.to_numeric(df_asignacion['Costo_Asignado'], errors='coerce').fillna(0.0)
    if not df_asignacion.empty:
         asignacion_total_obra = df_asignacion.groupby('ID_Obra_clean', dropna=False).agg(
            Cantidad_Asignada_Total=('Cantidad_Asignada', 'sum'),
            Costo_Asignado_Total=('Costo_Asignado', 'sum')
        ).reset_index()
    else:
         asignacion_total_obra = pd.DataFrame(columns=['ID_Obra_clean', 'Cantidad_Asignada_Total', 'Costo_Asignado_Total'])
    reporte_variacion_obras = pd.merge(presupuesto_total_obra, asignacion_total_obra, on='ID_Obra_clean', how='outer').fillna(0)
    df_proyectos_temp = st.session_state.df_proyectos.copy()
    if 'ID_Obra' in df_proyectos_temp.columns:
         df_proyectos_temp['ID_Obra_clean_for_merge'] = df_proyectos_temp['ID_Obra'].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).mask(df_proyectos_temp['ID_Obra'].isna(), None)
         reporte_variacion_obras = reporte_variacion_obras.merge(df_proyectos_temp[['ID_Obra_clean_for_merge', 'Nombre_Obra']], left_on='ID_Obra_clean', right_on='ID_Obra_clean_for_merge', how='left')
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras.apply(
             lambda row: row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) else f"Obra ID: {row['ID_Obra_clean']}" if row['ID_Obra_clean'] != 'ID Desconocida' else 'ID Desconocida', axis=1
         )
         reporte_variacion_obras = reporte_variacion_obras.drop(columns=['ID_Obra_clean_for_merge'], errors='ignore')
    else:
         reporte_variacion_obras['Nombre_Obra'] = reporte_variacion_obras['ID_Obra_clean'].apply(lambda x: f"Obra ID: {x}" if x != 'ID Desconocida' else 'ID Desconocida')
    reporte_variacion_obras = reporte_variacion_obras.rename(columns={'ID_Obra_clean': 'ID_Obra'})
    cost_cols = ['Costo_Presupuestado_Total', 'Costo_Asignado_Total']
    qty_cols = ['Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total']
    for col in cost_cols + qty_cols:
         if col not in reporte_variacion_obras.columns: reporte_variacion_obras[col] = 0.0
         else: reporte_variacion_obras[col] = pd.to_numeric(reporte_variacion_obras[col], errors='coerce').fillna(0.0)
    reporte_variacion_obras['Variacion_Total_Costo'] = reporte_variacion_obras['Costo_Asignado_Total'] - reporte_variacion_obras['Costo_Presupuestado_Total']
    reporte_variacion_obras['Variacion_Total_Cantidad'] = reporte_variacion_obras['Cantidad_Asignada_Total'] - reporte_variacion_obras['Cantidad_Presupuestada_Total']
    sort_cols = []
    if 'Nombre_Obra' in reporte_variacion_obras.columns: sort_cols.append('Nombre_Obra')
    if 'ID_Obra' in reporte_variacion_obras.columns: sort_cols.append('ID_Obra')
    if sort_cols:
         reporte_variacion_obras = reporte_variacion_obras.sort_values(by=sort_cols).reset_index(drop=True)
    st.subheader("Variación de Costo y Cantidad por Obra (Presupuesto vs Real)")
    if reporte_variacion_obras.empty:
        st.info("No hay datos válidos para generar el reporte de variación por obra.")
    else:
        display_cols = ['Nombre_Obra', 'ID_Obra', 'Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total', 'Variacion_Total_Cantidad',
                        'Costo_Presupuestado_Total', 'Costo_Asignado_Total', 'Variacion_Total_Costo']
        for col in display_cols:
             if col not in reporte_variacion_obras.columns: reporte_variacion_obras[col] = pd.NA
        display_cols_present = [col for col in display_cols if col in reporte_variacion_obras.columns]
        if display_cols_present: st.dataframe(reporte_variacion_obras[display_cols_present].round(2))
        else: st.warning("No se pudo mostrar el reporte de variación por obra.")
        total_presupuestado_general = pd.to_numeric(reporte_variacion_obras.get('Costo_Presupuestado_Total', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        total_asignado_general = pd.to_numeric(reporte_variacion_obras.get('Costo_Asignado_Total', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        total_variacion_general_costo = total_asignado_general - total_presupuestado_general
        variation_threshold_general = 0.01
        if abs(total_variacion_general_costo) >= variation_threshold_general or abs(total_presupuestado_general) >= variation_threshold_general or abs(total_asignado_general) >= variation_threshold_general:
            st.subheader("Gráfico de Cascada: Presupuesto Total vs Costo Real Total")
            labels_costo = ['Total Presupuestado']
            values_costo = [total_presupuestado_general]
            measures_costo = ['absolute']
            texts_costo = [f"${total_presupuestado_general:,.2f}"]
            if 'Variacion_Total_Costo' in reporte_variacion_obras.columns and 'Nombre_Obra' in reporte_variacion_obras.columns and 'ID_Obra' in reporte_variacion_obras.columns:
                reporte_variacion_obras_significant_cost_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Costo']) >= variation_threshold_general].sort_values('Variacion_Total_Costo', ascending=False).copy()
                for index, row in reporte_variacion_obras_significant_cost_var.iterrows():
                     obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) and str(row['Nombre_Obra']).strip() != '' else str(row['ID_Obra']) + ' (Desconocida)'
                     if len(obra_label) > 25: obra_label = obra_label[:22] + '...'
                     labels_costo.append(f"Var: {obra_label}")
                     values_costo.append(row['Variacion_Total_Costo'])
                     measures_costo.append('relative')
                     texts_costo.append(f"${row['Variacion_Total_Costo']:,.2f}")
            labels_costo.append('Total Asignado')
            values_costo.append(total_asignado_general)
            measures_costo.append('total')
            texts_costo.append(f"${total_asignado_general:,.2f}") # Corrected this line
            if (len(labels_costo) > 2) or (len(labels_costo) == 2 and abs(values_costo[0] - values_costo[1]) >= variation_threshold_general) or (len(labels_costo) == 2 and abs(values_costo[0]) >= variation_threshold_general):
                fig_total_variacion_costo = go.Figure(go.Waterfall(
                    name = "Variación Total Costo", orientation = "v", measure = measures_costo, x = labels_costo,
                    textposition = "outside", text = texts_costo, y = values_costo, connector = {"line":{"color":"rgb(63, 63, 63)"}},
                    increasing = {"marker":{"color":"#FF4136"}}, decreasing = {"marker":{"color":"#3D9970"}},
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
                ))
                fig_total_variacion_costo.update_layout(
                    title = 'Variación Total de Costos Materiales (Presupuesto vs Real)', showlegend = False,
                    yaxis_title="Monto ($)", margin=dict(l=20, r=20, t=60, b=20), height=600
                )
                st.plotly_chart(fig_total_variacion_costo, use_container_width=True)
            elif abs(total_presupuestado_general) < variation_threshold_general and abs(total_asignado_general) < variation_threshold_general:
                 st.info("El presupuesto y costo asignado son cero o insignificantes.")
            elif abs(total_presupuestado_general) >= variation_threshold_general and abs(total_asignado_general - total_presupuestado_general) < variation_threshold_general:
                 st.info("El costo asignado es igual al presupuestado o la variación es insignificante.")
            else: st.info("No hay datos de costos suficientes para mostrar el gráfico.")
        else: st.info("No hay costo presupuestado ni asignado total para mostrar el gráfico.")
        total_cantidad_presupuestada_general = pd.to_numeric(reporte_variacion_obras.get('Cantidad_Presupuestada_Total', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        total_cantidad_asignada_general = pd.to_numeric(reporte_variacion_obras.get('Cantidad_Asignada_Total', pd.Series(dtype=float)), errors='coerce').fillna(0.0).sum()
        total_variacion_general_cantidad = total_cantidad_asignada_general - total_cantidad_presupuestada_general
        if abs(total_variacion_general_cantidad) >= variation_threshold_general or abs(total_cantidad_presupuestada_general) >= variation_threshold_general or abs(total_cantidad_asignada_general) >= variation_threshold_general:
            st.subheader("Gráfico de Cascada: Cantidad Total Presupuestada vs Cantidad Real Total")
            labels_cantidad = ['Total Presupuestado (Cant.)']
            values_cantidad = [total_cantidad_presupuestada_general]
            measures_cantidad = ['absolute']
            texts_cantidad = [f"{total_cantidad_presupuestada_general:,.2f}"]
            if 'Variacion_Total_Cantidad' in reporte_variacion_obras.columns and 'Nombre_Obra' in reporte_variacion_obras.columns and 'ID_Obra' in reporte_variacion_obras.columns:
                reporte_variacion_obras_significant_qty_var = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Cantidad']) >= variation_threshold_general].sort_values('Variacion_Total_Cantidad', ascending=False).copy()
                for index, row in reporte_variacion_obras_significant_qty_var.iterrows():
                     obra_label = row['Nombre_Obra'] if pd.notna(row['Nombre_Obra']) and str(row['Nombre_Obra']).strip() != '' else str(row['ID_Obra']) + ' (Desconocida)'
                     if len(obra_label) > 25: obra_label = obra_label[:22] + '...'
                     labels_cantidad.append(f"Var Cant: {obra_label}")
                     values_cantidad.append(row['Variacion_Total_Cantidad'])
                     measures_cantidad.append('relative')
                     texts_cantidad.append(f"{row['Variacion_Total_Cantidad']:,.2f}")
            labels_cantidad.append('Total Asignado (Cant.)')
            values_cantidad.append(total_cantidad_asignada_general)
            measures_cantidad.append('total')
            texts_cantidad.append(f"{total_cantidad_asignada_general:,.2f}") # Corrected this line
            if (len(labels_cantidad) > 2) or (len(labels_cantidad) == 2 and abs(values_cantidad[0] - values_cantidad[1]) >= variation_threshold_general) or (len(labels_cantidad) == 2 and abs(values_cantidad[0]) >= variation_threshold_general):
                fig_total_variacion_cantidad = go.Figure(go.Waterfall(
                    name = "Variación Total Cantidad", orientation = "v", measure = measures_cantidad, x = labels_cantidad,
                    textposition = "outside", text = texts_cantidad, y = values_cantidad, connector = {"line":{"color":"rgb(63, 63, 63)"}},
                    increasing = {"marker":{"color":"#FF4136"}}, decreasing = {"marker":{"color":"#3D9970"}},
                    totals = {"marker":{"color":"#0074D9", "line":{"color":"#fff", "width":3}}}
                ))
                fig_total_variacion_cantidad.update_layout(
                    title = 'Variación Total de Cantidades Materiales (Presupuesto vs Real)', showlegend = False,
                    yaxis_title="Cantidad", margin=dict(l=20, r=20, t=60, b=20), height=600
                )
                st.plotly_chart(fig_total_variacion_cantidad, use_container_width=True)
            elif abs(total_cantidad_presupuestada_general) < variation_threshold_general and abs(total_cantidad_asignada_general) < variation_threshold_general:
                 st.info("La cantidad presupuestada y asignada son cero o insignificantes.")
            elif abs(total_cantidad_presupuestada_general) >= variation_threshold_general and abs(total_cantidad_asignada_general - total_cantidad_presupuestada_general) < variation_threshold_general:
                 st.info("La cantidad asignada es igual a la presupuestada o la variación es insignificante.")
            else: st.info("No hay datos de cantidades suficientes para mostrar el gráfico.")
        else: st.info("No hay cantidad presupuestada ni asignada total para mostrar el gráfico.")

# --- Main App Logic ---
with st.sidebar:
    st.title("Menú Principal")
    pages = {
        "Dashboard Principal": "dashboard", "Gestión de Flotas": "gestion_flotas", "Gestión de Equipos": "equipos",
        "Registro de Consumibles": "consumibles", "Registro de Costos Equipos": "costos_equipos",
        "Reportes Mina (Consumo/Costo)": "reportes_mina", "Variación Costos Flota": "variacion_costos_flota",
        "--- Gestión de Obras y Materiales ---": None, "Gestión de Obras (Proyectos)": "gestion_obras",
        "Reporte Presupuesto Total Obras": "reporte_presupuesto_total_obras",
        "Gestión Compras y Asignación": "compras_asignacion", "Reporte Variación Total Obras": "reporte_variacion_total_obras",
    }
    selected_page_key = st.radio("Ir a:", list(pages.keys()), index=0, key="main_navigation_radio")
    selected_page = pages[selected_page_key]

if selected_page == "dashboard":
    st.title("Dashboard Principal")
    st.write(f"Bienvenido al sistema de gestión.")
    st.info("Seleccione una opción del menú lateral para comenzar.")
    st.markdown("---")
    st.subheader("Resumen Rápido")
    total_equipos = len(st.session_state.get('df_equipos', pd.DataFrame()).dropna(subset=['Interno']).copy())
    total_obras = len(st.session_state.get('df_proyectos', pd.DataFrame()).dropna(subset=['ID_Obra']).copy())
    total_flotas = len(st.session_state.get('df_flotas', pd.DataFrame()).dropna(subset=['ID_Flota']).copy())
    df_presupuesto_summary = st.session_state.get('df_presupuesto_materiales', pd.DataFrame()).copy()
    for col in ['Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']:
        if col not in df_presupuesto_summary.columns: df_presupuesto_summary[col] = 0.0
        df_presupuesto_summary[col] = pd.to_numeric(df_presupuesto_summary[col], errors='coerce').fillna(0.0)
    df_presupuesto_summary = calcular_costo_presupuestado(df_presupuesto_summary)
    total_presupuesto_materiales = pd.to_numeric(df_presupuesto_summary.get('Costo_Presupuestado', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()
    df_compras_summary = st.session_state.get('df_compras_materiales', pd.DataFrame()).copy()
    for col in ['Cantidad_Comprada', 'Precio_Unitario_Comprado']:
        if col not in df_compras_summary.columns: df_compras_summary[col] = 0.0
        df_compras_summary[col] = pd.to_numeric(df_compras_summary[col], errors='coerce').fillna(0.0)
    df_compras_summary = calcular_costo_compra(df_compras_summary)
    total_comprado_materiales = pd.to_numeric(df_compras_summary.get('Costo_Compra', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()
    col_summary1, col_summary2, col_summary3, col_summary4, col_summary5 = st.columns(5)
    with col_summary1: st.metric("Total Equipos", total_equipos)
    with col_summary2: st.metric("Total Flotas", total_flotas)
    with col_summary3: st.metric("Total Obras", total_obras)
    with col_summary4: st.metric("Presupuesto Materiales Total", f"${total_presupuesto_materiales:,.0f}")
    with col_summary5: st.metric("Compras Materiales Total", f"${total_comprado_materiales:,.0f}")
elif selected_page == "gestion_flotas": page_flotas()
elif selected_page == "equipos": page_equipos()
elif selected_page == "consumibles": page_consumibles()
elif selected_page == "costos_equipos": page_costos_equipos()
elif selected_page == "reportes_mina": page_reportes_mina()
elif selected_page == "variacion_costos_flota": page_variacion_costos_flota()
elif selected_page == "gestion_obras": page_gestion_obras()
elif selected_page == "reporte_presupuesto_total_obras": page_reporte_presupuesto_total_obras()
elif selected_page == "compras_asignacion": page_compras_asignacion()
elif selected_page == "reporte_variacion_total_obras": page_reporte_variacion_total_obras()
elif selected_page is None: st.empty()

