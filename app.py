import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
# import hashlib # No necesario si usas streamlit-authenticator.Hasher
import streamlit_authenticator as stauth

# --- Configuración Inicial ---
st.set_page_config(layout="wide", page_title="Gestión de Equipos y Obras (Minería)")

# --- Archivos de Datos ---
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

FILE_EQUIPOS = os.path.join(DATA_DIR, "equipos.xlsx")
FILE_CONSUMO = os.path.join(DATA_DIR, "consumo.xlsx")
FILE_COSTOS_SALARIAL = os.path.join(DATA_DIR, "costos_salarial.xlsx")
FILE_GASTOS_FIJOS = os.path.join(DATA_DIR, "gastos_fijos.xlsx")
FILE_GASTOS_MANTENIMIENTO = os.path.join(DATA_DIR, "gastos_mantenimiento.xlsx")
FILE_PRECIOS_COMBUSTIBLE = os.path.join(DATA_DIR, "precios_combustible.xlsx")
FILE_PROYECTOS = os.path.join(DATA_DIR, "proyectos.xlsx")
FILE_PRESUPUESTO_MATERIALES = os.path.join(DATA_DIR, "presupuesto_materiales.xlsx")
FILE_COMPRAS_MATERIALES = os.path.join(DATA_DIR, "compras_materiales.xlsx")
FILE_ASIGNACION_MATERIALES = os.path.join(DATA_DIR, "asignacion_materiales.xlsx")

# --- Funciones para Cargar/Guardar Datos ---
# Mejorada para manejar posibles errores al leer
def load_data(file_path, expected_columns=None):
    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
            # Convertir columnas de fecha si existen
            for col in df.columns:
                if 'fecha' in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col]).dt.date # Almacenar solo la fecha
                    except Exception:
                        # Ignorar errores de conversión si la columna no es una fecha válida
                        pass
            # Opcional: Verificar si las columnas esperadas están presentes
            if expected_columns:
                missing_cols = [col for col in expected_columns if col not in df.columns]
                if missing_cols:
                    st.warning(f"Archivo {file_path} no contiene las columnas esperadas: {missing_cols}. Se usará un DataFrame vacío o se añadirán al guardar.")
                    return pd.DataFrame(columns=expected_columns) # Retorna vacío si faltan columnas críticas
            return df
        except FileNotFoundError:
            st.warning(f"Archivo no encontrado al intentar cargar: {file_path}")
            return pd.DataFrame(columns=expected_columns if expected_columns else [])
        except Exception as e:
            st.error(f"Error al leer el archivo {file_path}: {e}")
            return pd.DataFrame(columns=expected_columns if expected_columns else [])
    return pd.DataFrame(columns=expected_columns if expected_columns else []) # Retorna DataFrame vacío si el archivo no existe

def save_data(df, file_path):
    # Asegurar que el directorio exista antes de guardar
    data_dir = os.path.dirname(file_path)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    try:
        df.to_excel(file_path, index=False, engine='openpyxl')
        # st.success(f"Datos guardados en {os.path.basename(file_path)}") # Demasiado verboso, comentar
    except Exception as e:
        st.error(f"Error al guardar el archivo {file_path}: {e}")


# --- Cargar todos los DataFrames al inicio (si no están en session_state) ---
# Usamos st.session_state para mantener los datos a través de las interacciones del usuario
# Añadimos expected_columns para inicializar correctamente si el archivo no existe o está vacío/corrupto

if 'df_equipos' not in st.session_state:
    st.session_state.df_equipos = load_data(FILE_EQUIPOS, expected_columns=['Interno', 'Patente'])

if 'df_consumo' not in st.session_state:
    st.session_state.df_consumo = load_data(FILE_CONSUMO, expected_columns=['Interno', 'Fecha', 'Consumo_Litros', 'Horas_Trabajadas', 'Kilometros_Recorridos'])
    # Ensure date type, handling potential NaT from empty column or bad data
    if not st.session_state.df_consumo.empty:
        st.session_state.df_consumo['Fecha'] = pd.to_datetime(st.session_state.df_consumo['Fecha'], errors='coerce').dt.date.dropna() # Ensure date type and remove invalid dates

if 'df_costos_salarial' not in st.session_state:
    st.session_state.df_costos_salarial = load_data(FILE_COSTOS_SALARIAL, expected_columns=['Interno', 'Fecha', 'Monto_Salarial'])
    if not st.session_state.df_costos_salarial.empty:
         st.session_state.df_costos_salarial['Fecha'] = pd.to_datetime(st.session_state.df_costos_salarial['Fecha'], errors='coerce').dt.date.dropna()

if 'df_gastos_fijos' not in st.session_state:
    st.session_state.df_gastos_fijos = load_data(FILE_GASTOS_FIJOS, expected_columns=['Interno', 'Fecha', 'Tipo_Gasto_Fijo', 'Monto_Gasto_Fijo', 'Descripcion'])
    if not st.session_state.df_gastos_fijos.empty:
         st.session_state.df_gastos_fijos['Fecha'] = pd.to_datetime(st.session_state.df_gastos_fijos['Fecha'], errors='coerce').dt.date.dropna()

if 'df_gastos_mantenimiento' not in st.session_state:
    st.session_state.df_gastos_mantenimiento = load_data(FILE_GASTOS_MANTENIMIENTO, expected_columns=['Interno', 'Fecha', 'Tipo_Mantenimiento', 'Monto_Mantenimiento', 'Descripcion'])
    if not st.session_state.df_gastos_mantenimiento.empty:
         st.session_state.df_gastos_mantenimiento['Fecha'] = pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha'], errors='coerce').dt.date.dropna()

if 'df_precios_combustible' not in st.session_state:
    st.session_state.df_precios_combustible = load_data(FILE_PRECIOS_COMBUSTIBLE, expected_columns=['Fecha', 'Precio_Litro'])
    if not st.session_state.df_precios_combustible.empty:
         st.session_state.df_precios_combustible['Fecha'] = pd.to_datetime(st.session_state.df_precios_combustible['Fecha'], errors='coerce').dt.date.dropna()

if 'df_proyectos' not in st.session_state:
    st.session_state.df_proyectos = load_data(FILE_PROYECTOS, expected_columns=['ID_Obra', 'Nombre_Obra', 'Responsable'])
    if st.session_state.df_proyectos.empty:
         st.session_state.df_proyectos['ID_Obra'] = [] # Ensure ID column exists even if empty

if 'df_presupuesto_materiales' not in st.session_state:
    st.session_state.df_presupuesto_materiales = load_data(FILE_PRESUPUESTO_MATERIALES, expected_columns=['ID_Obra', 'Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado'])
    if 'Costo_Presupuestado' not in st.session_state.df_presupuesto_materiales.columns:
         st.session_state.df_presupuesto_materiales['Costo_Presupuestado'] = 0.0 # Add column if missing

if 'df_compras_materiales' not in st.session_state:
    st.session_state.df_compras_materiales = load_data(FILE_COMPRAS_MATERIALES, expected_columns=['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado'])
    if 'Costo_Compra' not in st.session_state.df_compras_materiales.columns:
         st.session_state.df_compras_materiales['Costo_Compra'] = 0.0 # Add column if missing
    if 'ID_Compra' not in st.session_state.df_compras_materiales.columns or st.session_state.df_compras_materiales['ID_Compra'].nunique() != len(st.session_state.df_compras_materiales):
         # Regenerate simple unique IDs if needed (e.g., loaded from file without unique IDs)
         st.session_state.df_compras_materiales['ID_Compra'] = [f"COMPRA_{int(pd.Timestamp.now().timestamp() * 1000)}_{i}" for i in range(len(st.session_state.df_compras_materiales))]

if 'df_asignacion_materiales' not in st.session_state:
    st.session_state.df_asignacion_materiales = load_data(FILE_ASIGNACION_MATERIALES, expected_columns=['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado'])
    if 'Costo_Asignado' not in st.session_state.df_asignacion_materiales.columns:
         st.session_state.df_asignacion_materiales['Costo_Asignado'] = 0.0 # Add column if missing
    if 'ID_Asignacion' not in st.session_state.df_asignacion_materiales.columns or st.session_state.df_asignacion_materiales['ID_Asignacion'].nunique() != len(st.session_state.df_asignacion_materiales):
         # Regenerate simple unique IDs if needed
         st.session_state.df_asignacion_materiales['ID_Asignacion'] = [f"ASIG_{int(pd.Timestamp.now().timestamp() * 1000)}_{i}" for i in range(len(st.session_state.df_asignacion_materiales))]


# --- Helper para calcular costos ---
# Se asegura de no fallar si alguna columna falta temporalmente
def calcular_costo_presupuestado(df):
    """Calcula el costo total presupuestado por fila."""
    if 'Cantidad_Presupuestada' in df.columns and 'Precio_Unitario_Presupuestado' in df.columns:
        df['Costo_Presupuestado'] = df['Cantidad_Presupuestada'] * df['Precio_Unitario_Presupuestado']
    else:
        df['Costo_Presupuestado'] = 0.0 # Añadir columna si no existe
    return df

def calcular_costo_compra(df):
    """Calcula el costo total de compra por fila."""
    if 'Cantidad_Comprada' in df.columns and 'Precio_Unitario_Comprado' in df.columns:
        df['Costo_Compra'] = df['Cantidad_Comprada'] * df['Precio_Unitario_Comprado']
    else:
        df['Costo_Compra'] = 0.0 # Añadir columna si no existe
    return df

def calcular_costo_asignado(df):
    """Calcula el costo total asignado por fila."""
    if 'Cantidad_Asignada' in df.columns and 'Precio_Unitario_Asignado' in df.columns:
        df['Costo_Asignado'] = df['Cantidad_Asignada'] * df['Precio_Unitario_Asignado']
    else:
        df['Costo_Asignado'] = 0.0 # Añadir columna si no existe
    return df


# Aplicar cálculos iniciales si los DataFrames no estaban vacíos
if not st.session_state.df_presupuesto_materiales.empty:
    st.session_state.df_presupuesto_materiales = calcular_costo_presupuestado(st.session_state.df_presupuesto_materiales)
if not st.session_state.df_compras_materiales.empty:
    st.session_state.df_compras_materiales = calcular_costo_compra(st.session_state.df_compras_materiales)
if not st.session_state.df_asignacion_materiales.empty:
    st.session_state.df_asignacion_materiales = calcular_costo_asignado(st.session_state.df_asignacion_materiales)


# --- Configuración de Usuarios y Autenticación ---
# Usa hash_gen.py para generar estos hashes:
# import streamlit_authenticator as stauth
# passwords = ['contraseña_user1', 'contraseña_user2', 'contraseña_user3', 'contraseña_user4'] # Reemplaza con las contraseñas reales
# hashed = stauth.Hasher(passwords).generate()
# print(hashed)
hashed_passwords = [
    # PEGA AQUÍ LOS HASHES GENERADOS
    '$2b$12$EXAMPLEHASHFORUSER1HERE..................', # Hash para user1
    '$2b$12$EXAMPLEHASHFORUSER2HERE..................', # Hash para user2
    '$2b$12$EXAMPLEHASHFORUSER3HERE..................', # Hash para user3
    '$2b$12$EXAMPLEHASHFORUSER4HERE..................'  # Hash para user4
]


authenticator = stauth.Authenticate(
    {
        'usernames':{
            'user1':{'name':'Usuario Uno','password':hashed_passwords[0]},
            'user2':{'name':'Usuario Dos','password':hashed_passwords[1]},
            'user3':{'name':'Usuario Tres','password':hashed_passwords[2]},
            'user4':{'name':'Usuario Cuatro','password':hashed_passwords[3]}
        }
    },
    'mining_dashboard_cookie', # Nombre de la cookie
    'abcdefgh', # Clave de la cookie (CÁMBIALA por una cadena aleatoria segura de al menos 32 caracteres)
    cookie_expiry_days=30
)


# --- Funciones para cada "Página" (sin cambios, se llaman desde el flujo principal) ---

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
                    save_data(st.session_state.df_equipos, FILE_EQUIPOS)
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
    # Esto es un poco complejo, una forma simple es siempre reemplazar el DF en session_state
    # si el editor devuelve algo diferente.
    if not df_equipos_edited.equals(st.session_state.df_equipos):
         st.session_state.df_equipos = df_equipos_edited
         if st.button("Guardar Cambios en Lista de Equipos"):
              # Validar antes de guardar (ej. Internos únicos)
              if st.session_state.df_equipos['Interno'].duplicated().any():
                  st.error("Error: Hay Internos de Equipo duplicados en la lista. Por favor, corrija los duplicados antes de guardar.")
              elif st.session_state.df_equipos['Interno'].isnull().any() or st.session_state.df_equipos['Patente'].isnull().any():
                  st.error("Error: Hay campos 'Interno' o 'Patente' vacíos. Por favor, complete la información faltante.")
              else:
                  save_data(st.session_state.df_equipos, FILE_EQUIPOS)
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
                    'Fecha': fecha,
                    'Consumo_Litros': consumo_litros,
                    'Horas_Trabajadas': horas_trabajadas,
                    'Kilometros_Recorridos': kilometros_recorridos
                 }])
                 new_consumo['Fecha'] = pd.to_datetime(new_consumo['Fecha']).dt.date

                 st.session_state.df_consumo = pd.concat([st.session_state.df_consumo, new_consumo], ignore_index=True)
                 save_data(st.session_state.df_consumo, FILE_CONSUMO)
                 st.success("Registro de consumo añadido.")
            else:
                st.warning("Por favor, complete todos los campos y añada al menos un valor (Litros, Horas o Kilómetros).")

    st.subheader("Registros de Consumo Existente")
    df_consumo_editable = st.session_state.df_consumo.copy()
    df_consumo_edited = st.data_editor(
         df_consumo_editable,
         key="data_editor_consumo",
         num_rows="dynamic"
         # Column config could be added here for specific types/formats
     )
    if not df_consumo_edited.equals(st.session_state.df_consumo):
         st.session_state.df_consumo = df_consumo_edited
         if st.button("Guardar Cambios en Registros de Consumo"):
              save_data(st.session_state.df_consumo, FILE_CONSUMO)
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
                       'Fecha': fecha,
                       'Monto_Salarial': monto_salarial
                    }])
                    new_costo['Fecha'] = pd.to_datetime(new_costo['Fecha']).dt.date
                    st.session_state.df_costos_salarial = pd.concat([st.session_state.df_costos_salarial, new_costo], ignore_index=True)
                    save_data(st.session_state.df_costos_salarial, FILE_COSTOS_SALARIAL)
                    st.success("Costo salarial registrado.")
                else:
                    st.warning("Por favor, complete todos los campos.")
        st.subheader("Registros Salariales Existente")
        df_salarial_editable = st.session_state.df_costos_salarial.copy()
        df_salarial_edited = st.data_editor(
            df_salarial_editable,
            key="data_editor_salarial",
            num_rows="dynamic"
        )
        if not df_salarial_edited.equals(st.session_state.df_costos_salarial):
             st.session_state.df_costos_salarial = df_salarial_edited
             if st.button("Guardar Cambios en Registros Salariales"):
                 save_data(st.session_state.df_costos_salarial, FILE_COSTOS_SALARIAL)
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
                       'Fecha': fecha,
                       'Tipo_Gasto_Fijo': tipo_gasto,
                       'Monto_Gasto_Fijo': monto_gasto,
                       'Descripcion': descripcion
                    }])
                    new_gasto['Fecha'] = pd.to_datetime(new_gasto['Fecha']).dt.date
                    st.session_state.df_gastos_fijos = pd.concat([st.session_state.df_gastos_fijos, new_gasto], ignore_index=True)
                    save_data(st.session_state.df_gastos_fijos, FILE_GASTOS_FIJOS)
                    st.success("Gasto fijo registrado.")
                else:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto).")
        st.subheader("Registros de Gastos Fijos Existente")
        df_fijos_editable = st.session_state.df_gastos_fijos.copy()
        df_fijos_edited = st.data_editor(
            df_fijos_editable,
            key="data_editor_fijos",
            num_rows="dynamic"
        )
        if not df_fijos_edited.equals(st.session_state.df_gastos_fijos):
             st.session_state.df_gastos_fijos = df_fijos_edited
             if st.button("Guardar Cambios en Registros de Gastos Fijos"):
                 save_data(st.session_state.df_gastos_fijos, FILE_GASTOS_FIJOS)
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
                       'Fecha': fecha,
                       'Tipo_Mantenimiento': tipo_mantenimiento,
                       'Monto_Mantenimiento': monto_mantenimiento,
                       'Descripcion': descripcion
                    }])
                    new_gasto['Fecha'] = pd.to_datetime(new_gasto['Fecha']).dt.date
                    st.session_state.df_gastos_mantenimiento = pd.concat([st.session_state.df_gastos_mantenimiento, new_gasto], ignore_index=True)
                    save_data(st.session_state.df_gastos_mantenimiento, FILE_GASTOS_MANTENIMIENTO)
                    st.success("Gasto de mantenimiento registrado.")
                else:
                    st.warning("Por favor, complete los campos obligatorios (Equipo, Fecha, Tipo, Monto).")
        st.subheader("Registros de Gastos de Mantenimiento Existente")
        df_mantenimiento_editable = st.session_state.df_gastos_mantenimiento.copy()
        df_mantenimiento_edited = st.data_editor(
            df_mantenimiento_editable,
            key="data_editor_mantenimiento",
            num_rows="dynamic"
        )
        if not df_mantenimiento_edited.equals(st.session_state.df_gastos_mantenimiento):
             st.session_state.df_gastos_mantenimiento = df_mantenimiento_edited
             if st.button("Guardar Cambios en Registros de Mantenimiento"):
                 save_data(st.session_state.df_gastos_mantenimiento, FILE_GASTOS_MANTENIMIENTO)
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
                new_precio = pd.DataFrame([{'Fecha': fecha_precio, 'Precio_Litro': precio_litro}])
                new_precio['Fecha'] = pd.to_datetime(new_precio['Fecha']).dt.date
                # Reemplazar si la fecha ya existe, de lo contrario añadir
                st.session_state.df_precios_combustible = st.session_state.df_precios_combustible[
                    st.session_state.df_precios_combustible['Fecha'] != fecha_precio
                ]
                st.session_state.df_precios_combustible = pd.concat([st.session_state.df_precios_combustible, new_precio], ignore_index=True)
                save_data(st.session_state.df_precios_combustible, FILE_PRECIOS_COMBUSTIBLE)
                st.success("Precio del combustible registrado/actualizado.")
            else:
                st.warning("Por favor, complete la fecha y el precio.")
    st.subheader("Precios del Combustible Existente")
    df_precios_editable = st.session_state.df_precios_combustible.copy()
    df_precios_edited = st.data_editor(
        df_precios_editable,
        key="data_editor_precios",
        num_rows="dynamic"
    )
    if not df_precios_edited.equals(st.session_state.df_precios_combustible):
         st.session_state.df_precios_combustible = df_precios_edited
         if st.button("Guardar Cambios en Precios de Combustible"):
              # Opcional: Validar fechas únicas si cada fecha debe tener un solo precio
              if st.session_state.df_precios_combustible['Fecha'].duplicated().any():
                   st.error("Error: Hay fechas duplicadas en los precios de combustible. Por favor, corrija los duplicados antes de guardar.")
              else:
                   save_data(st.session_state.df_precios_combustible, FILE_PRECIOS_COMBUSTIBLE)
                   st.success("Cambios en precios de combustible guardados.")
         else:
             st.info("Hay cambios sin guardar en precios de combustible.")


    st.subheader("Reporte por Rango de Fechas")
    col1, col2 = st.columns(2)
    min_date = pd.to_datetime(st.session_state.df_consumo['Fecha']).min() if not st.session_state.df_consumo.empty else pd.Timestamp.now().date()
    max_date = pd.to_datetime(st.session_state.df_consumo['Fecha']).max() if not st.session_state.df_consumo.empty else pd.Timestamp.now().date()

    with col1:
        fecha_inicio = st.date_input("Fecha de Inicio del Reporte", min_date)
    with col2:
        fecha_fin = st.date_input("Fecha de Fin del Reporte", max_date)

    if st.button("Generar Reporte"):
        if fecha_inicio > fecha_fin:
            st.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
            return

        # Asegurar que las fechas en los DFs son datetime para el filtro
        # Coerce errors para manejar fechas no válidas si las hubiera
        df_consumo_dt = st.session_state.df_consumo.copy()
        df_consumo_dt['Fecha'] = pd.to_datetime(df_consumo_dt['Fecha'], errors='coerce').dropna()

        df_precios_dt = st.session_state.df_precios_combustible.copy()
        df_precios_dt['Fecha'] = pd.to_datetime(df_precios_dt['Fecha'], errors='coerce').dropna()

        df_salarial_dt = st.session_state.df_costos_salarial.copy()
        df_salarial_dt['Fecha'] = pd.to_datetime(df_salarial_dt['Fecha'], errors='coerce').dropna()

        df_fijos_dt = st.session_state.df_gastos_fijos.copy()
        df_fijos_dt['Fecha'] = pd.to_datetime(df_fijos_dt['Fecha'], errors='coerce').dropna()

        df_mantenimiento_dt = st.session_state.df_gastos_mantenimiento.copy()
        df_mantenimiento_dt['Fecha'] = pd.to_datetime(df_mantenimiento_dt['Fecha'], errors='coerce').dropna()


        # Filtrar por fecha
        df_consumo_filtrado = df_consumo_dt[(df_consumo_dt['Fecha'] >= pd.to_datetime(fecha_inicio)) & (df_consumo_dt['Fecha'] <= pd.to_datetime(fecha_fin))].copy()
        salarial_filtrado = df_salarial_dt[(df_salarial_dt['Fecha'] >= pd.to_datetime(fecha_inicio)) & (df_salarial_dt['Fecha'] <= pd.to_datetime(fecha_fin))].copy()
        fijos_filtrado = df_fijos_dt[(df_fijos_dt['Fecha'] >= pd.to_datetime(fecha_inicio)) & (df_fijos_dt['Fecha'] <= pd.to_datetime(fecha_fin))].copy()
        mantenimiento_filtrado = df_mantenimiento_dt[(df_mantenimiento_dt['Fecha'] >= pd.to_datetime(fecha_inicio)) & (df_mantenimiento_dt['Fecha'] <= pd.to_datetime(fecha_fin))].copy()


        if df_consumo_filtrado.empty:
            st.info("No hay datos de consumo en el rango de fechas seleccionado.")
            # Mostrar otros costos si existen, incluso sin consumo
            # return # No retornamos aquí para mostrar al menos los costos

        # Calcular métricas por equipo y fecha en el periodo
        df_consumo_filtrado['Consumo_L_H'] = df_consumo_filtrado.apply(
            lambda row: row['Consumo_Litros'] / row['Horas_Trabajadas'] if row['Horas_Trabajadas'] > 0 else 0, axis=1
        )
        df_consumo_filtrado['Consumo_L_KM'] = df_consumo_filtrado.apply(
             lambda row: row['Consumo_Litros'] / row['Kilometros_Recorridos'] if row['Kilometros_Recorridos'] > 0 else 0, axis=1
        )

        # Unir con precios de combustible (usando el precio más reciente antes o en la fecha de consumo)
        df_precios_dt_sorted = df_precios_dt.sort_values('Fecha')

        # Merge asof para unir cada fecha de consumo con el precio de combustible más reciente <= a esa fecha
        # Asegurarse de que 'Fecha' sea el índice o columna para merge_asof
        if not df_consumo_filtrado.empty:
             reporte_consumo = pd.merge_asof(
                 df_consumo_filtrado.sort_values('Fecha'),
                 df_precios_dt_sorted,
                 on='Fecha',
                 by='Interno' if 'Interno' in df_precios_dt_sorted.columns else None, # Intenta unir por Interno si es posible (precio por equipo)
                 direction='backward'
             )
             # Calcular costo del combustible
             reporte_consumo['Costo_Combustible'] = reporte_consumo['Consumo_Litros'] * reporte_consumo['Precio_Litro'].fillna(0) # Si no hay precio, costo es 0
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
                  lambda row: row['Total_Consumo_Litros'] / row['Total_Kilometros'] if row['Total_Kilometros'] > 0 else 0, axis=1
             )

             # Unir con información de equipos (Patente)
             reporte_resumen_consumo = reporte_resumen_consumo.merge(st.session_state.df_equipos[['Interno', 'Patente']], on='Interno', how='left')

             st.subheader(f"Reporte Consumo y Costo Combustible ({fecha_inicio} a {fecha_fin})")
             st.dataframe(reporte_resumen_consumo[[
                 'Interno', 'Patente', 'Total_Consumo_Litros', 'Total_Horas', 'Total_Kilometros',
                 'Avg_Consumo_L_H', 'Avg_Consumo_L_KM', 'Costo_Total_Combustible'
             ]].round(2))
        else:
             st.info("No hay datos de consumo para calcular L/H, L/KM o costo de combustible en este período.")
             reporte_resumen_consumo = pd.DataFrame(columns=['Interno', 'Costo_Total_Combustible']) # DataFrame vacío para el merge posterior

        # --- Sumar otros costos (Salarial, Fijos, Mantenimiento) en el periodo ---
        # Agrupar por Interno
        salarial_agg = salarial_filtrado.groupby('Interno')['Monto_Salarial'].sum().reset_index(name='Total_Salarial')
        fijos_agg = fijos_filtrado.groupby('Interno')['Monto_Gasto_Fijo'].sum().reset_index(name='Total_Gastos_Fijos')
        mantenimiento_agg = mantenimiento_filtrado.groupby('Interno')['Monto_Mantenimiento'].sum().reset_index(name='Total_Gastos_Mantenimiento')

        # Unir todos los costos
        # Empezar con la lista única de equipos que tuvieron ALGÚN costo o consumo en el periodo
        all_internos_in_period = pd.concat([
            df_consumo_filtrado['Interno'],
            salarial_filtrado['Interno'],
            fijos_filtrado['Interno'],
            mantenimiento_filtrado['Interno']
        ]).unique()
        df_all_internos = pd.DataFrame(all_internos_in_period, columns=['Interno'])

        reporte_costo_total = df_all_internos.merge(reporte_resumen_consum[['Interno', 'Costo_Total_Combustible']], on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(salarial_agg, on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(fijos_agg, on='Interno', how='left').fillna(0)
        reporte_costo_total = reporte_costo_total.merge(mantenimiento_agg, on='Interno', how='left').fillna(0)

        # Añadir Patente
        reporte_costo_total = reporte_costo_total.merge(st.session_state.df_equipos[['Interno', 'Patente']], on='Interno', how='left')


        reporte_costo_total['Costo_Total_Equipo'] = reporte_costo_total['Costo_Total_Combustible'] + reporte_costo_total['Total_Salarial'] + reporte_costo_total['Total_Gastos_Fijos'] + reporte_costo_total['Total_Gastos_Mantenimiento']

        st.subheader(f"Reporte Costo Total por Equipo ({fecha_inicio} a {fecha_fin})")
        if reporte_costo_total.empty:
             st.info("No hay datos de costos (Combustible, Salarial, Fijos, Mantenimiento) en el rango de fechas seleccionado.")
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
    if not st.session_state.df_consumo.empty: all_cost_dates = all_cost_dates.union(pd.to_datetime(st.session_state.df_consumo['Fecha']).dt.date)
    if not st.session_state.df_costos_salarial.empty: all_cost_dates = all_cost_dates.union(pd.to_datetime(st.session_state.df_costos_salarial['Fecha']).dt.date)
    if not st.session_state.df_gastos_fijos.empty: all_cost_dates = all_cost_dates.union(pd.to_datetime(st.session_state.df_gastos_fijos['Fecha']).dt.date)
    if not st.session_state.df_gastos_mantenimiento.empty: all_cost_dates = all_cost_dates.union(pd.to_datetime(st.session_state.df_gastos_mantenimiento['Fecha']).dt.date)

    if not all_cost_dates.empty:
        min_app_date = min(all_cost_dates)
        max_app_date = max(all_cost_dates)
        # Suggest recent months
        default_end_p2 = max_app_date
        default_start_p2 = default_end_p2 - pd.Timedelta(days=30)
        default_end_p1 = default_start_p2 - pd.Timedelta(days=1)
        default_start_p1 = default_end_p1 - pd.Timedelta(days=30)

    else:
        min_app_date = pd.Timestamp.now().date() - pd.Timedelta(days=90)
        max_app_date = pd.Timestamp.now().date()
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
        if fecha_inicio_p1 >= fecha_fin_p1 or fecha_inicio_p2 >= fecha_fin_p2:
             st.error("Las fechas dentro de cada período no son válidas.")
             return
        if fecha_fin_p1 >= fecha_inicio_p2:
             st.warning("Los períodos se solapan o son adyacentes. Considere usar rangos no solapados para una mejor visualización de la variación.")
             # return # Allow generating but warn

        # --- Calcular Costos por Período y Categoría ---
        # Helper function to aggregate costs for a given date range
        def aggregate_costs(df, date_col, start_date, end_date):
            # Ensure date column is datetime and handle errors
            df_dt = df.copy()
            df_dt[date_col] = pd.to_datetime(df_dt[date_col], errors='coerce').dropna() # Remove rows with invalid dates
            # Filter
            df_filtered = df_dt[(df_dt[date_col] >= pd.to_datetime(start_date)) & (df_dt[date_col] <= pd.to_datetime(end_date))]
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
             consumo_p1_sorted = consumo_p1.sort_values('Fecha')
             precios_p1_sorted = precios_p1.sort_values('Fecha')
             # Merge asof - using date only is likely sufficient for fleet total
             consumo_p1_merged = pd.merge_asof(consumo_p1_sorted, precios_p1_sorted, on='Fecha', direction='backward')
             costo_combustible_p1 = (consumo_p1_merged['Consumo_Litros'] * consumo_p1_merged['Precio_Litro'].fillna(0)).sum()


        costo_salarial_p1 = salarial_p1['Monto_Salarial'].sum() if not salarial_p1.empty else 0
        costo_fijos_p1 = fijos_p1['Monto_Gasto_Fijo'].sum() if not fijos_p1.empty else 0
        costo_mantenimiento_p1 = mantenimiento_p1['Monto_Mantenimiento'].sum() if not mantenimiento_p1.empty else 0

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

        costo_salarial_p2 = salarial_p2['Monto_Salarial'].sum() if not salarial_p2.empty else 0
        costo_fijos_p2 = fijos_p2['Monto_Gasto_Fijo'].sum() if not fijos_p2.empty else 0
        costo_mantenimiento_p2 = mantenimiento_p2['Monto_Mantenimiento'].sum() if not mantenimiento_p2.empty else 0

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
        if abs(variacion_combustible) > 0.01: # Usar umbral pequeño para evitar variaciones insignificantes
            labels.append('Variación Combustible')
            measures.append('relative')
            values.append(variacion_combustible)
            texts.append(f"${variacion_combustible:,.2f}")

        if abs(variacion_salarial) > 0.01:
            labels.append('Variación Salarial')
            measures.append('relative')
            values.append(variacion_salarial)
            texts.append(f"${variacion_salarial:,.2f}")

        if abs(variacion_fijos) > 0.01:
            labels.append('Variación Fijos')
            measures.append('relative')
            values.append(variacion_fijos)
            texts.append(f"${variacion_fijos:,.2f}")

        if abs(variacion_mantenimiento) > 0.01:
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
        if len(labels) <= 1: # Only the starting point
             st.info("No hay datos o variación significativa para mostrar el gráfico de cascada en los períodos seleccionados.")
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
                 title = f'Variación de Costos de Flota: {fecha_inicio_p1}-{fecha_fin_p1} vs {fecha_inicio_p2}-{fecha_fin_p2}',
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

        if total_costo_p1 != total_costo_p2:
            st.subheader("Variaciones Absolutas")
            st.write(f"- Combustible: ${variacion_combustible:,.2f}")
            st.write(f"- Salarial: ${variacion_salarial:,.2f}")
            st.write(f"- Fijos: ${variacion_fijos:,.2f}")
            st.write(f"- Mantenimiento: ${variacion_mantenimiento:,.2f}")
            st.write(f"**Variación Total: ${total_costo_p2 - total_costo_p1:,.2f}**")
        else:
             st.info("Los costos totales entre los dos períodos son iguales.")


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
                id_obra = f"OBRA_{int(pd.Timestamp.now().timestamp())}_{len(st.session_state.df_proyectos)}"
                new_obra = pd.DataFrame([{'ID_Obra': id_obra, 'Nombre_Obra': nombre_obra, 'Responsable': responsable}])
                st.session_state.df_proyectos = pd.concat([st.session_state.df_proyectos, new_obra], ignore_index=True)
                save_data(st.session_state.df_proyectos, FILE_PROYECTOS)
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
        if not df_proyectos_edited.equals(st.session_state.df_proyectos):
             st.session_state.df_proyectos = df_proyectos_edited
             if st.button("Guardar Cambios en Lista de Obras"):
                 # Simple validation
                 if st.session_state.df_proyectos['Nombre_Obra'].isnull().any() or st.session_state.df_proyectos['Responsable'].isnull().any():
                      st.error("Error: Los campos 'Nombre Obra' y 'Responsable' no pueden estar vacíos.")
                 else:
                      save_data(st.session_state.df_proyectos, FILE_PROYECTOS)
                      st.success("Cambios en la lista de obras guardados.")
             else:
                 st.info("Hay cambios sin guardar en la lista de obras.")

        st.markdown("---")
        st.subheader("Gestionar Presupuesto por Obra")
        obras_disponibles = st.session_state.df_proyectos['ID_Obra'].tolist()
        obra_seleccionada_id = st.selectbox(
            "Seleccione una Obra:",
            obras_disponibles,
            format_func=lambda x: f"{st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x]['Nombre_Obra'].iloc[0]} (ID: {x})" if not st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == x].empty else x,
            key="select_obra_gestion"
        )

        if obra_seleccionada_id:
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
                         save_data(st.session_state.df_presupuesto_materiales, FILE_PRESUPUESTO_MATERIALES)
                         st.success(f"Material '{material}' añadido al presupuesto de la obra.")
                         # No need to manually update df_presupuesto_obra here, Streamlit rerun handles it.
                    else:
                        st.warning("Por favor, complete todos los campos para añadir material.")

            # Mostrar y editar presupuesto existente (data_editor)
            st.write("Editar presupuesto existente:")
            df_presupuesto_obra_editable = df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']].copy() # Solo columnas editables
            df_presupuesto_obra_edited = st.data_editor(
                df_presupuesto_obra_editable,
                key=f"data_editor_presupuesto_{obra_seleccionada_id}",
                num_rows="dynamic",
                column_config={
                    "Material": st.column_config.TextColumn("Material", required=True),
                    "Cantidad_Presupuestada": st.column_config.NumberColumn("Cantidad Presupuestada", min_value=0.0, format="%.2f", required=True),
                    "Precio_Unitario_Presupuestado": st.column_config.NumberColumn("Precio Unitario Presupuestado", min_value=0.0, format="%.2f", required=True)
                }
            )

            # Lógica para guardar cambios del data_editor
            # Comparar con el original filtrado
            if not df_presupuesto_obra_edited.equals(df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado']]):
                 if st.button(f"Guardar Cambios en Presupuesto de {obra_nombre}"):
                     # Reconstruir el DataFrame principal para esta obra
                     df_rest_presupuesto = st.session_state.df_presupuesto_materiales[
                         st.session_state.df_presupuesto_materiales['ID_Obra'] != obra_seleccionada_id # Eliminar filas viejas de esta obra
                     ].copy()
                     # Añadir las filas editadas/añadidas (incluyendo la columna ID_Obra)
                     df_presupuesto_obra_edited['ID_Obra'] = obra_seleccionada_id
                     df_presupuesto_obra_edited = calcular_costo_presupuestado(df_presupuesto_obra_edited) # Recalcular costo total
                     st.session_state.df_presupuesto_materiales = pd.concat([df_rest_presupuesto, df_presupuesto_obra_edited], ignore_index=True)
                     save_data(st.session_state.df_presupuesto_materiales, FILE_PRESUPUESTO_MATERIALES)
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
                st.dataframe(df_presupuesto_obra[['Material', 'Cantidad_Presupuestada', 'Precio_Unitario_Presupuestado', 'Costo_Presupuestado']].round(2))

                total_cantidad_presupuestada = df_presupuesto_obra['Cantidad_Presupuestada'].sum()
                total_costo_presupuestado = df_presupuesto_obra['Costo_Presupuestado'].sum()

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
               presupuesto_agg = df_presupuesto_obra.groupby('Material').agg(
                   Cantidad_Presupuestada=('Cantidad_Presupuestada', 'sum'),
                   Costo_Presupuestado=('Costo_Presupuestado', 'sum')
               ).reset_index()

               # Agrupar asignaciones por material
               asignacion_agg = df_asignacion_obra.groupby('Material').agg(
                   Cantidad_Asignada=('Cantidad_Asignada', 'sum'),
                   Costo_Asignado=('Costo_Asignado', 'sum')
               ).reset_index()

               # Unir presupuesto y asignación
               variacion_obra = pd.merge(presupuesto_agg, asignacion_agg, on='Material', how='outer').fillna(0)

               # Calcular variaciones
               variacion_obra['Cantidad_Variacion'] = variacion_obra['Cantidad_Asignada'] - variacion_obra['Cantidad_Presupuestada']
               variacion_obra['Costo_Variacion'] = variacion_obra['Costo_Asignado'] - variacion_obra['Costo_Presupuestado']

               st.subheader("Reporte de Variación por Material")
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
               if abs(total_variacion_costo_obra) > 0.01:
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
                   st.info("El costo presupuestado y asignado para esta obra son iguales.")


def page_reporte_presupuesto_total_obras():
    st.title("Reporte de Presupuesto Total por Obras")
    st.write("Suma el presupuesto total de materiales de todas las obras.")

    if st.session_state.df_presupuesto_materiales.empty:
        st.info("No hay presupuesto de materiales registrado para ninguna obra.")
    else:
        # Asegurar que la columna calculada existe y los datos son numéricos
        df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
        df_presupuesto['Cantidad_Presupuestada'] = pd.to_numeric(df_presupuesto['Cantidad_Presupuestada'], errors='coerce').fillna(0)
        df_presupuesto['Precio_Unitario_Presupuestado'] = pd.to_numeric(df_presupuesto['Precio_Unitario_Presupuestado'], errors='coerce').fillna(0)
        df_presupuesto = calcular_costo_presupuestado(df_presupuesto)

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
                    'Fecha_Compra': fecha_compra,
                    'Material': material_compra,
                    'Cantidad_Comprada': cantidad_comprada,
                    'Precio_Unitario_Comprado': precio_unitario_comprado
                }])
                new_compra['Fecha_Compra'] = pd.to_datetime(new_compra['Fecha_Compra']).dt.date
                new_compra = calcular_costo_compra(new_compra)
                st.session_state.df_compras_materiales = pd.concat([st.session_state.df_compras_materiales, new_compra], ignore_index=True)
                save_data(st.session_state.df_compras_materiales, FILE_COMPRAS_MATERIALES)
                st.success(f"Compra de '{material_compra}' registrada con ID: {id_compra}")
            else:
                st.warning("Por favor, complete todos los campos de la compra.")

    st.subheader("Historial de Compras")
    if st.session_state.df_compras_materiales.empty:
        st.info("No hay compras registradas aún.")
    else:
         # Usar data_editor para permitir edición si se desea, o solo mostrar con dataframe
         df_compras_editable = st.session_state.df_compras_materiales.copy()
         df_compras_edited = st.data_editor(
             df_compras_editable[['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']],
             key="data_editor_compras",
             num_rows="dynamic",
             column_config={
                 "ID_Compra": st.column_config.TextColumn("ID Compra", disabled=True),
                 "Fecha_Compra": st.column_config.DateColumn("Fecha Compra", required=True),
                 "Material": st.column_config.TextColumn("Material", required=True),
                 "Cantidad_Comprada": st.column_config.NumberColumn("Cantidad Comprada", min_value=0.0, format="%.2f", required=True),
                 "Precio_Unitario_Comprado": st.column_config.NumberColumn("Precio Unitario Compra", min_value=0.0, format="%.2f", required=True),
                 "Costo_Compra": st.column_config.NumberColumn("Costo Compra", disabled=True, format="%.2f") # Calculado, no editable
             }
         )
         # Lógica de guardado para el editor
         if not df_compras_edited.equals(st.session_state.df_compras_materiales[['ID_Compra', 'Fecha_Compra', 'Material', 'Cantidad_Comprada', 'Precio_Unitario_Comprado', 'Costo_Compra']]):
             # Necesitamos unir los cambios con el DF original para mantener columnas ocultas o calculadas si las hubiera
             # Para este caso simple, podemos simplemente actualizar y recalcular el costo
             st.session_state.df_compras_materiales = df_compras_edited.copy()
             st.session_state.df_compras_materiales = calcular_costo_compra(st.session_state.df_compras_materiales)

             if st.button("Guardar Cambios en Historial de Compras"):
                 # Validar antes de guardar (ej. campos requeridos)
                 if st.session_state.df_compras_materiales['Material'].isnull().any() or st.session_state.df_compras_materiales['Cantidad_Comprada'].isnull().any() or st.session_state.df_compras_materiales['Precio_Unitario_Comprado'].isnull().any():
                      st.error("Error: Hay campos obligatorios vacíos en el historial de compras.")
                 else:
                      save_data(st.session_state.df_compras_materiales, FILE_COMPRAS_MATERIALES)
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
            # Opción 1: Seleccionar de comprados (más restrictivo pero consistente)
            # material_asignado = st.selectbox(
            #     "Material a Asignar:",
            #     materiales_comprados_unicos if materiales_comprados_unicos else ["(No hay materiales comprados)"],
            #     disabled=not materiales_comprados_unicos,
            #     key="asig_material"
            # )
            # Opción 2: Texto libre (permite asignar stock inicial o no ligado a compra específica)
            material_asignado = st.text_input("Material a Asignar").strip()


            cantidad_asignada = st.number_input("Cantidad a Asignar", min_value=0.0, format="%.2f", key="asig_cantidad")
            # Precio al que se ASIGNA (puede ser diferente al de compra, ej. costo promedio, o ingreso manual del costo real)
            precio_unitario_asignado = st.number_input("Precio Unitario Asignado (Costo Real)", min_value=0.0, format="%.2f", key="asig_precio")

            submitted = st.form_submit_button("Asignar Material")
            if submitted:
                if fecha_asignacion and obra_destino_id and material_asignado and cantidad_asignada >= 0 and precio_unitario_asignado >= 0: # Permitir cantidad 0 para registrar item en 0? No, mejor >0
                     if cantidad_asignada == 0 and precio_unitario_asignado == 0:
                         st.warning("La cantidad y el precio unitario asignado no pueden ser ambos cero a menos que represente un ítem sin costo/cantidad.")
                     else:
                          id_asignacion = f"ASIG_{int(pd.Timestamp.now().timestamp() * 1000)}_{len(st.session_state.df_asignacion_materiales)}" # Simple ID único
                          new_asignacion = pd.DataFrame([{
                              'ID_Asignacion': id_asignacion,
                              'Fecha_Asignacion': fecha_asignacion,
                              'ID_Obra': obra_destino_id,
                              'Material': material_asignado,
                              'Cantidad_Asignada': cantidad_asignada,
                              'Precio_Unitario_Asignado': precio_unitario_asignado
                          }])
                          new_asignacion['Fecha_Asignacion'] = pd.to_datetime(new_asignacion['Fecha_Asignacion']).dt.date
                          new_asignacion = calcular_costo_asignado(new_asignacion)
                          st.session_state.df_asignacion_materiales = pd.concat([st.session_state.df_asignacion_materiales, new_asignacion], ignore_index=True)
                          save_data(st.session_state.df_asignacion_materiales, FILE_ASIGNACION_MATERIALES)
                          st.success(f"Material '{material_asignado}' ({cantidad_asignada} unidades) asignado a obra '{st.session_state.df_proyectos[st.session_state.df_proyectos['ID_Obra'] == obra_destino_id]['Nombre_Obra'].iloc[0]}'.")
                else:
                    st.warning("Por favor, complete todos los campos de asignación.")

        st.subheader("Historial de Asignaciones")
        if st.session_state.df_asignacion_materiales.empty:
            st.info("No hay materiales asignados aún.")
        else:
            # Usar data_editor para permitir edición si se desea
             df_asignaciones_editable = st.session_state.df_asignacion_materiales.copy()
             df_asignaciones_edited = st.data_editor(
                 df_asignaciones_editable[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']],
                 key="data_editor_asignaciones",
                 num_rows="dynamic",
                  column_config={
                      "ID_Asignacion": st.column_config.TextColumn("ID Asignación", disabled=True),
                      "Fecha_Asignacion": st.column_config.DateColumn("Fecha Asignación", required=True),
                      "ID_Obra": st.column_config.TextColumn("ID Obra", required=True), # Podría ser selectbox si solo se asigna a obras existentes
                      "Material": st.column_config.TextColumn("Material", required=True),
                      "Cantidad_Asignada": st.column_config.NumberColumn("Cantidad Asignada", min_value=0.0, format="%.2f", required=True),
                      "Precio_Unitario_Asignado": st.column_config.NumberColumn("Precio Unitario Asignado", min_value=0.0, format="%.2f", required=True),
                      "Costo_Asignado": st.column_config.NumberColumn("Costo Asignado", disabled=True, format="%.2f") # Calculado
                  }
             )
             # Lógica de guardado para el editor
             if not df_asignaciones_edited.equals(st.session_state.df_asignacion_materiales[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada', 'Precio_Unitario_Asignado', 'Costo_Asignado']]):
                  st.session_state.df_asignacion_materiales = df_asignaciones_edited.copy()
                  st.session_state.df_asignacion_materiales = calcular_costo_asignado(st.session_state.df_asignacion_materiales)
                  if st.button("Guardar Cambios en Historial de Asignaciones"):
                      # Validar antes de guardar (ej. campos requeridos, ID_Obra exista)
                      if st.session_state.df_asignacion_materiales['ID_Obra'].isnull().any() or st.session_state.df_asignacion_materiales['Material'].isnull().any() or st.session_state.df_asignacion_materiales['Cantidad_Asignada'].isnull().any() or st.session_state.df_asignacion_materiales['Precio_Unitario_Asignado'].isnull().any():
                           st.error("Error: Hay campos obligatorios vacíos en el historial de asignaciones.")
                      elif not st.session_state.df_asignacion_materiales['ID_Obra'].isin(st.session_state.df_proyectos['ID_Obra']).all():
                           st.error("Error: Una o más asignaciones tienen un 'ID Obra' que no existe en la lista de obras. Por favor, corrija.")
                      else:
                           save_data(st.session_state.df_asignacion_materiales, FILE_ASIGNACION_MATERIALES)
                           st.success("Cambios en historial de asignaciones guardados.")
                           st.experimental_rerun()
                  else:
                      st.info("Hay cambios sin guardar en el historial de asignaciones.")

# ... código anterior dentro de page_compras_asignacion() ...

        st.subheader("Historial de Asignaciones")
        if st.session_state.df_asignacion_materiales.empty:
            st.info("No hay materiales asignados aún.")
        else:
            # ... código para mostrar el historial de asignaciones (data_editor) ...

            st.subheader("Deshacer Asignación (por ID)")
            # Esta línea calcula la lista de IDs
            asignaciones_disponibles = st.session_state.df_asignacion_materiales['ID_Asignacion'].tolist()

            # >>>>> LA LÍNEA 1217 ESTÁ AQUÍ <<<<<
            # Esta línea 'if' debe estar alineada con la línea de arriba
            if not asignaciones_disponibles:
                # Este bloque está indentado bajo el 'if'
                st.info("No hay asignaciones para deshacer.")
            else:
                # Este bloque 'else' debe estar alineado con el 'if'
                # Y el código dentro de este 'else' debe estar indentado bajo él
                # Mostrar un selectbox con IDs y algo de info
                asig_options = st.session_state.df_asignacion_materiales[['ID_Asignacion', 'Fecha_Asignacion', 'ID_Obra', 'Material', 'Cantidad_Asignada']].to_dict('records')
                format_func = lambda x: next((f"{item['ID_Asignacion']} ({item['Fecha_Asignacion']} - {item['ID_Obra']} - {item['Material']} - {item['Cantidad_Asignada']:.2f})", x) for item in asig_options if item['ID_Asignacion'] == x)[0] if x in [item['ID_Asignacion'] for item in asig_options] else x

                id_asignacion_deshacer = st.selectbox(
                    "Seleccione ID de Asignación a deshacer:",
                    asignaciones_disponibles,
                    format_func=format_func
                )

                if st.button(f"Deshacer Asignación Seleccionada ({id_asignacion_deshacer})"):
                    # Este bloque está indentado bajo el 'if' del botón
                    st.session_state.df_asignacion_materiales = st.session_state.df_asignacion_materiales[
                        st.session_state.df_asignacion_materiales['ID_Asignacion'] != id_asignacion_deshacer
                    ]
                    save_data(st.session_state.df_asignacion_materiales, FILE_ASIGNACION_MATERIALES)
                    st.success(f"Asignación {id_asignacion_deshacer} deshecha.")
                    st.experimental_rerun()

# ... resto del código dentro de page_compras_asignacion() ...


def page_reporte_variacion_total_obras():
    st.title("Reporte de Variación Total Obras (Presupuesto vs Real)")
    st.write("Compara el costo total presupuestado vs el costo total real (asignado) para cada obra.")

    if st.session_state.df_presupuesto_materiales.empty and st.session_state.df_asignacion_materiales.empty:
        st.info("No hay datos de presupuesto ni de asignación para generar el reporte.")
        return

    # Calcular totales presupuestados por obra
    df_presupuesto = st.session_state.df_presupuesto_materiales.copy()
    # Asegurar numérico y calcular costo
    df_presupuesto['Cantidad_Presupuestada'] = pd.to_numeric(df_presupuesto['Cantidad_Presupuestada'], errors='coerce').fillna(0)
    df_presupuesto['Precio_Unitario_Presupuestado'] = pd.to_numeric(df_presupuesto['Precio_Unitario_Presupuestado'], errors='coerce').fillna(0)
    df_presupuesto = calcular_costo_presupuestado(df_presupuesto)
    presupuesto_total_obra = df_presupuesto.groupby('ID_Obra')['Costo_Presupuestado'].sum().reset_index(name='Costo_Presupuestado_Total')
    presupuesto_cantidad_obra = df_presupuesto.groupby('ID_Obra')['Cantidad_Presupuestada'].sum().reset_index(name='Cantidad_Presupuestada_Total')


    # Calcular totales asignados por obra
    df_asignacion = st.session_state.df_asignacion_materiales.copy()
    # Asegurar numérico y calcular costo
    df_asignacion['Cantidad_Asignada'] = pd.to_numeric(df_asignacion['Cantidad_Asignada'], errors='coerce').fillna(0)
    df_asignacion['Precio_Unitario_Asignado'] = pd.to_numeric(df_asignacion['Precio_Unitario_Asignado'], errors='coerce').fillna(0)
    df_asignacion = calcular_costo_asignado(df_asignacion)
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

    # Calcular variación
    reporte_variacion_obras['Variacion_Total_Costo'] = reporte_variacion_obras['Costo_Asignado_Total'] - reporte_variacion_obras['Costo_Presupuestado_Total']
    reporte_variacion_obras['Variacion_Total_Cantidad'] = reporte_variacion_obras['Cantidad_Asignada_Total'] - reporte_variacion_obras['Cantidad_Presupuestada_Total']


    st.subheader("Variación de Costo y Cantidad por Obra (Presupuesto vs Real)")
    if reporte_variacion_obras.empty:
        st.info("No hay datos válidos para generar el reporte de variación.")
    else:
        st.dataframe(reporte_variacion_obras[[
            'Nombre_Obra', 'ID_Obra',
            'Cantidad_Presupuestada_Total', 'Cantidad_Asignada_Total', 'Variacion_Total_Cantidad',
            'Costo_Presupuestado_Total', 'Costo_Asignado_Total', 'Variacion_Total_Costo'
        ]].round(2))

        # --- Gráfico de Cascada Total (Costo) ---
        total_presupuestado_general = reporte_variacion_obras['Costo_Presupuestado_Total'].sum()
        total_asignado_general = reporte_variacion_obras['Costo_Asignado_Total'].sum()

        if abs(total_asignado_general - total_presupuestado_general) > 0.01 or total_presupuestado_general > 0 or total_asignado_general > 0: # Solo mostrar si hay algo o si hay variación
            st.subheader("Gráfico de Cascada: Presupuesto Total vs Costo Real Total")

            # Preparar datos para la cascada de COSTO
            labels_costo = ['Total Presupuestado']
            values_costo = [total_presupuestado_general]
            measures_costo = ['absolute']
            texts_costo = [f"${total_presupuestado_general:,.2f}"]

            # Añadir variaciones por obra (solo costo, solo si hay variación en la obra)
            # Ordenar por variación para un gráfico más legible
            reporte_variacion_obras_sorted_costo = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Costo']) > 0.01].sort_values('Variacion_Total_Costo', ascending=False)

            for index, row in reporte_variacion_obras_sorted_costo.iterrows():
                 labels_costo.append(f"Var: {row['Nombre_Obra']}")
                 values_costo.append(row['Variacion_Total_Costo'])
                 measures_costo.append('relative')
                 texts_costo.append(f"${row['Variacion_Total_Costo']:,.2f}")

            # Añadir el total asignado
            labels_costo.append('Total Asignado')
            values_costo.append(total_asignado_general)
            measures_costo.append('total')
            texts_costo.append(f"${total_asignado_general:,.2f}")


            if len(labels_costo) > 1: # Solo dibujar si hay algo más que el punto de inicio
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
                 st.info("El costo total presupuestado es igual al costo total asignado y ambos son cero. No hay variación de costo para mostrar.")

        else:
             st.info("No hay costo presupuestado ni asignado total para mostrar el gráfico de variación de costo.")

        # --- Gráfico de Cascada Total (Cantidad) ---
        total_cantidad_presupuestada_general = reporte_variacion_obras['Cantidad_Presupuestada_Total'].sum()
        total_cantidad_asignada_general = reporte_variacion_obras['Cantidad_Asignada_Total'].sum()

        if abs(total_cantidad_asignada_general - total_cantidad_presupuestada_general) > 0.01 or total_cantidad_presupuestada_general > 0 or total_cantidad_asignada_general > 0: # Solo mostrar si hay algo o si hay variación
            st.subheader("Gráfico de Cascada: Cantidad Total Presupuestada vs Cantidad Real Total")

            # Preparar datos para la cascada de CANTIDAD
            labels_cantidad = ['Total Presupuestado (Cant.)']
            values_cantidad = [total_cantidad_presupuestada_general]
            measures_cantidad = ['absolute']
            texts_cantidad = [f"{total_cantidad_presupuestada_general:,.2f}"]

            # Añadir variaciones por obra (solo cantidad, solo si hay variación en la obra)
            reporte_variacion_obras_sorted_cantidad = reporte_variacion_obras[abs(reporte_variacion_obras['Variacion_Total_Cantidad']) > 0.01].sort_values('Variacion_Total_Cantidad', ascending=False)

            for index, row in reporte_variacion_obras_sorted_cantidad.iterrows():
                 labels_cantidad.append(f"Var Cant: {row['Nombre_Obra']}")
                 values_cantidad.append(row['Variacion_Total_Cantidad'])
                 measures_cantidad.append('relative')
                 texts_cantidad.append(f"{row['Variacion_Total_Cantidad']:,.2f}")

            # Añadir el total asignado
            labels_cantidad.append('Total Asignado (Cant.)')
            values_cantidad.append(total_cantidad_asignada_general)
            measures_cantidad.append('total')
            texts_cantidad.append(f"{total_cantidad_asignada_general:,.2f}")


            if len(labels_cantidad) > 1: # Solo dibujar si hay algo más que el punto de inicio
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
                 st.info("La cantidad total presupuestada es igual a la cantidad total asignada y ambas son cero. No hay variación de cantidad para mostrar.")
        else:
            st.info("No hay cantidad presupuestada ni asignada total para mostrar el gráfico de variación de cantidad.")



# --- Main App Logic (Auth and Page Routing) ---

# Intenta autenticar. El login form aparece en la 'main' area por defecto.
name, authentication_status, username = authenticator.login('Login', 'main')

# Verifica el estado de autenticación
if authentication_status:
    # --- INICIO DEL BLOQUE DE LA BARRA LATERAL ---
    # TODO lo que va en la barra lateral DEBE estar dentro de este 'with'
    with st.sidebar:
        # Botón de logout
        # 'main' aquí significa que el botón se alinea con el contenido principal de la sidebar,
        # no en la parte superior específica de auth (que también se llama 'sidebar' en algunas versiones).
        # 'Cerrar Sesión' es el texto del botón, 'main' es la ubicación DENTRO de la sidebar.
        authenticator.logout('Cerrar Sesión', 'sidebar')

        st.title(f"Bienvenido, {name}")

        # Definición de las páginas para la navegación
        # Esto DEBE estar dentro del bloque 'with st.sidebar:'
        st.header("Navegación")
        pages = {
            "Dashboard Principal": "dashboard", # Simple placeholder
            "Gestión de Equipos": "equipos",
            "Registro de Consumibles": "consumibles",
            "Registro de Costos Equipos": "costos_equipos",
            "Reportes Mina (Consumo/Costo)": "reportes_mina",
            "Variación Costos Flota": "variacion_costos_flota",
            "--- Gestión de Obras y Materiales ---": None, # Separador (No es una página real)
            "Gestión de Obras (Proyectos)": "gestion_obras",
            "Reporte Presupuesto Total Obras": "reporte_presupuesto_total_obras",
            "Gestión Compras y Asignación": "compras_asignacion",
            "Reporte Variación Total Obras": "reporte_variacion_total_obras",
        }

        # Selector de página (radio button en la sidebar)
        # Esto DEBE estar dentro del bloque 'with st.sidebar:'
        selected_page_key = st.radio("Ir a:", list(pages.keys()), index=0)
        selected_page = pages[selected_page_key]

    # --- FIN DEL BLOQUE DE LA BARRA LATERAL ---


    # --- CONTENIDO PRINCIPAL DE LA PÁGINA SELECCIONADA ---
    # Este código se ejecuta FUERA del bloque 'with st.sidebar:'
    # Llama a la función de la página seleccionada
    if selected_page == "dashboard":
        st.title("Dashboard Principal")
        st.write(f"Bienvenido {name} al sistema de gestión para la empresa proveedora de minería.")
        st.info("Seleccione una opción del menú lateral para comenzar.")
        st.markdown("---")
        st.subheader("Resumen Rápido")
        # Aquí podrías agregar métricas clave rápidas
        total_equipos = len(st.session_state.df_equipos)
        total_obras = len(st.session_state.df_proyectos)
        # Asegurar que los cálculos de totales no fallen en DF vacíos
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
        # Si selected_page es None (para separadores como "---"), no renderizar nada en el área principal
        st.empty() # Asegurar que el área principal esté vacía
        # Opcional: mostrar un mensaje genérico si se selecciona un separador
        # st.info("Seleccione una página válida del menú lateral.")


# Esto maneja los estados cuando el login NO es exitoso
elif authentication_status == False:
    st.error('Usuario/Contraseña incorrecto.')
elif authentication_status == None:
    st.warning('Por favor, ingrese su usuario y contraseña.')

# Final del script
