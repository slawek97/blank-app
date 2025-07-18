import streamlit as st
import pandas as pd
import os
import sqlite3
import requests
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from datetime import datetime

# --- Stałe
FILE_ID = "1fGIqDloAMGWQjfSGVwqJX3nRLq3UUHAt"
DOWNLOAD_URL = f"https://drive.google.com/uc?id={FILE_ID}"
LOCAL_DB_PATH = "/tmp/baza.sqlite"
TABLE_NAME = "gen_jw_data"

# --- Pobierz bazę jeśli nie istnieje
@st.cache_data(show_spinner="Pobieranie bazy danych...")
def download_db():
    if not os.path.exists(LOCAL_DB_PATH):
        with requests.get(DOWNLOAD_URL, stream=True) as r:
            r.raise_for_status()
            with open(LOCAL_DB_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    return LOCAL_DB_PATH

# --- Pobierz unikalne jednostki
@st.cache_data
def get_power_plants():
    con = sqlite3.connect(LOCAL_DB_PATH)
    df = pd.read_sql(f"SELECT DISTINCT power_plant FROM {TABLE_NAME} ORDER BY power_plant", con)
    con.close()
    return df["power_plant"].dropna().tolist()

# --- Pobierz dane z bazy
def fetch_data(start_date, end_date, plant_name):
    con = sqlite3.connect(LOCAL_DB_PATH)
    query = f"""
    SELECT *
    FROM {TABLE_NAME}
    WHERE business_date BETWEEN ? AND ?
    AND power_plant LIKE ?
    ORDER BY dtime_utc
    LIMIT 100000
    """
    start_str = start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else str(start_date)
    end_str = end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else str(end_date)
    param = f"%{plant_name}%" if plant_name else "%"
    df = pd.read_sql_query(query, con, params=(start_str, end_str, param))
    con.close()
    return df

# --- Interfejs główny
def main():
    st.set_page_config(page_title="PSE generacja jednostek konwencjonalnych", layout="wide")
    st.title("⚡ PSE generacja jednostek konwencjonalnych")

    # Sidebar - menu wyboru widoku
    page = st.sidebar.radio("Wybierz widok", ["Widok tabeli", "Wykresy"])

    # Tylko w trybie tabeli
    if page == "Widok tabeli":
        download_db()
        plant_options = ["(Wszystkie)"] + get_power_plants()

        # Filtry
        col1, col2, col3 = st.columns(3)
        with col1:
            start_date = st.date_input("Data początkowa", datetime(2025, 1, 1))
        with col2:
            end_date = st.date_input("Data końcowa", datetime(2025, 12, 31))
        with col3:
            selected_plant = st.selectbox("Jednostka (power_plant)", plant_options)

        # Pobierz dane przy kliknięciu
        if st.button("🔄 Pobierz dane"):
            plant_filter = "" if selected_plant == "(Wszystkie)" else selected_plant

            with st.spinner("Ładowanie danych z bazy..."):
                df = fetch_data(start_date, end_date, plant_filter)
                st.session_state["df"] = df
        else:
            if "df" not in st.session_state:
                st.info("Ustaw filtry i kliknij **Pobierz dane**, aby rozpocząć.")

        # Tabela i eksport (jeśli są dane)
        if "df" in st.session_state and not st.session_state["df"].empty:
            df = st.session_state["df"]

            # Eksport CSV nad tabelą (poszerzony przycisk po prawej)
            col_space, col_dl = st.columns([7, 1])
            with col_dl:
                st.download_button(
                    label="📥 CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name="dane_generacja.csv",
                    mime="text/csv",
                    key="download_csv",
                    use_container_width=True
                )

            # Konfiguracja tabeli
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_pagination(enabled=True)
            gb.configure_default_column(
                enableRowGroup=True,
                enableValue=True,
                enablePivot=True,
                filter="agTextColumnFilter",
                sortable=True,
                editable=False,
            )
            grid_options = gb.build()

            AgGrid(
                df,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.NO_UPDATE,
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=False,
                height=600,
            )
        elif "df" in st.session_state and st.session_state["df"].empty:
            st.warning("Brak danych dla wybranych filtrów.")

    elif page == "Wykresy gen":
        st.subheader("📈 Wykresy (do zaimplementowania)")
        st.info("Tutaj pojawią się wykresy po implementacji.")

    # Stopka wyrównana do prawej
    st.markdown("---")
    st.markdown(
        '<div style="text-align:right; font-size:12px; color:gray;">v.1.1 &nbsp;&nbsp;|&nbsp;&nbsp; SB &nbsp;&nbsp;|&nbsp;&nbsp; bekasiewiczslawomir@gmail.com</div>',
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
