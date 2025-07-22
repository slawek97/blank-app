import streamlit as st
import pandas as pd
import os
import sqlite3
import requests
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from datetime import datetime
import plotly.graph_objs as go
import folium
from streamlit_folium import st_folium
from io import BytesIO

# --- Stałe 
DROPBOX_URL = "https://www.dropbox.com/scl/fi/illcf1t8fjazd7ic0vvu6/mybase.db?rlkey=bfvio8dkozl9vnx1f7se1eemq&st=14hf4fgb&dl=1"
LOCAL_DB_PATH = "/tmp/mybase.db"
TABLE_NAME = "gen_jw_data"

# --- Pobierz bazę jeśli nie istnieje
@st.cache_data(show_spinner="Pobieranie bazy danych...")
def download_db():
    if not os.path.exists(LOCAL_DB_PATH):
        with requests.get(DROPBOX_URL, stream=True) as r:
            r.raise_for_status()
            with open(LOCAL_DB_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    return LOCAL_DB_PATH

# --- Reszta kodu jak wcześniej, np.:

@st.cache_data
def get_power_plants():
    con = sqlite3.connect(LOCAL_DB_PATH)
    df = pd.read_sql(f"SELECT DISTINCT power_plant FROM {TABLE_NAME} ORDER BY power_plant", con)
    con.close()
    return df["power_plant"].dropna().tolist()

def fetch_data_multi(start_date, end_date, plant_names):
    if not plant_names:
        return pd.DataFrame()

    placeholders = ",".join("?" for _ in plant_names)
    query = f"""
    SELECT *
    FROM {TABLE_NAME}
    WHERE business_date BETWEEN ? AND ?
    AND power_plant IN ({placeholders})
    ORDER BY dtime_utc
    LIMIT 100000
    """
    params = [start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")] + plant_names

    con = sqlite3.connect(LOCAL_DB_PATH)
    df = pd.read_sql_query(query, con, params=params)
    con.close()
    return df

# --- Wczytanie danych lokalizacji elektrowni z Excela (Dropbox)
@st.cache_data
def load_power_plant_locations_pl():
    dropbox_url_p = "https://www.dropbox.com/scl/fi/n67poigdaxa1bvd7qga8l/power_plants_info.xlsx?rlkey=51c64jfi10fjuz31vz3dtwdu8&st=9im89fa3&dl=1"
    try:
        response = requests.get(dropbox_url_p)
        response.raise_for_status()
    except Exception as e:
        st.error(f"❌ Nie udało się pobrać danych lokalizacyjnych z Dropboxa: {e}")
        return pd.DataFrame()

    try:
        df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
    except Exception as e:
        st.error(f"❌ Błąd podczas odczytu pliku Excel: {e}")
        return pd.DataFrame()

    # Konwersje nazw kolumn na łatwiejsze
    df = df.rename(columns={
        "Nazwa obiektu []": "plant_name",
        "Paliwo 1 PL []": "fuel_type",
        "Moc zainstalowana elektryczna brutto [MW_e]": "capacity_mw",
        "Szerokość geograficzna []": "lat",
        "Długość geograficzna []": "lon"
    })

    # Konwersja mocy i współrzędnych na liczby
    df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    # Usuwamy wiersze bez koordynatów
    df = df.dropna(subset=["lat", "lon", "plant_name"])

    # Grupujemy dane po plant_name — sumujemy moce i bierzemy średnie koordynaty
    # Wybierzemy fuel_type jako najczęstszy dla danej elektrowni (mode)
    agg_df = df.groupby("plant_name").agg({
        "capacity_mw": "sum",
        "lat": "mean",
        "lon": "mean",
        "fuel_type": lambda x: x.mode().iloc[0] if not x.mode().empty else "Nieznany"
    }).reset_index()

    return agg_df[["plant_name", "fuel_type", "capacity_mw", "lat", "lon"]]

def add_legend_to_map(m):
    legend_html = """
     <div style="
         position: fixed;
         bottom: 30px;
         left: 30px;
         width: 200px;
         background-color: white;
         border: 1px solid grey;
         z-index: 9999;
         font-size: 12px;
         padding: 8px;
         box-shadow: 2px 2px 5px rgba(0,0,0,0.2);
     ">
         <b>Legenda - rodzaj paliwa</b><br>
         <i style="background:orange;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Gaz ziemny<br>
         <i style="background:brown;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Węgiel brunatny<br>
         <i style="background:black;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Węgiel kamienny<br>
         <i style="background:green;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Biomasa<br>
         <i style="background:gray;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Gaz koksowniczy<br>
         <i style="background:darkblue;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Olej ciężki<br>
         <i style="background:purple;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Odpady komunalne<br>
         <i style="background:lightgray;width:12px;height:12px;display:inline-block;margin-right:5px;"></i> Gaz wielkopiecowy<br>
         <hr style="margin:6px 0;">
         <b>Rozmiar znacznika:</b><br>
         <span style="font-size:11px;">● &lt;100 MW</span><br>
         <span style="font-size:13px;">● 100–500 MW</span><br>
         <span style="font-size:15px;">● 500–1000 MW</span><br>
         <span style="font-size:17px;">● &gt;1000 MW</span>
     </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

# --- Wyświetlenie mapy
fuel_colors = {
    "Gaz ziemny": "orange",
    "Węgiel brunatny": "brown",
    "Węgiel kamienny": "black",
    "Biomasa": "green",
    "Gaz koksowniczy": "gray",
    "Olej ciężki": "darkblue",
    "Odpady komunalne": "purple",
    "Gaz wielkopiecowy": "lightgray"
}

default_color = "gray"

def get_marker_radius(capacity):
    if pd.isna(capacity):
        return 3
    if capacity > 1000:
        return 10
    elif capacity > 500:
        return 7
    elif capacity > 100:
        return 5
    else:
        return 3

def mapa_view():
    st.title("Mapa jednostek wytwórczych")

    df = load_power_plant_locations_pl()
    if df.empty:
        st.warning("Brak danych lokalizacyjnych do wyświetlenia.")
        return

    m = folium.Map(location=[52.2, 19.2], zoom_start=6, tiles="CartoDB positron")

    for _, row in df.iterrows():
        capacity = row["capacity_mw"]
        color = fuel_colors.get(row["fuel_type"], default_color)
        radius = get_marker_radius(capacity)

        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=radius,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup(
                f"<b>{row['plant_name']}</b><br>"
                f"Typ: {row['fuel_type']}<br>"
                f"Moc: {capacity:.1f} MW" if not pd.isna(capacity) else "Brak danych",
                max_width=250
            )
        ).add_to(m)
    add_legend_to_map(m)  # <--- dodano tutaj
    st_folium(m, width=900, height=600)

# --- Interfejs główny
def main():
    st.set_page_config(page_title="PSE generacja jednostek konwencjonalnych", layout="wide")
    st.title("⚡ PSE generacja jednostek konwencjonalnych")

    # Sidebar - menu wyboru widoku
    page = st.sidebar.radio("Wybierz widok", ["Widok tabeli", "Wykresy", "Mapa"])

    if page == "Widok tabeli":
        download_db()
        plant_options = ["(Wszystkie)"] + get_power_plants()

        col1, col2, col3 = st.columns(3)
        with col1:
            start_date = st.date_input("Data początkowa", datetime(2025, 1, 1))
        with col2:
            end_date = st.date_input("Data końcowa", datetime(2025, 12, 31))
        with col3:
            selected_plant = st.selectbox("Jednostka (power_plant)", plant_options)

        if st.button("🔄 Pobierz dane"):
            plant_filter = "" if selected_plant == "(Wszystkie)" else selected_plant
            with st.spinner("Ładowanie danych z bazy..."):
                df = fetch_data_multi(start_date, end_date, [plant_filter] if plant_filter else get_power_plants())
                st.session_state["df"] = df
        else:
            if "df" not in st.session_state:
                st.info("Ustaw filtry i kliknij **Pobierz dane**, aby rozpocząć.")

        if "df" in st.session_state and not st.session_state["df"].empty:
            df = st.session_state["df"]

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

    # --- Wykresy
    elif page == "Wykresy":
        st.subheader("Wykresy generacji bloków w jednostkach")

        download_db()
        plant_options = get_power_plants()

        col1, col2 = st.columns(2)
        with col1:
            selected_plants = st.multiselect("Wybierz jednostki (power_plant)", plant_options)
        with col2:
            date_range = st.date_input("Zakres dat", [datetime(2025, 1, 1), datetime(2025, 1, 7)])

        if len(date_range) != 2:
            st.warning("Wybierz poprawny zakres dat.")
            return

        start_date, end_date = date_range

        if st.button("Generuj wykres"):
            if not selected_plants:
                st.warning("Wybierz przynajmniej jedną jednostkę.")
                return

            with st.spinner("Ładowanie danych..."):
                df = fetch_data_multi(start_date, end_date, selected_plants)

            if df.empty:
                st.warning("Brak danych dla wybranych filtrów.")
            else:
                df["dtime_utc"] = pd.to_datetime(df["dtime_utc"])
                df["blok"] = df["power_plant"] + " / " + df["resource_code"]

                fig = go.Figure()

                for (plant, resource), group in df.groupby(["power_plant", "resource_code"]):
                    group = group.sort_values("dtime_utc")
                    fig.add_trace(go.Scatter(
                        x=group["dtime_utc"],
                        y=group["wartosc"],
                        mode="lines+markers",
                        name=f"{plant} / {resource}",
                        hovertemplate=f"{plant} / {resource}<br>%{{x}}<br>%{{y:.2f}} MW<extra></extra>"
                    ))

                fig.update_layout(
                    title="Generacja bloków (MW)",
                    xaxis_title="Czas",
                    yaxis_title="Moc [MW]",
                    hovermode="x unified",
                    template="plotly_white",
                    legend_title="Bloki"
                )

                fig.update_xaxes(rangeslider_visible=True)
                fig.update_layout(dragmode="zoom")

                st.plotly_chart(fig, use_container_width=True)
    elif page == "Mapa":
        mapa_view()


    # --- Stopka
    st.markdown("---")
    st.markdown(
        '<div style="text-align:right; font-size:12px; color:gray;">v.1.2 &nbsp;&nbsp;|&nbsp;&nbsp; SB &nbsp;&nbsp;|&nbsp;&nbsp; bekasiewiczslawomir@gmail.com</div>',
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
