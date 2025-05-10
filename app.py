import requests
import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st

base_address = "http://intra-erp:4444/EPLAN_WS_FREE_EDP"
# Aktuelles Datum und in 10 Arbeitstagen & 3 Tage zurück
heute = datetime.today()
arbeitstage_10spaeter = np.busday_offset(heute.date(), 10, roll='forward')
arbeitstage_3frueher = np.busday_offset(heute.date(), -3, roll='backward')

# Als Strings im Format DD.MM.YYYY
datum_heute = heute.strftime("%d.%m.%Y")
datum_plus10 = pd.Timestamp(arbeitstage_10spaeter).strftime("%d.%m.%Y")
datum_minus3 = pd.Timestamp(arbeitstage_3frueher).strftime("%d.%m.%Y")

# Zuordnung der Kalkulationsstunden zu den Abteilungen
CALC_FIELD_TO_DEPT = {
    "yprjmcad":  "MCAD",
    "yprjecad":  "ECAD",
    "yprjauto":  "AUTOMATION",
    "yprjbild":  "BILDGEBUNG",
    "yprjas":    "PROJECTMANAGEMENT",
    "yprjpm":    "PRODUCT DEVELOPMENT",
    "yprjtd":    "TD",
    "yprjsoft":  "SOFTWARE",
}

#create a task for a specific person in ABAS ERP
def create_project_task_for_person(project_number, person_short, task_name, time_budget, date_start, date_end):
    
    
    # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "create",
        "database_and_group": "149:02",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "ypersonal", "value": person_short},
            {"name": "namebspr", "value": task_name},
            {"name": "ypvsollstd", "value": time_budget},
            {"name": "ypvplanstd", "value": time_budget},
            {"name": "ypvforecaststd", "value": time_budget},
            {"name": "yadatum", "value": date_start},
            {"name": "yedatum", "value": date_end }
        ],
    }
    try:
        response = requests.post(base_address, json=params)
        response.raise_for_status()  # Löst eine Exception bei HTTP-Fehlern aus
        return response.json()       # Erwartet eine JSON-Antwort vom Server
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen des Gateway Numbers: {e}")
        return None

def create_project_task_for_department(project_number, department_short, task_name, time_budget, date_start, date_end):
    
    
    # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "create",
        "database_and_group": "149:02",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "yprojteam", "value": department_short},
            {"name": "yleiart", "value": department_short},
            {"name": "namebspr", "value": task_name},
            {"name": "ypvsollstd", "value": time_budget},
            {"name": "ypvplanstd", "value": time_budget},
            {"name": "ypvforecaststd", "value": time_budget},
            {"name": "yadatum", "value": date_start},
            {"name": "yedatum", "value": date_end }
        ],
    }
    try:
        response = requests.post(base_address, json=params)
        response.raise_for_status()  # Löst eine Exception bei HTTP-Fehlern aus
        return response.json()       # Erwartet eine JSON-Antwort vom Server
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen des Gateway Numbers: {e}")
        return None

def create_dispatch_milestone(project_number, person_short, date_start, date_end):
    
    # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "create",
        "database_and_group": "149:02",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "ypersonal", "value": person_short},
            {"name": "yadatum", "value": date_start},
            {"name": "yedatum", "value": date_end },
            {"name": "ypvtyp", "value": "Meilenstein" },
            {"name": "namebspr", "value": "Dispatch"}
        ],
    }
    try:
        response = requests.post(base_address, json=params)
        response.raise_for_status()  # Löst eine Exception bei HTTP-Fehlern aus
        return response.json()       # Erwartet eine JSON-Antwort vom Server
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen des Gateway Numbers: {e}")
        return None

def fetch_gateway_data(gateway_id):
        # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "read",
        "id": gateway_id,
        "fields": [],
        "table_fields": ["ytzid","ytname","ytenddate"]
    }
    try:
        response = requests.post(base_address, json=params)
        response.raise_for_status()  # Löst eine Exception bei HTTP-Fehlern aus
        return response.json()       # Erwartet eine JSON-Antwort vom Server
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen des Gateway Numbers: {e}")
        return None

# gGet the gateway number and ID from the project number
def get_gatewaynumber_and_id(project_number):
    params = {
        "action": "query",
        "database_and_group": "32:00",
        "fields": [
            "nummer", "id","ycalc^nummer",
        ],
        "filter":{
            "type": "atomic_condition",
            "name": "yproject",
            "value": project_number,
            "operator": "EQUALS"
        }
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der Daten: {e}")
        return None

#Extract the calculated hours for this project
def fetch_calculation_hours(calculation_number):
    params = {
        "action": "query",
        "database_and_group": "41:00",
        "fields": [
            "yprjmcad","yprjauto","yprjecad","yprjbild","yprjas","yprjpm","yprjtd","yprjsoft"
        ],
        "filter":{
            "type": "atomic_condition",
            "name": "nummer",
            "value": calculation_number,
            "operator": "EQUALS"
        }
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Abrufen der Daten: {e}")
        return None


def fetch_calculation_hours(calculation_number):
    params = {
        "action": "query",
        "database_and_group": "41:00",
        "fields": [
            "yprjmcad","yprjauto","yprjecad","yprjbild","yprjas","yprjpm","yprjtd","yprjsoft"
        ],
        "filter":{
            "type": "atomic_condition",
            "name": "nummer",
            "value": calculation_number,
            "operator": "EQUALS"
        }
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Daten: {e}")
        return None


def fetch_gateway_infosystem_sold_phase():
    params = {
        "action": "infosystem",
        "infosystem": "GATEWAYDASHBOARD",
        "data": [
            {"name": "prjphase", "value": "Sold Phase"},
            {"name": "bstart", "value": "1"}   
        ],
        "table_fields": [
            "tprojekt^nummer","tserprod^nummer","tprojektname^name",
            "tprjphase^name","ytaktgw","ygwinfo","ytprjampel",
            "ytkundeans^name","ytstandortans^name","ytprjverantw^such"
        ]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Daten: {e}")
        return None

def fetch_gateway_infosystem(projektleiter_kuerzel):
    params = {
        "action": "infosystem",
        "infosystem": "GATEWAYDASHBOARD",
        "data": [
            {"name": "prjleit", "value": projektleiter_kuerzel},
            {"name": "bstart", "value": "1"}
        ],
        "table_fields": [
            "tprojekt^nummer","tserprod^nummer","tprojektname^name",
            "tprjphase^name","ytaktgw","ygwinfo","ytprjampel",
            "ytkundeans^name","ytstandortans^name","ytprjverantw^such"
        ]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Daten: {e}")
        return None

def fetch_dispatch_infosystem(projektleiter_kuerzel):
    params = {
        "action": "infosystem",
        "infosystem": "DISPATCH",
        "data": [
            {"name": "yprjleit", "value": projektleiter_kuerzel},
            {"name": "yvon", "value": datum_heute},
            {"name": "ybis", "value": datum_plus10},
            {"name": "bstart", "value": "1"}
        ],
        "table_fields": [
            "ytprojekt^nummer","ytserprod^nummer",
            "ytserprodname^name","ytwarenempfname^name","ytdispatch"
        ]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Dispatch-Daten: {e}")
        return None
    
def fetch_open_tasks(projektleiter_kuerzel):
    params = {
        "action": "infosystem",
        "infosystem": "10345",
        "data": [
            {"name": "bearbeit", "value": projektleiter_kuerzel},
            {"name": "bstart", "value": "1"}
        ],
        "table_fields": ["taufgabe^nummer","taufgabe^projekt^nummer","taufgabe^yprojektname^namebspr","taufgabe^start","taufgabe^end","taufgabenname^namebspr","tbestaetigername^namebspr"]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Aufgaben: {e}")
        return None

def fetch_booked_hours(projektleiter_kuerzel):
    params = {
        "action": "infosystem",
        "infosystem": "PRJMLM",
        "data": [
            {"name": "ypersonal", "value": projektleiter_kuerzel},
            {"name": "ystdvondatum", "value": datum_minus3},
            {"name": "ystdbisdatum", "value": datum_heute},
            {"name": "bstart", "value": "1"}
        ],
        "table_fields": ["yadatum","ystdtats","ytprojekt^nummer","ytprojekt^namebspr"]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der gebuchten Stunden: {e}")
        return None
    
def fetch_overbooked_projects(projektleiter_kuerzel):
    params = {
        "action": "infosystem",
        "infosystem": "PRJM5080LISTE",
        "data": [
            {"name": "yprojleit", "value": projektleiter_kuerzel},
            {"name": "ybprabgeschlossen", "value": "1"},
            {"name": "yvondatum", "value": datum_minus3},
            {"name": "ybisdatum", "value": datum_heute},
            {"name": "bstart", "value": "1"}
        ],
        "table_fields": ["ytprojekt^nummer","ytprojname^namebspr","ytfortistbudget","ytsollstd","ytiststd"]
    }
    try:
        res = requests.post(base_address, json=params, timeout=30)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der überbuchten Projekte: {e}")
        return None


def main():
    st.title("PJM OVERVIEW")
    projektleiter = st.text_input("Projektleiter-Kürzel eingeben:", value="MRG")
    if not projektleiter:
        st.warning("Bitte ein Projektleiter-Kürzel eingeben.")
        return

    # Gateways in SOLD Phase
    gw_sold = fetch_gateway_infosystem_sold_phase()
    if gw_sold and gw_sold.get("success"):
        df_gw_sold = pd.DataFrame(gw_sold["result_data"]["table"])
        df_gw_sold.rename(columns={
            "tprojekt^nummer": "Projekt-Nr.",
            "tserprod^nummer": "Serviceprodukt",
            "tprojektname^name": "Projektname",
            "tprjphase^name": "Phase",
            "ytaktgw": "Gateway",
            "ygwinfo": "Gateway-Info",
            "ytprjampel": "Ampel",
            "ytkundeans^name": "Kunde",
            "ytstandortans^name": "Standort",
            "ytprjverantw^such": "Verantwortlich"
        }, inplace=True)
        order_gw_sold = [
            "Projekt-Nr.","Serviceprodukt","Verantwortlich","Projektname",
            "Phase","Ampel","Gateway","Gateway-Info",
            "Kunde","Standort"
        ]
        df_gw_sold = df_gw_sold[[c for c in order_gw_sold if c in df_gw_sold.columns]]
        st.subheader("💸Projekte in Sold Phase")
        st.dataframe(df_gw_sold, use_container_width=True)

    else:
        st.error("API-Fehler oder keine Verbindung beim Gateway-Datenabruf.")

    # Gateway-Daten anzeigen
    gw = fetch_gateway_infosystem(projektleiter)
    if gw and gw.get("success"):
        df_gw = pd.DataFrame(gw["result_data"]["table"])
        df_gw.rename(columns={
            "tprojekt^nummer": "Projekt-Nr.",
            "tserprod^nummer": "Serviceprodukt",
            "tprojektname^name": "Projektname",
            "tprjphase^name": "Phase",
            "ytaktgw": "Gateway",
            "ygwinfo": "Gateway-Info",
            "ytprjampel": "Ampel",
            "ytkundeans^name": "Kunde",
            "ytstandortans^name": "Standort",
            "ytprjverantw^such": "Verantwortlich"
        }, inplace=True)
        order_gw = [
            "Projekt-Nr.","Serviceprodukt","Projektname",
            "Phase","Ampel","Gateway","Gateway-Info",
            "Kunde","Standort","Verantwortlich"
        ]
        df_gw = df_gw[[c for c in order_gw if c in df_gw.columns]]
        st.subheader("🔴 Projekte mit roter Ampel")
        st.dataframe(df_gw[df_gw["Ampel"]=="icon:ball_red"], use_container_width=True)
        st.subheader("🔵 Projekte mit blauer Ampel")
        st.dataframe(df_gw[df_gw["Ampel"]=="icon:ball_blue"], use_container_width=True)
    else:
        st.error("API-Fehler oder keine Verbindung beim Gateway-Datenabruf.")

    # Dispatch-Daten anzeigen
    disp = fetch_dispatch_infosystem(projektleiter)
    if disp and disp.get("success"):
        df_disp = pd.DataFrame(disp["result_data"]["table"])
        df_disp.rename(columns={
            "ytprojekt^nummer": "Projekt-Nr.",
            "ytserprod^nummer": "Serviceprodukt",
            "ytserprodname^name": "Systemtyp",
            "ytwarenempfname^name": "Empfänger",
            "ytdispatch": "Dispatch"
        }, inplace=True)
        order_disp = ["Dispatch","Projekt-Nr.","Serviceprodukt","Systemtyp","Empfänger"]
        df_disp = df_disp[[c for c in order_disp if c in df_disp.columns]]
        st.subheader("📦 Dispatch-Übersicht nächste 10 Tage")
        st.dataframe(df_disp, use_container_width=True)
    else:
        st.info("Dispatch-Daten konnten nicht geladen werden.")

    #Aufgaben anzeigen
    tasks = fetch_open_tasks(projektleiter)
    if tasks and tasks.get("success"):
        df_t = pd.DataFrame(tasks["result_data"]["table"])
        df_t.rename(columns={
            "taufgabe^nummer" : "Nummer",
            "taufgabenname^namebspr": "Titel",
            "tbestaetigername^namebspr": "von",
            "taufgabe^projekt^nummer": "Projektnummer",
            "taufgabe^yprojektname^namebspr": "Projektname",
            "taufgabe^start": "Startdatum",
            "taufgabe^end": "Enddatum",
            "taufgabenname^namebspr": "Aufgabenbeschreibung"
        }, inplace=True)
        order_t = ["Nummer","Titel","von","Projektnummer","Projektname","Startdatum","Enddatum","Aufgabenbeschreibung"]
        df_t = df_t[[c for c in order_t if c in df_t.columns]]
        st.subheader("☑️ Aktive Abas-Aufgaben")
        st.dataframe(df_t, use_container_width=True)
    else:
        st.info("Aufgaben konnten nicht geladen werden.")

    #Überbuchte Projekte Anzeigen
    overbooked_projects = fetch_overbooked_projects(projektleiter)
    if overbooked_projects and overbooked_projects.get("success"):
        df_overbooked_projects = pd.DataFrame(overbooked_projects["result_data"]["table"])
        df_overbooked_projects.rename(columns={
            "ytprojekt^nummer": "Projektnummer",
            "ytprojname^namebspr": "Projektname",
            "ytfortistbudget": "Buchung in Prozent des Budgets (%)",
            "ytsollstd": "Budget",
            "ytiststd": "Gebucht"
        }, inplace=True)
        order_overbooked_projects= ["Projektnummer","Buchung in Prozent des Budgets (%)","Projektname","Budget","Gebucht"]
        df_overbooked_projects = df_overbooked_projects[[c for c in order_overbooked_projects if c in df_overbooked_projects.columns]]
        st.subheader("☠️ Überbuchte Projekte")
        # make sure the column is numeric once, up-front
        df_overbooked_projects["Buchung in Prozent des Budgets (%)"] = (
            pd.to_numeric(df_overbooked_projects["Buchung in Prozent des Budgets (%)"],
                        errors="coerce")
        )

        over_100 = df_overbooked_projects[
            df_overbooked_projects["Buchung in Prozent des Budgets (%)"] > 100
        ]

        st.dataframe(over_100, use_container_width=True)
    else:
        st.info("Überbuchte Projekte konnten nicht geladen werden.")


    #Gebuchte Stunden anzeigen
    booked = fetch_booked_hours(projektleiter)
    if booked and booked.get("success"):
        df_b = pd.DataFrame(booked["result_data"]["table"])
        df_b.rename(columns={
            "yadatum": "Datum",
            "ystdtats": "Stundenzahl",
            "ytprojekt^nummer": "Projekt-Nr.",
            "ytprojekt^namebspr": "Leistungsmeldung"
        }, inplace=True)
        order_b = ["Datum","Stundenzahl","Projekt-Nr.","Leistungsmeldung"]
        df_b = df_b[[c for c in order_b if c in df_b.columns]]
        st.subheader("⏱️ Gebuchte Stunden (letzte 3 Tage)")
        st.dataframe(df_b, use_container_width=True)
    else:
        st.info("Stundendaten konnten nicht geladen werden.")



if __name__ == "__main__":
    main()
