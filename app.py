import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import streamlit as st
from typing import Dict, Tuple

base_address = "http://intra-erp:4444/EPLAN_WS_FREE_EDP"
# Aktuelles Datum und in 10 Arbeitstagen & 3 Tage zurück
heute = datetime.today()
arbeitstage_10spaeter = np.busday_offset(heute.date(), 10, roll='forward')
arbeitstage_3frueher = np.busday_offset(heute.date(), -3, roll='backward')

# Als Strings im Format DD.MM.YYYY
datum_heute = heute.strftime("%d.%m.%Y")
datum_plus10 = pd.Timestamp(arbeitstage_10spaeter).strftime("%d.%m.%Y")
datum_minus3 = pd.Timestamp(arbeitstage_3frueher).strftime("%d.%m.%Y")



from typing import Optional, Dict, Any, Tuple, List  # mypy‑friendly

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
ALL_DEPTS = list(CALC_FIELD_TO_DEPT.values())


#Aufgabennamen
TASK_NAMES = {
    "BILDGEBUNG": "Imaging Design",
    "MCAD": "Konstruktionsphase (inklusive Kundenlayout)",
    "ECAD": "ECAD KOnstruktionsphase",
    "PROJECTMANAGEMENT": "Project Planning",
    "PRODUCT DEVELOPMENT": "Special Development",  
    "SOFTWARE": "Software Installation",
    "TD": "Manual",
    "AUTOMATION": "Automation",
}
# ---------------------------------------------------------------------------
# TERMINREGELN – leicht anpassbar
# ---------------------------------------------------------------------------
# Jeder Eintrag: (Start‑Anker, Start‑Offset‑Tage, End‑Anker, End‑Offset‑Tage)
# Anker = "G6", "G7", "G8"  oder "TODAY"
DATE_RULES = {
    "BILDGEBUNG":          ("G6", 0,    "G6",  7),
    "MCAD":                ("G7", -14,  "G7", -7),
    "ECAD":                ("G7", -14,  "G7", -7),
    "PROJECTMANAGEMENT":   ("TODAY", 0, "G8",  0),
    "PRODUCT DEVELOPMENT": ("G6", 0,    "G7",  0),  # Ende Engineering = G7
    "SOFTWARE":            ("G7", 0,    "G7", 14),
    "TD":                  ("G8", -10,  "G8", -3),
    "AUTOMATION":          ("G7", 0,  "G7", 14)
}

# ---------------------------------------------------------------------------
# HILFSFUNKTION – Default‑Start/Ende nach Regelwerk berechnen
# ---------------------------------------------------------------------------

def _default_dates(dept: str, gw_dates: Dict[str, str]):
    rule = TASK_DATE_RULES.get(dept)
    if not rule:
        return heute.date(), heute.date()

    start_anchor, start_off, end_anchor, end_off = rule

    def _anchor_date(key):
        if key == "TODAY":
            return heute.date()
        d_str = gw_dates.get(key)
        return pd.to_datetime(d_str, dayfirst=True).date() if d_str else heute.date()

    s_date = _anchor_date(start_anchor) + timedelta(days=start_off)
    e_date = _anchor_date(end_anchor) + timedelta(days=end_off)
    return s_date, e_date

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
def get_gateway_id_and_calculation_number(project_number: str) -> Optional[Dict[str, Any]]:
    """Query GW‑header in ABAS → liefert Nummer, ID & Kalkulations‑Nr."""
    params = {
        "action": "query",
        "database_and_group": "32:00",
        "fields": ["nummer", "id", "ycalc^nummer"],
        "filter": {
            "type": "atomic_condition",
            "name": "yproject",
            "value": project_number,
            "operator": "EQUALS",
        },
    }
    try:
        r = requests.post(base_address, json=params, timeout=30)
        r.raise_for_status()
        # response looks like this: {'success': True, 'result_data': [{'ycalc^nummer': '1530', 'id': '(1565,32,0)', 'nummer': '1013'}], 'message': ''}
        # lets extract the reslt_data
        r_json = r.json()
        row = r_json["result_data"][0] 
        return row.get("ycalc^nummer", ""), row.get("id", ""), row.get("nummer", "")  
    
    except requests.exceptions.RequestException as e:
        st.error(f"Fehler beim Abrufen der Gateway‑Kopf­daten: {e}")
        return None


def get_phase_end_dates(response: dict) -> Tuple[Dict[str, str], Dict[str, date]]:
    """
    Extrahiert die Enddaten der Phasen G6, G7 und G8 aus dem API-Payload
    und liefert sie (a) als String-Dictionary und (b) bereits geparst
    als date-Objekte zurück.

    Parameters
    ----------
    response : dict
        Voller JSON-Response, der u. a. response["result_data"]["table"] enthält.

    Returns
    -------
    Tuple[dict[str, str], dict[str, date]]
        (1) {phase_id: "dd.mm.yyyy"}  – unverändert als String  
        (2) {phase_id: date(yyyy, mm, dd)} – geparst für weitere Berechnungen
    """
    target_phases = {"G6", "G7", "G8"}

    # (1) Rohdaten als String sammeln
    end_dates = {
        item.get("ytzid"): item.get("ytenddate", "")
        for item in response.get("result_data", {}).get("table", [])
        if item.get("ytzid") in target_phases
    }

    # (2) In echte date-Objekte umwandeln, leere Strings überspringen
    milestones = {
        k: datetime.strptime(v, "%d.%m.%Y").date()
        for k, v in end_dates.items()
        if v
    }

    return end_dates, milestones




def page_overview(projektleiter):
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

def extract_department_hours(api_response: dict) -> dict[str, int]:
    """
    Convert the API's 'result_data' section to {department: hours}.

    Parameters
    ----------
    api_response : dict
        The full JSON object returned by the API call.

    Returns
    -------
    dict[str, int]
        Aggregated hours keyed by the human-readable department names.
    """
    dept_hours: dict[str, int] = {}
    for row in api_response.get("result_data", []):
        # row is a dict with raw calc fields and numeric hour totals
        for raw_field, hours in row.items():
            if raw_field in CALC_FIELD_TO_DEPT and isinstance(hours, (int, float)):
                dept = CALC_FIELD_TO_DEPT[raw_field]
                # accumulate in case the same dept appears in multiple rows
                dept_hours[dept] = dept_hours.get(dept, 0) + hours
    return dept_hours




def _resolve_anchor(anchor: str, milestones: dict[str, date]) -> date:
    """Gibt das Basisdatum für einen Ankerstring zurück."""
    if anchor == "TODAY":
        return date.today()
    return milestones[anchor]

def default_interval(dept: str, milestones: dict[str, date]) -> tuple[date, date]:
    """Berechnet Start- und Enddatum gemäss DATE_RULES."""
    a_s, off_s, a_e, off_e = DATE_RULES[dept]
    start = _resolve_anchor(a_s, milestones) + timedelta(days=off_s)
    end   = _resolve_anchor(a_e, milestones) + timedelta(days=off_e)
    return start, end

def page_task_creator():
    st.title("📋 Task Creator")
    project = st.text_input("Projekt‑Nr.")
    if project:
        # Get the gateway number, gateway ID and calculation number from the project number 
        calculation_number, gateway_id, gateway_number = get_gateway_id_and_calculation_number(project)
        gateway_data = fetch_gateway_data(gateway_id)
        global MILESTONES
        end_dates, MILESTONES = get_phase_end_dates(gateway_data)

        

        #Gateway Termine nebeneinander anzeigen
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("**Kick‑Off:**")
            st.write(end_dates["G6"])
        with col2:
            st.write("**Design:**")
            st.write(end_dates["G7"])
        with col3:
            st.write("**Produktion:**")
            st.write(end_dates["G8"])   


        # Get the calculation hours for this project
        calc_hours = fetch_calculation_hours(calculation_number)
        departement_hours = extract_department_hours(calc_hours)
        st.write("**Kalkulationsstunden:**")
        # Create a DataFrame from the dictionary        
        df_hours = pd.DataFrame.from_dict(departement_hours, orient='index', columns=['Stunden'])
        # Reset the index to get the department names as a column   
        df_hours.reset_index(inplace=True)
        # Rename the columns
        df_hours.columns = ['Abteilung', 'Stunden']
        # Sort the DataFrame by the 'Abteilung' column
        df_hours.sort_values(by='Abteilung', inplace=True)
        # Display the DataFrame
        st.dataframe(df_hours, use_container_width=True, hide_index=True)
        print(df_hours)
        # Button to create project plan
        if st.button("Projektplan erstellen"):
            df_active = df_hours[df_hours["Stunden"] > 0].copy()

            # 3) Leerspalten für Start, Ende, Aufgabe (anlegen für Data-Editor)
            df_active["Start"]   = pd.NaT
            df_active["Ende"]    = pd.NaT
            df_active["Aufgabe"] = ""

            for _, row in df_active.iterrows():
                dept = row["Abteilung"]
                start_def, end_def = default_interval(dept, MILESTONES)

                # Vier Spalten nebeneinander
                c1, c2, c3, c4 = st.columns([2, 2, 2, 4], gap="small")

                with c1:
                    st.markdown(f"**{dept}**")

                with c2:
                    row_start = st.date_input(
                        label="Start",
                        key=f"{dept}_start",
                        value=start_def,
                        format="YYYY-MM-DD",
                    )

                with c3:
                    row_end = st.date_input(
                        label="Ende",
                        key=f"{dept}_end",
                        value=end_def,
                        format="YYYY-MM-DD",
                    )

                with c4:
                    st.text_input(
                        label="Aufgabe",
                        key=f"{dept}_task",
                        value=TASK_NAMES.get(dept, ""),
                    )

    else:
        st.warning("Keine Daten  gefunden.")

        

# ---------------------------------------------------------------------------
# MAIN – navigation wrapper
# ---------------------------------------------------------------------------

def main():
    st.sidebar.title("Navigation")
    page_choice = st.sidebar.radio(
        "Seite auswählen:", ("PJM Overview", "Task Creator"), key="page_select"
    )

    # Projektleiter Kürzel (needs to be available on every page)
    if "projektleiter" not in st.session_state:
        st.session_state["projektleiter"] = "MRG"
    st.sidebar.text_input(
        "Projektleiter‑Kürzel:",
        key="projektleiter",
        value=st.session_state["projektleiter"],
    )

    # Route to the selected page
    if page_choice == "PJM Overview":
        page_overview(st.session_state["projektleiter"])
    else:
        page_task_creator()


if __name__ == "__main__":
    main()
