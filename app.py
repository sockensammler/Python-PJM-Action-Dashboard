import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import streamlit as st
from typing import Dict, Tuple, Optional
import streamlit.column_config as cc 
import matplotlib.pyplot as plt
from pathlib import Path
import json
from copy import deepcopy


base_address = "http://intra-erp:4444/EPLAN_WS_FREE_EDP"

SETTINGS_PATH = Path(__file__).parent / "settings.json"

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
# add the Department "IPC" and "TECHNIKUM" to the list
ALL_DEPTS += ["IPC", "TECHNIKUM"]

#Aufgabennamen
TASK_NAMES = {
    "BILDGEBUNG": "Imaging Design",
    "MCAD": "MCAD Konstruktionsphase (inklusive Kundenlayout)",
    "ECAD": "ECAD Konstruktionsphase",
    "PROJECTMANAGEMENT": "Project Planning",
    "PRODUCT DEVELOPMENT": "Special Development",  
    "SOFTWARE": "Software Installation",
    "TD": "Manual",
    "AUTOMATION": "Automation",
}
# -------
DATE_RULES = {
    "BILDGEBUNG":          ("G6", 0,    "G6",  7),
    "MCAD":                ("G7", -14,  "G7", -7),
    "ECAD":                ("G7", -14,  "G7", -7),
    "PROJECTMANAGEMENT":   ("TODAY", -5, "G8",  0),
    "PRODUCT DEVELOPMENT": ("G6", 0,    "G7",  0),  # Ende Engineering = G7
    "SOFTWARE":            ("G7", 1,    "G7", 14),
    "TD":                  ("G8", -10,  "G8", -3),
    "AUTOMATION":          ("G7", 0,  "G7", 14)
}


DEFAULT_SETTINGS = {
    "doppelte_bildgebungsaufgabe": True,
    "mcad_ecad_freigabeaufgabe": True,
    "task_names": TASK_NAMES.copy(),     # aus der alten Konstante
    "date_rules": {k: list(v) for k, v in DATE_RULES.items()},
}

# TERMINREGELN – leicht anpassbar
# ---------------------------------------------------------------------------
# Jeder Eintrag: (Start‑Anker, Start‑Offset‑Tage, End‑Anker, End‑Offset‑Tage)
# Anker = "G6", "G7", "G8"  oder "TODAY"


# ---------------------------------------------------------------------------
# HILFSFUNKTION – Default‑Start/Ende nach Regelwerk berechnen
# ---------------------------------------------------------------------------
def release_tasks_to_departments(project_number: str):
        params = {
        "action": "infosystem",
        "infosystem": "PRJMAUFAN",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "yvondatum", "value": ""},
            {"name": "ybisdatum", "value": ""},
            {"name": "bstart", "value": "1"},
            {"name": "ybuanlegen", "value": "1"}
        ]
        }
        try:
            res = requests.post(base_address, json=params, timeout=30)
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Fehler beim Abrufen der Dispatch-Daten: {e}")
            return None


def load_settings() -> dict:
    settings = deepcopy(DEFAULT_SETTINGS)
    if SETTINGS_PATH.exists():
        try:
            user_cfg = json.loads(SETTINGS_PATH.read_text("utf-8"))
            # flach plus verschachteltes Überschreiben
            for k, v in user_cfg.items():
                if isinstance(v, dict) and k in settings:
                    settings[k].update(v)
                else:
                    settings[k] = v
        except json.JSONDecodeError as e:
            st.warning(f"settings.json defekt ({e}) – benutze Defaults")
    return settings
def save_settings(settings: dict):
    SETTINGS_PATH.write_text(json.dumps(settings, indent=2), encoding="utf-8")

def _default_dates(dept: str, gw_dates: Dict[str, str]):
    rule = DATE_RULES.get(dept)
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

def create_project_task_folder(project_number, task_name, date_start, date_end):
    
    # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "create",
        "database_and_group": "149:02",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "namebspr", "value": task_name},
            {"name": "ypvtyp", "value": "Sammelvorgang"},
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

#create a task for a specific person in ABAS ERP
def create_project_task_for_person(project_number, person_short, task_name, leiart, time_budget, date_start, date_end):
    
    
    # JSON-Payload analog zum C# Beispiel
    params = {
        "action": "create",
        "database_and_group": "149:02",
        "data": [
            {"name": "yprojekt", "value": project_number},
            {"name": "ypersonal", "value": person_short},
            {"name": "namebspr", "value": task_name},
            {"name": "yleiart", "value": leiart},
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
    
def get_gateway_id_and_calculation_number(
    project_number: str,
) -> Optional[Tuple[str, str, str]]:
    """
    Liefert (calc_no, gateway_id, gateway_no).
    Gibt None zurück, wenn etwas fehlt oder ein Fehler auftritt.
    """
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
        r_json = r.json()

        if not r_json.get("success"):
            st.error(f"ABAS meldet Fehler: {r_json.get('message', 'kein Text')}")
            return None

        rows = r_json.get("result_data", [])
        if not rows:
            st.warning(
                f"Projekt {project_number} wurde nicht gefunden. "
                "Existiert hier möglicherweise ein Teilprojekt?"
            )
            return None

        row = rows[0]

        calc_no = row.get("ycalc^nummer")
        if not calc_no:                                   # <<–– neuer Check
            st.error(
                "❌ Für dieses Gateway ist keine Kalkulationsnummer "
                "('ycalc^nummer') hinterlegt. "
                "Bitte in ABAS nachtragen oder das Projekt prüfen."
            )
            return None

        return calc_no, row.get("id", ""), row.get("nummer", "")

    except requests.exceptions.RequestException as e:
        st.error(f"HTTP-Fehler beim Abruf der Gateway-Kopfdaten: {e}")
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

def plot_gantt(df_active: pd.DataFrame, milestones: dict[str, date]):
    """
    Erstellt ein Gantt-Diagramm mit
      • einer Task-Zeile pro Abteilung
      • Kalenderwochen-Gitternetz
      • Meilenstein-Linien G6–G8
    Gibt eine matplotlib-Figure zurück.
    """
    df = df_active.copy()
    df["Start"] = pd.to_datetime(df["Start"])
    df["Ende"]  = pd.to_datetime(df["Ende"])
    df = df.sort_values("Start")

    # Achsen-Setup
    fig, ax = plt.subplots(figsize=(10, 0.6 * len(df) + 2))
    y_pos = range(len(df))

    # Tasks als horizontale Balken
    for i, (_, r) in enumerate(df.iterrows()):
        ax.barh(i, (r["Ende"] - r["Start"]).days, left=r["Start"])
        ax.text(r["Start"], i, f' {r["Abteilung"]}', va="center")

    # Meilensteine (gestrichelte Linien)
    for label, when in milestones.items():
        ax.axvline(when, linestyle="--")
        ax.text(when, len(df) + 0.2, label, rotation=90, va="bottom")

    # Kalenderwochen-Gitternetz & Beschriftung
    start, end = df["Start"].min().normalize(), df["Ende"].max().normalize()
    for w in pd.date_range(start, end, freq="W-MON"):
        ax.axvline(w, alpha=0.2, linewidth=0.5)
        ax.text(w, -1, f'KW{w.isocalendar().week}', rotation=90,
                va="top", fontsize=12)

    ax.set_yticks([])
    ax.set_ylim(-1, len(df) + 1)
    ax.set_xlabel("Datum")
    fig.tight_layout()
    return fig

def page_overview(projektleiter: str, settings: dict):
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

# Seite zum Erstelllen eines neuen Projektplans
def page_task_creator(projektleiter: str, settings: dict):
    st.title("📋 Projektplan anlegen")
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
        
        st.write("**Projektplan:**")


        # nur Abteilungen mit gebuchten Stunden
        # Abteilungen >0 h
        df_active = df_hours[df_hours["Stunden"] > 0].copy()

        # << Wichtig >>
        df_active.reset_index(drop=True, inplace=True)   # lückenlosen Index erzwingen

        # Start/Ende spaltenweise einsetzen
        df_active[["Start", "Ende"]] = (
            df_active["Abteilung"]
            .apply(lambda d: pd.Series(default_interval(d, MILESTONES)))
        )

        # Aufgabentexte
        df_active["Aufgabe"] = df_active["Abteilung"].map(TASK_NAMES)

        # Datumstyp für Streamlit
        df_active[["Start", "Ende"]] = df_active[["Start", "Ende"]].apply(pd.to_datetime)


        
        # Editor
        cfg = {
            "Abteilung": cc.SelectboxColumn(
                "Abteilung",
                options= ALL_DEPTS,      # Dropdown-Einträge
                required=True,             # darf nicht leer bleiben
                help="Zuständige Abteilung auswählen",
            ),
            "Start": cc.DatetimeColumn("Start", format="DD.MM.YYYY"),
            "Ende": cc.DatetimeColumn("Ende", format="DD.MM.YYYY"),
        }
        
        edited = st.data_editor(
            df_active,
            column_config=cfg,
            num_rows="dynamic",      # Zeilen hinzufügen/entfernen
            hide_index=True,
            use_container_width=True,
            key="task_editor",
        )

        # Gantt zeichnen
        # Meilensteine (bereits als date-Objekte geparst)
        milestones = {"Ende Kick-Off": MILESTONES["G6"], "Ende Design": MILESTONES["G7"], "Ende Produktion" : MILESTONES["G8"]}
        fig = plot_gantt(edited, milestones)
        st.pyplot(fig, use_container_width=True)

        # Button für die Erstellung der Aufgaben
        if st.button("Aufgaben anlegen"):
            # Aufgaben für alle Abteilungen anlegen
            for _, row in edited.iterrows():

                if row["Abteilung"] == "PROJECTMANAGEMENT":
                    # Aufgabe für Projektleiter anlegen
                    create_project_task_for_person(
                        project,
                        st.session_state["projektleiter"],
                        row["Aufgabe"],
                        "AUFTRAGSTEUERUNG",
                        row["Stunden"],
                        row["Start"].strftime("%d.%m.%Y"),
                        row["Ende"].strftime("%d.%m.%Y"),
                    )
                elif row["Abteilung"] == "BILDGEBUNG":
                    # Hauptbildgebungsaufgabe anlegen
                    create_project_task_for_department(
                        project,
                        row["Abteilung"],
                        row["Aufgabe"],
                        row["Stunden"],
                        row["Start"].strftime("%d.%m.%Y"),
                        row["Ende"].strftime("%d.%m.%Y"),
                    )
                    if settings["doppelte_bildgebungsaufgabe"] == True:
                    # Bildgebung MCAD/ECAD unterstützung anlegen
                        imaging_support_start = row["Ende"] + timedelta(days=1)
                        imaging_support_end = imaging_support_start + timedelta(days=14)
                        create_project_task_for_department(
                            project,
                            row["Abteilung"],
                            "Bildgebung - MCAD/ECAD Unterstützung",
                            row["Stunden"],
                            imaging_support_start .strftime("%d.%m.%Y"),
                            imaging_support_end.strftime("%d.%m.%Y"),
                        )
                elif row["Abteilung"] == "IPC":
                    # Hauptbildgebungsaufgabe anlegen
                    create_project_task_for_person(
                        project,
                        "RH",
                        row["Aufgabe"],
                        "SOFTWARE",
                        row["Stunden"],
                        row["Start"].strftime("%d.%m.%Y"),
                        row["Ende"].strftime("%d.%m.%Y"),
                    )

                else:
                    create_project_task_for_department(
                        project,
                        row["Abteilung"],
                        row["Aufgabe"],
                        row["Stunden"],
                        row["Start"].strftime("%d.%m.%Y"),
                        row["Ende"].strftime("%d.%m.%Y"),
                    )

            if settings["mcad_ecad_freigabeaufgabe"] == True:
            # Freigabe-Task anlegen MCAD
                if "MCAD" in edited["Abteilung"].values:
                    create_project_task_for_department(
                        project,
                        "MCAD",
                        "MCAD - Interne Freigabe",
                        0,
                        MILESTONES["G7"].strftime("%d.%m.%Y"),
                        MILESTONES["G7"].strftime("%d.%m.%Y"),
                    )      
                if "ECAD" in edited["Abteilung"].values:
                    create_project_task_for_department(
                        project,
                        "ECAD",
                        "ECAD - Interne Freigabe",
                        0,
                        MILESTONES["G7"].strftime("%d.%m.%Y"),
                        MILESTONES["G7"].strftime("%d.%m.%Y"),
                    )        
            # Meilenstein DISPATCH anlegen
            create_dispatch_milestone(
                project,
                st.session_state["projektleiter"],
                MILESTONES["G8"].strftime("%d.%m.%Y"),
                MILESTONES["G8"].strftime("%d.%m.%Y")
            )
            # Support-Aufgabe anlegen
            create_project_task_for_department(
                project,
                "INTRAVIS",
                "Support",
                0,
                MILESTONES["G8"].strftime("%d.%m.%Y"),
                MILESTONES["G8"].strftime("%d.%m.%Y")
            )
            # Release Tasks anlegen

            st.success("Aufgaben erfolgreich angelegt.")

            
    else:
        st.warning("Keine Daten  gefunden.")


def page_settings(settings: dict):
    st.title("⚙️ Einstellungen")

    # 1. einfache Flags
    settings["doppelte_bildgebungsaufgabe"] = st.checkbox(
        "Zweite Bildgebungsaufgabe",
        value=settings.get("doppelte_bildgebungsaufgabe", False)
    )
    settings["mcad_ecad_freigabeaufgabe"] = st.checkbox(
        "MCAD/ECAD-Freigabeaufgaben anlegen",
        value=settings.get("mcad_ecad_freigabeaufgabe", False)
    )

    # 2. TASK_NAMES als editierbare Tabelle
    st.subheader("Aufgabentexte pro Abteilung")
    tn_df = pd.DataFrame(
        [{"Abteilung": k, "Aufgabe": v} for k, v in settings["task_names"].items()]
    )
    edited_tn = st.data_editor(tn_df, hide_index=True, num_rows="dynamic")
    settings["task_names"] = {
        row["Abteilung"]: row["Aufgabe"] for _, row in edited_tn.iterrows()
        if row["Abteilung"]
    }

    # 3. DATE_RULES (Vierer-Liste) editierbar machen
    st.subheader("Terminregeln")
    dr_df = pd.DataFrame(
        [
            {"Abteilung": k, "StartAnchor": v[0], "StartOff": v[1],
             "EndAnchor": v[2], "EndOff": v[3]}
            for k, v in settings["date_rules"].items()
        ]
    )
    edited_dr = st.data_editor(
        dr_df, hide_index=True, num_rows="dynamic",
        column_config={
            "StartOff": st.column_config.NumberColumn("StartOff", step=1),
            "EndOff":   st.column_config.NumberColumn("EndOff",   step=1),
        }
    )
    settings["date_rules"] = {
        row["Abteilung"]: [row["StartAnchor"], int(row["StartOff"]),
                           row["EndAnchor"],   int(row["EndOff"])]
        for _, row in edited_dr.iterrows() if row["Abteilung"]
    }
    st.write("G6: Kick-Off / G7: Design / G8: Produktion")

    if st.button("Speichern"):
        save_settings(settings)
        st.success("Einstellungen gespeichert")
   

# ---------------------------------------------------------------------------
# MAIN – navigation wrapper
# ---------------------------------------------------------------------------

def main():
    st.sidebar.title("Navigation")
    page_choice = st.sidebar.radio(
        "Seite auswählen:",
        ("PJM Overview", "Projektplan anlegen", "Einstellungen"),  # hier ergänzt
        key="page_select"
    )

    # Projektleiter-Kürzel wie gehabt …
    if "projektleiter" not in st.session_state:
        st.session_state["projektleiter"] = "MRG"
    st.sidebar.text_input("Projektleiter-Kürzel:", key="projektleiter",
                          value=st.session_state["projektleiter"])

    # Einstellungen laden
    settings = load_settings()
    TASK_NAMES.update(settings["task_names"])
    DATE_RULES.update({k: tuple(v) for k, v in settings["date_rules"].items()})


    if page_choice == "PJM Overview":
        page_overview(st.session_state["projektleiter"], settings)
    elif page_choice == "Projektplan anlegen":
        page_task_creator(st.session_state["projektleiter"], settings)
    else:
        page_settings(settings)



if __name__ == "__main__":
    main()
