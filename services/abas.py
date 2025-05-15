from requests.adapters import HTTPAdapter, Retry
import requests
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional
from typing import Any
from pathlib import Path
import json
from copy import deepcopy
import streamlit as st

from .exceptions import (
    AbasApiError,
    AbasAuthError,
    AbasConnectionError,
    AbasHTTPError,
    AbasTimeoutError,
)
from requests import Response, Timeout, RequestException

SETTINGS_PATH = Path(__file__).parent / "settings.json"
# Aktuelles Datum und in 10 Arbeitstagen & 3 Tage zurück
heute = datetime.today()
arbeitstage_10spaeter = np.busday_offset(heute.date(), 10, roll='forward')
arbeitstage_3frueher = np.busday_offset(heute.date(), -3, roll='backward')

# Als Strings im Format DD.MM.YYYY
datum_heute = heute.strftime("%d.%m.%Y")
datum_plus10 = pd.Timestamp(arbeitstage_10spaeter).strftime("%d.%m.%Y")
datum_minus3 = pd.Timestamp(arbeitstage_3frueher).strftime("%d.%m.%Y")

DEFAULT_BASE_ADDRESS = "http://intra-erp:4444/EPLAN_WS_FREE_EDP"

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

SETTINGS_PATH = Path(__file__).parent / "settings.json"

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
    "base_adress": "http://intra-erp:4444/EPLAN_WS_FREE_EDP"
}

def load_settings() -> dict:
    settings = deepcopy(DEFAULT_SETTINGS)
    if SETTINGS_PATH.exists():
        try:
            user_cfg = json.loads(SETTINGS_PATH.read_text("utf-8"))

            # Tippfehler-Alias: base_adress → base_address
            if "base_adress" in user_cfg and "base_address" not in user_cfg:
                user_cfg["base_address"] = user_cfg.pop("base_adress")

            for k, v in user_cfg.items():
                if isinstance(v, dict) and k in settings:
                    settings[k].update(v)
                else:
                    settings[k] = v
        except json.JSONDecodeError as e:
            st.warning(f"settings.json defekt ({e}) – benutze Defaults")

    # ❶ globale Variable setzen, damit alle alten free functions weiterlaufen
    globals()["base_address"] = settings["base_address"]
    return settings

_SETTINGS = _load_settings()
BASE_ADDRESS = _SETTINGS["base_address"]        # ← EINMAL global verfügbar



class AbasService:
    
    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        timeout: int = 15,
    ):
        self._base = base_url.rstrip("/")
        self._session = session or AbasService.make_retry_session(timeout=timeout)
        self._timeout = timeout


    def release_tasks_to_departments(self,project_number: str):
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
        return self._post(params)
    
    def create_project_task_folder(self, project_number, task_name, date_start, date_end):
        
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
        return self._post(params)

    #create a task for a specific person in ABAS ERP
    def create_project_task_for_person(self, project_number, person_short, task_name, leiart, time_budget, date_start, date_end):
        
        
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
        return self._post(params)
    
    def create_project_task_for_department(self, project_number, department_short, task_name, time_budget, date_start, date_end):
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
        return self._post(params)

    def create_dispatch_milestone(self, project_number, person_short, date_start, date_end):
    
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
        return self._post(params)

    def fetch_gateway_data(self, gateway_id):
        # JSON-Payload analog zum C# Beispiel
        params = {
            "action": "read",
            "id": gateway_id,
            "fields": [],
            "table_fields": ["ytzid","ytname","ytenddate"]
        }
        return self._post(params)
    
    def fetch_calculation_hours(self, calculation_number):
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
        return self._post(params)
    
    def fetch_gateway_infosystem_sold_phase(self):
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
        return self._post(params)
    
    def fetch_gateway_infosystem(self, projektleiter_kuerzel):
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
        return self._post(params)
    
    def fetch_booked_hours(self, projektleiter_kuerzel):
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
        return self._post(params)
    
    def fetch_overbooked_projects(self, projektleiter_kuerzel):
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
        return self._post(params)
    
    def get_gateway_id_and_calculation_number(self,
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
        return self._post(params)

    def fetch_dispatch_infosystem(self, projektleiter_kuerzel):
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
        return self._post(params)
    
    def fetch_open_tasks(self, projektleiter_kuerzel):
        params = {
            "action": "infosystem",
            "infosystem": "10345",
            "data": [
                {"name": "bearbeit", "value": projektleiter_kuerzel},
                {"name": "bstart", "value": "1"}
            ],
            "table_fields": ["taufgabe^nummer","taufgabe^projekt^nummer","taufgabe^yprojektname^namebspr","taufgabe^start","taufgabe^end","taufgabenname^namebspr","tbestaetigername^namebspr"]
        }
        return self._post(params)
    
    def make_retry_session(retries: int = 3, timeout: int = 15) -> requests.Session:
        sess = requests.Session()
        retry = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=(502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
        sess.request_timeout = timeout          # eigener Attr – praktisch fürs Logging
        return sess

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            resp: Response = self._session.post(self._base, json=payload, timeout=self._timeout)
        except Timeout as exc:
            raise AbasTimeoutError(
                "Gateway Timeout", endpoint=self._base, payload=payload
            ) from exc
        except RequestException as exc:
            raise AbasConnectionError(
                "Netzwerkfehler", endpoint=self._base, payload=payload
            ) from exc

        if resp.status_code >= 400:
            raise AbasHTTPError.from_response(resp, payload=payload)

        data = resp.json()
        if not data.get("success", True):
            code = data.get("code")
            exc_cls = AbasAuthError if code == "AUTH" else AbasApiError
            raise exc_cls(code, data.get("message", "Unbekannter API-Fehler"),
                          endpoint=self._base, payload=payload)
        return data
    
    
