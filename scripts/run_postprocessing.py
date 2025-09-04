import pandas as pd

geordie_df = pd.read_parquet('../data/energy_planning_geordie.parquet')
affilgood_df = pd.read_pickle('../data/energy_planning_affilgood.pkl')

#merge both outputs
merged_df = geordie_df.reset_index().merge(affilgood_df.reset_index()[['index', 'affilgood']], on='index', how='left').drop('index',axis=1)

import re
from typing import Any, Dict, List, Optional, Tuple
import re
import pandas as pd
import numpy as np

# =========================
# Affiliations
# =========================

def _clean_ror(value):
    """
    Normalize ROR to a list of strings, split on '|',
    and strip trailing ':0.xx' probabilities.
    """
    if value is None or _isnan(value):
        return []
    if isinstance(value, (list, np.ndarray)):
        values = []
        for v in value:
            if v is None or str(v).strip() == "":
                continue
            for part in str(v).split("|"):
                cleaned = re.sub(r':[0-9.]+$', '', part.strip())
                if cleaned:
                    values.append(cleaned)
        return values
    if isinstance(value, str):
        return [re.sub(r':[0-9.]+$', '', p.strip()) for p in value.split("|") if p.strip()]
    return [re.sub(r':[0-9.]+$', '', str(value))]

def _isnan(x):
    """Safe NaN checker for scalars only."""
    if isinstance(x, (list, dict, np.ndarray)):
        return False
    try:
        return pd.isna(x)
    except Exception:
        return False


def _coords_to_osm_link(coords_str):
    if not coords_str or _isnan(coords_str):
        return None
    m = re.match(r"\('?\s*([0-9.\-]+)'?\s*,\s*'?\s*([0-9.\-]+)'?\s*\)", str(coords_str))
    if not m:
        return None
    lat, lon = m.group(1), m.group(2)
    return f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=12/{lat}/{lon}"

def _as_list(x):
    """Normalize input to a list of non-empty strings."""
    if x is None or _isnan(x):
        return []
    if isinstance(x, (list, np.ndarray)):
        return [str(s) for s in x if str(s).strip() != ""]
    if isinstance(x, str):
        return [x] if x.strip() else []
    return [str(x)]

def _is_empty_ror(ror_value):
    if ror_value is None:
        return True
    if isinstance(ror_value, list):
        return len([s for s in ror_value if str(s).strip() != ""]) == 0
    return str(ror_value).strip() == ""

def parse_affil_list(lst):
    if not isinstance(lst, list) or len(lst) == 0 or _isnan(lst):
        return []

    out = []
    for item in lst:
        if not isinstance(item, dict):
            continue

        text = item.get('raw_text') or item.get('text')

        # --- ROR (clean, split on '|' and strip ':prob') ---
        ror_block = (item.get('entity_linking') or {}).get('ror') or {}
        ror_raw = ror_block.get('linked_orgs') or ror_block.get('linked_orgs_spans')
        # IMPORTANT: _clean_ror now returns a list already
        ror_list = _clean_ror(ror_raw)  # e.g. ["Org A {url}", "Org B {url}"]

        # --- Fallback to ORG from ner if ROR empty ---
        if not ror_list:  # empty or None -> use ORG from ner
            ner = item.get('ner') or []
            if isinstance(ner, list) and ner:
                first_ner = ner[0] if isinstance(ner[0], dict) else {}
                org_field = first_ner.get('ORG') or []
                ror_list = _as_list(org_field)
            else:
                ror_list = []

        # raw_entity from ner
        ner = item.get('ner') or []
        city = country = None
        if isinstance(ner, list) and ner:
            first_ner = ner[0] if isinstance(ner[0], dict) else {}
            city_list = _as_list(first_ner.get('CITY'))
            country_list = _as_list(first_ner.get('COUNTRY'))
            city = city_list[0] if city_list else None
            country = country_list[0] if country_list else None

        raw_entity = f"{city}, {country}" if city and country else (city or country or None)

        # OSM fields
        osm_city = osm_country = osm_link = None
        osm = item.get('osm') or []
        if isinstance(osm, list) and osm:
            osm0 = osm[0]
            if isinstance(osm0, dict):
                osm_city = osm0.get('CITY')
                osm_country = osm0.get('COUNTRY')
                osm_link = _coords_to_osm_link(osm0.get('COORDS'))

        out.append({
            'text': text,
            'ror': ror_list,          # list after split/clean
            'raw_entity': raw_entity,
            'osm_city': osm_city,
            'osm_country': osm_country,
            'osm_link': osm_link
        })

    return out

def _tolist(x: Any) -> Any:
    if hasattr(x, "tolist"):
        try:
            return x.tolist()
        except Exception:
            return x
    return x

def _as_list_of_dicts(obj: Any) -> List[Dict[str, Any]]:
    """Accept list/dict/np.ndarray/None and return a clean list[dict]."""
    obj = _tolist(obj)
    if obj is None:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []

def _first_role_label(role_field: Any) -> Optional[str]:
    """Role can be list/np.ndarray/dict; return first label if present."""
    role_field = _tolist(role_field)
    if role_field is None:
        return None
    if isinstance(role_field, list) and role_field:
        first = role_field[0]
        if isinstance(first, dict):
            return first.get("label")
        if isinstance(first, str):
            return first
    if isinstance(role_field, dict):
        return role_field.get("label")
    return None

def _extract_osm_id(osm_raw: Dict[str, Any]) -> Optional[str]:
    """Return a clean string osm_id if present (e.g., 52411.0 -> '52411')."""
    if not isinstance(osm_raw, dict):
        return None
    osm_id = osm_raw.get("osm_id") or osm_raw.get("OSM_ID")
    if osm_id is None:
        return None
    if isinstance(osm_id, float):
        return str(int(osm_id)) if float(osm_id).is_integer() else str(osm_id)
    return str(osm_id)

def _extract_place_id(osm_raw: Dict[str, Any]) -> Optional[str]:
    """Return a clean string place_id if present (handles floats like 95920172.0)."""
    if not isinstance(osm_raw, dict):
        return None
    pid = osm_raw.get("place_id") or osm_raw.get("PLACE_ID")
    if pid is None:
        return None
    if isinstance(pid, float):
        return str(int(pid)) if float(pid).is_integer() else str(pid)
    return str(pid)

def _build_osm_url_from_any(osm_raw: Dict[str, Any], fallback_osm: Dict[str, Any] = None) -> Optional[str]:
    """
    Prefer /{osm_type}/{osm_id}; otherwise fall back to lat/lon
    (in either osm_raw or fallback_osm or AffilGood-style COORDS).
    """
    d = osm_raw if isinstance(osm_raw, dict) else {}
    osm_type = (d.get("osm_type") or d.get("OSM_TYPE") or "").lower()
    osm_id = d.get("osm_id") or d.get("OSM_ID")
    if osm_type and osm_id is not None:
        try:
            if isinstance(osm_id, float) and float(osm_id).is_integer():
                osm_id = int(osm_id)
        except Exception:
            pass
        return f"https://www.openstreetmap.org/{osm_type}/{osm_id}"

    lat, lon = d.get("lat"), d.get("lon")

    # AffilGood-style "COORDS": "('lat','lon')"
    if (lat is None or lon is None) and d.get("COORDS"):
        m = re.search(r"\(\s*'?(?P<lat>[-\d\.]+)'?\s*,\s*'?(?P<lon>[-\d\.]+)'?\s*\)", str(d["COORDS"]))
        if m:
            lat, lon = m.group("lat"), m.group("lon")

    f = fallback_osm if isinstance(fallback_osm, dict) else {}
    lat = lat if lat is not None else f.get("lat")
    lon = lon if lon is not None else f.get("lon")

    if lat is not None and lon is not None:
        return f"https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=12/{lat}/{lon}"
    return None

# --- role normalization -----------------------------------------------------

_ALLOWED_TO_OBJECT = {
    "Object of study",
    "Target audience, beneficiary or impacted entity",
    "Location of research",
}

def _normalize_role(label: Optional[str]) -> str:
    """
    Map incoming role label to our two-way scheme.
    """
    if label in _ALLOWED_TO_OBJECT:
        return "Object of study"
    return "Other"

# --- entity shaping + de-dup ------------------------------------------------

def _entity_record_from_item(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map one Geordie entity dict -> requested schema."""
    if not isinstance(item, dict):
        return None
    raw_ent = item.get("entity")
    norm_ent = item.get("entity_normalised")
    if not (raw_ent or norm_ent):
        return None
    osm_raw = item.get("osm_raw") or {}
    fallback_osm = item.get("osm") or {}
    role_label = _first_role_label(item.get("role"))
    role = _normalize_role(role_label)

    return {
        "raw_entity": raw_ent if raw_ent is not None else (norm_ent or ""),
        "role": role,
        "osm_entity": norm_ent if norm_ent is not None else (raw_ent or ""),
        "osm_link": _build_osm_url_from_any(osm_raw, fallback_osm),
        "osm_id": _extract_osm_id(osm_raw),
        "place_id": _extract_place_id(osm_raw),
    }

def _entities_from_geordie(geordie_like: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in _as_list_of_dicts(geordie_like):
        rec = _entity_record_from_item(it)
        if rec:
            out.append(rec)
    return out

def _dedupe_entities(ents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Drop duplicates based on (osm_id, place_id, role). Keep first occurrence.
    This collapses e.g. 'Chile' vs 'Chilean' mapping to same OSM record.
    """
    seen = set()
    out: List[Dict[str, Any]] = []
    for e in ents:
        key = (e.get("osm_id"), e.get("place_id"), e.get("role"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out

# =========================
# Title & abstract
# =========================
def build_title_abstract_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Input row keys: 'title', 'title_geordie', 'abstract', 'abstract_geordie'
    Output:
    {
      "title":   {"text": "<raw title>",    "entities": [ ... ]},
      "abstract":{"text": "<raw abstract>", "entities": [ ... ]}
    }
    Entities schema: {raw_entity, role, osm_entity, osm_link, osm_id, place_id}
    """
    title_text = row.get("title") or ""
    abs_text = row.get("abstract") or ""
    title_entities = _dedupe_entities(_entities_from_geordie(row.get("title_geordie")))
    abstract_entities = _dedupe_entities(_entities_from_geordie(row.get("abstract_geordie")))
    return {
        "title":    {"text": str(title_text), "entities": title_entities},
        "abstract": {"text": str(abs_text),   "entities": abstract_entities},
    }

# =========================
# Full text sections
# =========================

# --- helpers for section number parsing & matching -------------------------

def _num_tuple(section_num: Any) -> Tuple[int, ...]:
    """
    Convert '3.', '3.1', '3.1a', '02.10.' -> (3,) (3,1) (3,1) (2,10)
    Non-numeric parts are ignored; empty/None -> ().
    """
    if section_num is None:
        return ()
    s = str(section_num).strip().rstrip(".")
    if not s:
        return ()
    parts = []
    for p in s.split("."):
        p = p.strip()
        if not p:
            continue
        m = re.search(r"(\d+)", p)
        if m:
            parts.append(int(m.group(1)))
    return tuple(parts)

def _is_sub_of(num: Tuple[int, ...], root: Tuple[int, ...]) -> bool:
    """True if 'num' is the same as or nested under 'root' (prefix match)."""
    if not root:
        return False
    if len(num) < len(root):
        return False
    return num[:len(root)] == root

# --- label gating configuration -------------------------------------------

_METHODY_RE = re.compile(r"\b(methods?|methodolog(?:y|ies)|methdology)\b", re.IGNORECASE)
_DATA_RE    = re.compile(r"\bdata\b", re.IGNORECASE)

def _section_is_allowed_by_name(name: str) -> bool:
    return bool(_METHODY_RE.search(name or "")) or bool(_DATA_RE.search(name or ""))

# --- main builder for geordie output----------------------------------------------------------

def build_fulltext_sections_payload(
    fulltext_sections: Any,
    fulltext_sections_geordie: Any = None
) -> List[Dict[str, Any]]:
    """
    Returns a list:
      [
        {
          "section_name": str,
          "section_num": Any,
          "text": str,
          "entities": [ {raw_entity, role, osm_entity, osm_link, osm_id, place_id}, ... ]
        },
        ...
      ]

    Refinement: if a section is NOT (Methods/Methodology/Methdology/Data) and
    NOT a *subsection of a Methods* section (by numbering), then every entity's
    role is overridden to "Other". In allowed sections, roles are normalized
    with the two-way mapping (Object of study / Other).
    """
    secs = _as_list_of_dicts(fulltext_sections)
    geos = _tolist(fulltext_sections_geordie)
    use_parallel = isinstance(geos, list) and len(geos) == len(secs)

    # 1) Identify the numeric roots for any Methods-like sections
    method_roots: List[Tuple[int, ...]] = []
    for sec in secs:
        name = (sec.get("section_name") or "").strip()
        if _METHODY_RE.search(name):  # only "Methods"-family count for subtree
            rt = _num_tuple(sec.get("section_num"))
            if rt:
                method_roots.append(rt)

    sections_list: List[Dict[str, Any]] = []

    # 2) Build payload with role override logic
    for idx, sec in enumerate(secs):
        name = (sec.get("section_name") or "").strip()
        num_raw = sec.get("section_num")
        text = (sec.get("section_content") or "").strip()

        # pick matching geordie output for this section
        g = None
        if use_parallel:
            g = geos[idx]
        if isinstance(g, dict) and "section_content_geordie" in g:
            g = g["section_content_geordie"]
        if g is None and "section_content_geordie" in sec:
            g = sec["section_content_geordie"]

        ents = _entities_from_geordie(g)

        # Decide if this section keeps normalized roles
        allowed = _section_is_allowed_by_name(name)
        if not allowed:
            nt = _num_tuple(num_raw)
            if any(_is_sub_of(nt, rt) for rt in method_roots):
                allowed = True

        # Normalize & optionally override roles; then de-duplicate
        if allowed:
            # roles already normalized inside _entity_record_from_item()
            pass
        else:
            for e in ents:
                e["role"] = "Other"

        ents = _dedupe_entities(ents)

        sections_list.append({
            "section_num": num_raw,
            "section_name": name,
            "text": text,
            "entities": ents
        })

    return sections_list

# =========================
#  wrapper
# =========================
def build_all_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience: process title/abstract and fulltext from one row.
    Expects keys: title, title_geordie, abstract, abstract_geordie,
                  fulltext_sections, fulltext_sections_geordie
    """

    affils = parse_affil_list(row['affilgood'])
    ta = build_title_abstract_payload(row)
    ft = build_fulltext_sections_payload(
        row.get("fulltext_sections"),
        row.get("fulltext_sections_geordie")
    )

    return {"doi":row['doi'], "affiliations":affils, "title": ta["title"], "abstract": ta["abstract"], "fulltext": ft}


import gzip
import json
from datetime import datetime

# Optional: simple logger
def _log(msg):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def write_payloads_jsonl_gz(df, path, builder, log_every=1000):
    """
    Stream rows of df through `builder(row)` and write each payload as one JSON line
    to a gzip-compressed file at `path`.
    """
    n = len(df.index)
    written = 0
    errors = 0

    with gzip.open(path, "wt", encoding="utf-8") as out:
        for i, idx in enumerate(df.index, start=1):
            try:
                payload = builder(df.loc[idx])  # your build_all_payload
                if payload is None:
                    continue
                out.write(json.dumps(payload, ensure_ascii=False) + "\n")
                written += 1
            except Exception as e:
                errors += 1
                # Comment out if you prefer silence
                _log(f"row {idx}: skipped due to error: {e}")

            if log_every and i % log_every == 0:
                _log(f"processed {i}/{n} rows, written={written}, errors={errors}")

    _log(f"done. total rows={n}, written={written}, errors={errors}")


# Everything in one go (if row has all columns)
# build_all_payload(merged_df.loc[8])

write_payloads_jsonl_gz(merged_df, "../data/processed_papers.jsonl.gz", build_all_payload, log_every=2000)