# SciLake Energy Pilot – GeoTagging Scientific Publications

This repository provides examples and workflows for **geotagging scientific publications** in the context of the **Energy Planning Pilot** of the [SciLake European Project](https://www.scilake.eu/).  

The repository demonstrates how to extract and normalize **geographical information** from scientific publications using two main components:

- [**AffilGood**](https://github.com/sirisacademic/affilgood/tree/main/docs)  
  Identifies and geolocates geographical components of **affiliations** extracted from publications.  
  📄 Paper: [*AffilGood: Building reliable institution name disambiguation tools to improve scientific literature analysis* (ACL 2024)](https://aclanthology.org/2024.sdp-1.13/)

- [**GEORDIE**](https://github.com/sirisacademic/geordie/tree/dev)  
  Extracts **geographical mentions** in text (title, abstract, full text), normalizes them, and characterizes them by semantic role.  
  📄 Paper: under development

---

## ✨ Goal

To build a reproducible pipeline that:
1. Uses **[GROBID](https://github.com/kermitt2/grobid)** to extract structured metadata and fulltext (affiliations, title, abstract, body).  
2. Applies **AffilGood** to process author affiliations and obtain geotagged institution locations.  
3. Applies **GEORDIE** to detect and normalize geographical mentions across the publication text.  
4. Produces enriched publication data for **energy planning research** in the SciLake pilot.

## 🔬 Context
This work supports the Energy Planning pilot of the SciLake project by enabling:
* Mapping of research contributions to geographical contexts
* Supporting knowledge graphs with spatial dimensions
* Facilitating regional and cross-country energy analysis

---

## 📂 Repository Structure

```
scilake-energy-pilot-geo/
├── output/ # processed files
├── notebooks/ # Jupyter notebooks with usage examples
├── scripts/ # Helper scripts for batch processing
├── examples/ # Sample outputs (geotagged results)
└── README.md # This file
```

---

## 🧩 Environment setup

We recommend using Python 3.10+ for best compatibility.


**Conda (recommended)**
```
# from repo root (where environment.yml lives)
mamba env create -f environment.yml   # or: conda env create -f environment.yml
mamba activate sl-energy-geo          # or: conda activate sl-energy-geo

# (optional) register the kernel name for Jupyter
python -m ipykernel install --user --name sl-energy-geo
```

**Python env**
```
# create venv with Python 3.10 (adjust path/command if needed)
python3.10 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows PowerShell

# upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

---
## 🚀 Usage

A complete notebook for processing one publication is available in [notebooks/demo](https://github.com/nicolauduran45/scilake-geotagging-energy-pilot/blob/main/notebooks/demo.ipynb).

### 1. Process publications with GROBID
Extract structured metadata and full text, documention available [here](https://github.com/kermitt2/grobid_client_python).

### 2. Run on the sample of papers of interest

```bash
# Example: process a batch of publications
python scripts/run_geordie.py
python scripts/run_affilgood.py
python scripts/run_postprocessing.py
```

---
## 📊 Example Output
```json
{
  "doi": "....",
  "affiliations": [
    {
      "text": "....",
      "ror": ["...."],                // normalized; multiple items possible
      "raw_entity": "....",           // "City, Country" (from NER)
      "osm_city": "....",
      "osm_country": "....",
      "osm_link": "...."
    }
  ],
  "title": {
    "text": "....",
    "entities": [
      {
        "raw_entity": "....",
        "role": "Object of Study",    // see roles note below
        "osm_entity": "....",
        "osm_link": "....",
        "osm_id": "....",
        "place_id": "...."
      }
    ]
  },
  "abstract": {
    "text": "....",
    "entities": [
      {
        "raw_entity": "....",
        "role": "Object of Study",
        "osm_entity": "....",
        "osm_link": "....",
        "osm_id": "....",
        "place_id": "...."
      }
    ]
  },
  "fulltext": [
    {
      "section_num": "....",
      "section_name": "....",
      "text": "....",
      "entities": [
        {
          "raw_entity": "....",
          "role": "Other",
          "osm_entity": "....",
          "osm_link": "....",
          "osm_id": "....",
          "place_id": "...."
        }
      ]
    }
  ]
}
```
**Notes & decisions**

* We are keeping both `osm_id` and `place_id` for each geographic entity, to gather the shapes.
* Geotagging form textual content (`geordie`): All geographic mentions are identified across the whole document—not just the object of study. We used these two types:
  * **Object of Study** for the actual study target, locations of research, and target places (captured from title + abstract + methods).
  * **Other** for contextual mentions elsewhere (e.g., literature review). You can safely ignore these if you only care about core study locations.
* Affiliations (`affilgood`): organizations are normalized and geolocated. RORs are split and cleaned; when a ROR isn’t available, I fall back to the raw ORG from NER. The OSM link centers on the reported coordinates.

---

