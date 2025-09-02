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
├── data/ # Example input publications (GROBID-processed XML/TEI or JSON)
├── notebooks/ # Jupyter notebooks with usage examples
├── scripts/ # Helper scripts for batch processing
├── examples/ # Sample outputs (geotagged results)
└── README.md # This file
```

---

## ⚙️ Requirements

- Python 3.9+
- [AffilGood](https://github.com/sirisacademic/affilgood)
- [GEORDIE](https://github.com/sirisacademic/geordie/tree/dev)
- Jupyter (for running notebooks)

Install dependencies:

```bash
pip install -r requirements.txt
```
---
## 🚀 Usage

### 1. Process publications with GROBID
Extract structured metadata and full text:
```bash
# Example: process a batch of PDFs with GROBID
```

### 2. Run AffilGood
```bash
# Example: process a batch of publications
```

### 3. Run GEORDIE
```bash
# Example: process a batch of publications
```

### 4. Combine results and output postprocessing
- case study fixing / grouping / section processing
---
## 📊 Example Output
```json
{
  "publication_id": "12345",
  "title": "Renewable energy integration in Southern Europe",
  "affiliations": [
    {
      "raw": "Department of Energy, University of Athens, Greece",
      "country": "Greece",
      "lat": 37.9838,
      "lon": 23.7275
    }
  ],
  "geographical_mentions": [
    {
      "mention": "Southern Europe",
      "normalized": "Europe, Southern",
      "role": "study_region"
    }
  ]
}
```
---

