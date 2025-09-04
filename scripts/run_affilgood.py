import os
import pandas as pd
from datasets import load_dataset
from tqdm import tqdm
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import time
from affilgood import AffilGood

# ---------------------------------------------------------------------------
# Config
OUT_PARQUET = "../data/energy_planning_affilgood.parquet"
BATCH_SIZE = 20  # how many rows to buffer in memory before flushing
# ---------------------------------------------------------------------------

# 1) Download/load dataset
ds = load_dataset("nicolauduran45/scilake-additional-fulltext-corpus")
df = pd.DataFrame(ds["energy_planning"])

# Or customize components
affil_good = AffilGood(
    span_separator='',  # Use model-based span identification
    span_model_path='SIRIS-Lab/affilgood-span-multilingual',  # Custom span model
    ner_model_path='SIRIS-Lab/affilgood-NER-multilingual',  # Custom NER model
    entity_linkers=['Whoosh'],#, 'DenseLinker'],  # Use multiple linkers
    return_scores=True,  # Return confidence scores with predictions
    metadata_normalization=True,  # Enable location normalization
    verbose=False,  # Detailed logging
    device='cpu'  # Auto-detect device (CPU or CUDA)
)

from tqdm import tqdm

def process_with_progress(x):
    time.sleep(0.1)
    return affil_good.process(x) if len(x) > 0 else []

# Enable progress bar for pandas apply
tqdm.pandas()
df['affilgood'] = df['raw_affiliations'].progress_apply(process_with_progress)

OUT_PARQUET = '../data/energy_planning_affilgood.pkl'
df.to_pickle(OUT_PARQUET)
