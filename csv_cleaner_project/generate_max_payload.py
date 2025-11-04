import pandas as pd
import numpy as np
import random
import string
from datetime import datetime, timedelta

# === CONFIG ===
ROWS = 500_000             # realistic heavy test
COLS = 50
CHUNK_SIZE = 50_000        # rows per write chunk
OUTFILE = "stress_test_realistic.csv"

DUP_RATE = 0.02            # 2% duplicates
NULL_RATE = 0.02           # 2% missing values
TYPO_RATE = 0.01           # 1% typos
OUTLIER_RATE = 0.001       # 0.1% extreme values

# === HELPERS ===
def rand_date():
    base = datetime(2020, 1, 1)
    d = base + timedelta(days=random.randint(0, 2000))
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d %b %Y"]
    return d.strftime(random.choice(formats))

def rand_text():
    pools = [
        "John Smith", "María López", "علی رضایی", "李伟",
        "Anaïs Dupont", "Hans Müller", "😀 Happy Guy", "Data Cruncher"
    ]
    val = random.choice(pools)
    if random.random() < TYPO_RATE:
        val = val.replace("a", "aaa") if "a" in val else val + "111"
    return val

def rand_numeric():
    if random.random() < OUTLIER_RATE:
        return round(random.uniform(1e5, 1e6), 2)
    val = random.uniform(0, 1000)
    if random.random() < 0.01:
        val = -val
    return round(val, 2)

def rand_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

# === STREAMING GENERATION ===
header_written = False
rows_remaining = ROWS

while rows_remaining > 0:
    current_size = min(CHUNK_SIZE, rows_remaining)
    data = {}

    for c in range(COLS):
        col_type = c % 5
        if col_type == 0:
            data[f"NumCol_{c}"] = [rand_numeric() for _ in range(current_size)]
        elif col_type == 1:
            data[f"DateCol_{c}"] = [rand_date() for _ in range(current_size)]
        elif col_type == 2:
            data[f"TextCol_{c}"] = [rand_text() for _ in range(current_size)]
        elif col_type == 3:
            data[f"IDCol_{c}"] = [rand_id() for _ in range(current_size)]
        else:
            messy = [f"  {rand_text()}  \n" for _ in range(current_size)]
            data[f"MessyCol_{c}"] = messy

    df_chunk = pd.DataFrame(data)

    # Inject nulls
    mask = np.random.rand(*df_chunk.shape) < NULL_RATE
    df_chunk = df_chunk.mask(mask)

    # Write chunk
    df_chunk.to_csv(
        OUTFILE,
        mode='a',
        index=False,
        header=not header_written,
        encoding="utf-8"
    )

    header_written = True
    rows_remaining -= current_size
    print(f"Chunk written: {current_size} rows, {rows_remaining} remaining")

# Inject duplicates AFTER generation
df = pd.read_csv(OUTFILE)
dupes = df.sample(frac=DUP_RATE)
df = pd.concat([df, dupes], ignore_index=True)
df = df.sample(frac=1).reset_index(drop=True)
df.to_csv(OUTFILE, index=False, encoding="utf-8")
print(f"🚀 Final file ready: {OUTFILE} with shape {df.shape}")
