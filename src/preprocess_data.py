# src/preprocess_data.py
from pathlib import Path
from datetime import datetime, timedelta
import random                   # for reproducible sampling/sharing
import pandas as pd             # data handling
from hashlib import blake2b     # for hashing IDs (privacy, Chapter 3)

# ---------- settings you can tweak ----------
RAW_FILENAME      = "Dataset_Momo.csv"
TAKE_SAMPLE       = True
SAMPLE_ROWS       = 100_000
START_DATE        = datetime(2025, 1, 1)

SHARE_RATE        = 0.24     # 24% of senders who share devices
MIN_GROUP         = 2
MAX_GROUP         = 6
MERCHANT_RATE     = 0.16     # ~16% of receivers become merchants
LOCATION_COUNT    = 50       # number of locations to assign users to
PHONE_SHARE_RATE  = 0.30     # ~30% of users share phones/emails
RANDOM_SEED       = 28

# ---------- paths (script is in src/, project root is one level up) ----------
ROOT     = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / RAW_FILENAME
OUT_PATH = ROOT / "data" / "raw" / "sampled_momo_transactions.csv"

def h(x: str) -> str:
    """Short, deterministic hash for anonymizing IDs (privacy, Chapter 3)."""
    return blake2b(str(x).encode(), digest_size=12).hexdigest()

def read_raw_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Put the file at {path}")
    # `type` included for completeness; not required downstream
    usecols = ["step","type","amount","nameOrig","nameDest","isFraud"]
    print(f"[1/6] Reading {path.name}")
    return pd.read_csv(path, usecols=usecols)

def maybe_sample(df: pd.DataFrame) -> pd.DataFrame:
    if TAKE_SAMPLE and len(df) > SAMPLE_ROWS:
        print(f"[2/6] Sampling to {SAMPLE_ROWS:,} rows (seed={RANDOM_SEED})")
        return df.sample(n=SAMPLE_ROWS, random_state=RANDOM_SEED).reset_index(drop=True)
    print("[2/6] Using full data")
    return df

def build_core_fields(df: pd.DataFrame) -> pd.DataFrame:
    print("[3/6] Building core columns")
    n = len(df)
    out = pd.DataFrame(index=df.index)

    # Raw IDs
    out["sender_id"]      = df["nameOrig"].astype(str)
    out["receiver_id"]    = df["nameDest"].astype(str)

    # Hash IDs for privacy (Chapter 3 commitment)
    out["sender_id"]      = out["sender_id"].map(h)
    out["receiver_id"]    = out["receiver_id"].map(h)

    # Transaction metadata
    out["transaction_id"] = [f"T{i:07d}" for i in range(n)]
    out["amount"]         = pd.to_numeric(df["amount"], errors="coerce")
    out["label"]          = df["isFraud"].astype(int)
    out["timestamp"]      = df["step"].apply(
        lambda hstep: (START_DATE + timedelta(hours=int(hstep))).isoformat()
    )

    # Clean
    out = out.dropna(subset=["amount"])
    out = out[out["amount"] > 0]
    return out

    #  Devices: everyone has one; some share (fraud signal)
def assign_devices(out: pd.DataFrame) -> pd.DataFrame:
    print("[4/6] Assigning devices (with sharing)")
    random.seed(RANDOM_SEED)
    senders = out["sender_id"].unique().tolist()

    # default: unique device per sender
    device_for_sender = {s: f"D{random.randint(100000,999999)}" for s in senders}

    # introduce shared devices among random groups
    shuffled = senders.copy(); random.shuffle(shuffled)
    i = 0
    while i < len(shuffled):
        if random.random() < SHARE_RATE:
            k = random.randint(MIN_GROUP, MAX_GROUP)
            group = shuffled[i:i+k]
            if len(group) < 2:
                break
            shared_id = f"D{random.randint(100000,999999)}"
            for s in group:
                device_for_sender[s] = shared_id
            i += k
        else:
            i += 1

    out["sender_device_id"] = out["sender_id"].map(device_for_sender)
    return out

# Mark some receivers as merchants
def assign_merchants(out: pd.DataFrame) -> pd.DataFrame:
    print("[5/6] Marking some receivers as merchants")
    receivers = out["receiver_id"].unique().tolist()
    random.seed(RANDOM_SEED)
    m = int(len(receivers) * MERCHANT_RATE)
    merchants = set(random.sample(receivers, m if m > 0 else 1))
    out["receiver_type"] = out["receiver_id"].apply(
        lambda r: "merchant" if r in merchants else "user"
    )
    return out

# Locations, phones, emails (some shared)
def assign_locations_and_contacts(out: pd.DataFrame) -> pd.DataFrame:
    print("[6/6] Assigning locations and contacts (with sharing)")
    random.seed(RANDOM_SEED)
    senders = out["sender_id"].unique().tolist()

    # Locations
    locations = [f"L{i:03d}" for i in range(1, LOCATION_COUNT+1)]
    loc_map = {s: random.choice(locations) for s in senders}
    out["sender_location"] = out["sender_id"].map(loc_map)

    # Phones / emails (some shared)
    phone_for_sender = {s: f"P{random.randint(100000,999999)}" for s in senders}
    email_for_sender = {s: f"user{random.randint(1000,9999)}@mail.com" for s in senders}

    shuffled = senders.copy(); random.shuffle(shuffled)
    i = 0
    while i < len(shuffled):
        if random.random() < PHONE_SHARE_RATE:
            k = random.randint(2, 5)
            group = shuffled[i:i+k]
            shared_phone = f"P{random.randint(100000,999999)}"
            shared_email = f"fraud{random.randint(1000,9999)}@mail.com"
            for s in group:
                phone_for_sender[s] = shared_phone
                email_for_sender[s] = shared_email
            i += k
        else:
            i += 1

# Map
    out["sender_phone"] = out["sender_id"].map(phone_for_sender)
    out["sender_email"] = out["sender_id"].map(email_for_sender)
    return out

# --- Main flow ---
def main() -> None:
    df = read_raw_csv(RAW_PATH)
    df = maybe_sample(df)
    out = build_core_fields(df)
    out = assign_devices(out)
    out = assign_merchants(out)
    out = assign_locations_and_contacts(out)

    final = out[[
        "transaction_id","sender_id","receiver_id","receiver_type",
        "amount","timestamp","sender_device_id",
        "sender_location","sender_phone","sender_email",
        "label"
    ]]

# Save
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUT_PATH, index=False)
    print(f"Saved {len(final):,} rows to {OUT_PATH}")

if __name__ == "__main__":
    main()
