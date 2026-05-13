import os
import base64
import time
from datetime import datetime, timezone
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_API_KEY     = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY", "")
KALSHI_BASE_URL    = "https://trading-api.kalshi.com/trade-api/v2"

def load_private_key():
    if not KALSHI_PRIVATE_KEY:
        return None
    try:
        key_data = KALSHI_PRIVATE_KEY.encode("utf-8")
        private_key = serialization.load_pem_private_key(key_data, password=None)
        return private_key
    except Exception as e:
        print(f"[WARN] Failed to load private key: {e}")
        return None

def sign_request(method, path):
    private_key = load_private_key()
    if not private_key:
        return None

    timestamp_ms = str(int(time.time() * 1000))
    path_no_query = path.split("?")[0]
    message = timestamp_ms + method.upper() + path_no_query

    try:
        signature = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        sig_b64 = base64.b64encode(signature).decode("utf-8")
        return {
            "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "Content-Type":            "application/json",
        }
    except Exception as e:
        print(f"[WARN] Failed to sign request: {e}")
        return None

def get_auth_headers(method, path):
    headers = sign_request(method, path)
    if not headers:
        print("[WARN] Could not generate auth headers — check KALSHI_API_KEY and KALSHI_PRIVATE_KEY")
        return {"Content-Type": "application/json"}
    return headers
