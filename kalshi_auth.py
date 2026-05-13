import os
import base64
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_API_KEY     = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY = os.getenv("KALSHI_PRIVATE_KEY", "")
KALSHI_BASE_URL    = "https://trading-api.kalshi.com/trade-api/v2"

def load_private_key():
    if not KALSHI_PRIVATE_KEY:
        print("[WARN] KALSHI_PRIVATE_KEY not set")
        return None
    try:
        key_str = KALSHI_PRIVATE_KEY.strip()
        key_str = key_str.replace("\\n", "\n")

        if "RSA PRIVATE KEY" in key_str:
            header = "-----BEGIN RSA PRIVATE KEY-----"
            footer = "-----END RSA PRIVATE KEY-----"
        else:
            header = "-----BEGIN PRIVATE KEY-----"
            footer = "-----END PRIVATE KEY-----"

        if "\n" not in key_str:
            body = key_str.replace(header, "").replace(footer, "").strip()
            body = body.replace(" ", "")
            body_lines = [body[i:i+64] for i in range(0, len(body), 64)]
            key_str = header + "\n" + "\n".join(body_lines) + "\n" + footer + "\n"

        private_key = serialization.load_pem_private_key(
            key_str.encode("utf-8"),
            password=None
        )
        print("[INFO] Kalshi private key loaded successfully")
        return private_key
    except Exception as e:
        print(f"[WARN] Failed to load private key: {e}")
        return None

def get_auth_headers(method, path):
    private_key = load_private_key()
    if not private_key:
        print("[WARN] Auth failed — check KALSHI_API_KEY and KALSHI_PRIVATE_KEY")
        return {"Content-Type": "application/json"}

    timestamp_ms  = str(int(time.time() * 1000))
    path_no_query = path.split("?")[0]
    message       = timestamp_ms + method.upper() + path_no_query

    try:
        signature = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH  # Kalshi requires DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        return {
            "KALSHI-ACCESS-KEY":       KALSHI_API_KEY,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
            "Content-Type":            "application/json",
        }
    except Exception as e:
        print(f"[WARN] Failed to sign request: {e}")
        return {"Content-Type": "application/json"}
