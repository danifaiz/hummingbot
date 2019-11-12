import time
import hmac
import hashlib
import base64
import json
from typing import Dict, List


class DraniteAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    def generate_auth_dict(
        self,
        url: str,
    ) -> Dict[str, any]:
        """
        Generates the payload and signature for a submitted url.
        """

        def generate_hash(uri: str) -> List[str, str]:
            timestamp = str(int(time.time() * 1000))
            _payload = {}
            _payload['request'] = uri
            _payload['nonce'] = timestamp
            _payloadString = json.dumps(_payload)

            # Standard Base64 Encoding
            encodedBytes = base64.b64encode(_payloadString.encode("utf-8"))
            requestPayload = str(encodedBytes, "utf-8")
            # Convert String using Secret Key to Hash using SHA384
            digest = hmac.new(self.secret_key.encode("utf-8"), msg=encodedBytes, digestmod=hashlib.sha384).digest()
            signature = base64.b64encode(digest).decode()
            return [signature, requestPayload]

        result = generate_hash(url)
        signature = result[0]
        requestPayload = result[1]
        # timestamp = str(time.time())
        # V1 Authentication headers
        headers = {
            "X-DR-APIKEY": self.api_key,
            "X-DR-PAYLOAD": requestPayload,
            "X-DR-SIGNATURE": signature,
            # "Content-Type": "application/json",
            # "Accept": "application/json",
        }
        return headers
