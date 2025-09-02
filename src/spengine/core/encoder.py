import base64


class Encoder:
    @staticmethod
    def encode(data: str, encoding: str) -> bytes:
        match (encoding):
            case "base64":
                return Encoder._b64(data)

        return None

    @staticmethod
    def _b64(data: str) -> bytes:
        return base64.b64decode(data)
