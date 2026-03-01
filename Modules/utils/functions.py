import base64


def bytes_to_base64(bytes_data):
    return base64.b64encode(bytes_data).decode("utf-8")


def base64_to_bytes(base64_data):
    return base64.b64decode(base64_data)
