from dataclasses import dataclass, asdict
import hashlib
import json
from datetime import datetime
import jmespath
import requests

from typing import List, Union, Optional
import logging
import random


def generate_id(data: dict):
    json_data = json.dumps(data, sort_keys=True)
    md5_hash = hashlib.md5(json_data.encode("utf-8")).hexdigest()
    return md5_hash


def generate_id_str(data: str):
    md5_hash = hashlib.md5(data.encode("utf-8")).hexdigest()
    return md5_hash


def CommonDatetimeLayout() -> str:
    return "%Y-%m-%d %H:%M:%S"


def to_datetime(raw: str | int | None, epoch_unit: str = "ms") -> str | None:
    if raw is None:
        return None

    if isinstance(raw, str):
        return raw

    elif isinstance(raw, int):
        if epoch_unit == "ms":
            epoch = raw // 1000
        elif epoch_unit == "s":
            epoch = raw
        else:
            raise ValueError("epoch unit is not supported (ms or s)")

        return datetime.fromtimestamp(epoch).strftime(CommonDatetimeLayout())

    return None


@dataclass
class CommonLocationModel:
    country_code: str | None
    country_name: str | None
    province_code: str | None
    province_name: str | None
    city_code: str | None
    city_name: str | None
    district_code: str | None
    district_name: str | None
    subdistrict_code: str | None
    subdistrict_name: str | None
    name: str | None
    lat: float | None
    lon: float | None


# fetch from nominatim or check catalogue
def nominatim(location_raw: str):
    if location_raw is None:
        return None

    url = f"http://192.168.107.91:8939/nominatim?key={location_raw}"

    resp = requests.get(url)

    if resp is None:
        return None

    if resp.status_code != 200:
        return None

    resp_json = resp.json()

    if resp_json is None:
        return None

    return asdict(
        CommonLocationModel(
            **{
                "country_code": jmespath.search("result.country_code3", resp_json),
                "country_name": jmespath.search("result.country_name", resp_json),
                "province_code": jmespath.search("result.province_code", resp_json),
                "province_name": jmespath.search("result.province_name", resp_json),
                "city_code": jmespath.search("result.city_code", resp_json),
                "city_name": jmespath.search("result.city_name", resp_json),
                "district_code": jmespath.search("result.district_code", resp_json),
                "district_name": jmespath.search("result.district_name", resp_json),
                "subdistrict_code": jmespath.search("result.subdistrict_code", resp_json),
                "subdistrict_name": jmespath.search("result.subdistrict_name", resp_json),
                "name": location_raw,
                "lat": jmespath.search("raw.coordinate.lat", resp_json),
                "lon": jmespath.search("raw.coordinate.long", resp_json),
            }
        )
    )


def generate_random_number(digit: int) -> int:
    return int("".join(str(random.randint(0, 9)) for _ in range(digit)))
