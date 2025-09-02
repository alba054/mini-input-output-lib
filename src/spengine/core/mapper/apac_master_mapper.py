from loguru import logger
import requests
from base.mapper import BaseMapper
from core.context import Context
from helper.helper import generate_id


class ApacMasterMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        country = data.get("country") if data.get("country") != "Null" else None
        resp = requests.get(f"http://192.168.107.91:8939/other/percolator/countries?query={country}&mode=detail")
        cc = None
        cn = None
        coc = None
        con = None

        if resp.status_code == 200:
            resp = resp.json()
            cc = resp.get("country_code3")
            cn = resp.get("country_name")
            coc = resp.get("continent_code")
            con = resp.get("continent_name")

        coordinate = (
            f'POINT({data.get("cordinates")[1]} {data.get("cordinates")[0]})'
            if data.get("cordinates") != "Null"
            else None
        )

        try:
            mapped = {
                "date": str(data.get("date")) if str(data.get("date")) != "Null" else None,
                "event_title": data.get("event_title") if data.get("event_title") != "Null" else None,
                "place": data.get("place") if data.get("place") != "Null" else None,
                "location": data.get("location") if data.get("location") != "Null" else None,
                "fatalities": data.get("fatalities") if data.get("fatalities") != "Null" else None,
                "injuries": data.get("injuries") if data.get("injuries") != "Null" else None,
                "source_of_information": (
                    data.get("source_of_information") if data.get("source_of_information") != "Null" else None
                ),
                "language_of_input": data.get("language_of_input") if data.get("language_of_input") != "Null" else None,
                "reliability_of_source": (
                    data.get("reliability_of_source") if data.get("reliability_of_source") != "Null" else None
                ),
                "credibility_of_information": (
                    data.get("credibility_of_information") if data.get("credibility_of_information") != "Null" else None
                ),
                "event_type": data.get("event_type") if data.get("event_type") != "Null" else None,
                "detailed_description_of_event": (
                    data.get("detailed_description_of_event")
                    if data.get("detailed_description_of_event") != "Null"
                    else None
                ),
                "region": data.get("region") if data.get("region") != "Null" else None,
                "sub_region": data.get("sub_region") if data.get("sub_region") != "Null" else None,
                "country": country,
                "cordinates": coordinate,
                "country_code": cc,
                "country_name": cn,
                "continent_code": coc,
                "continent_name": con,
            }
            mapped["id"] = generate_id(mapped)
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_apac_master_mapper(include_in_field: str) -> ApacMasterMapper:
    return ApacMasterMapper(include_in_field)
