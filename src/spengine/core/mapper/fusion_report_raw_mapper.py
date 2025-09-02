import jmespath
from loguru import logger
import requests
from base.mapper import BaseMapper
from core.context import Context
from helper.helper import generate_id, generate_id_str


class FusionReportLocationMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:

        parameters = jmespath.search("fusion_parameters || parameters || coordinate", data)
        location_zoom = None
        coordinate = None
        resp = None

        if parameters is not None:
            if isinstance(parameters, dict):
                location_zoom = parameters.get("zoom")
                coordinate = parameters.get("center")
                resp = requests.get(
                    f"http://192.168.107.91:8939/v2/coordinates/single/kemendagri/2024/1?latitude={coordinate[1]}&longitude={coordinate[0]}"
                )
            elif isinstance(parameters, list):
                coordinate = parameters
                resp = requests.get(
                    f"http://192.168.107.91:8939/v2/coordinates/single/kemendagri/2024/1?latitude={coordinate[1]}&longitude={coordinate[0]}"
                )

        cc = None
        cn = None
        pc = None
        pn = None
        cic = None
        cin = None
        dc = None
        dn = None
        sc = None
        sn = None

        if resp is not None and resp.status_code is not None and resp.status_code == 200:
            resp = resp.json()
            dn = resp.get("district_name")
            cin = resp.get("city_name")
            dc = resp.get("district_code")
            sn = resp.get("subdistrict_name")
            sc = resp.get("subdistrict_code")
            cic = resp.get("city_code")
            pc = resp.get("province_code")
            pn = resp.get("province_name")
            cc = resp.get("country_code3")
            cn = resp.get("country_name")

        location_center = f"POINT({coordinate[0]} {coordinate[1]})" if coordinate else None

        try:
            mapped = {
                "country_name": cn,
                "country_code": cc,
                "province_code": pc,
                "province_name": pn,
                "district_code": dc,
                "district_name": dn,
                "city_name": cin,
                "city_code": cic,
                "subdistrict_name": sn,
                "subdistrict_code": sc,
            }
            mapped["id"] = generate_id(mapped)
            ctx.set("location_id", mapped["id"])
            ctx.set("location_zoom", location_zoom)
            ctx.set("location_center", location_center)
            ctx.set("coordinate", coordinate)
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_fusion_report_location_mapper(include_in_field: str) -> FusionReportLocationMapper:
    return FusionReportLocationMapper(include_in_field)


class FusionReportMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            location_zoom = ctx.get("location_zoom")
            location_id = ctx.get("location_id")
            location_center = ctx.get("location_center")
            images = jmespath.search("images", data)

            if images is not None and isinstance(images, str):
                images = images.replace("[", "").replace("]", "").split(",")
            mapped = {
                "action": jmespath.search("fusion_action || action", data),
                "feature": jmespath.search("fusion_feature || feature", data),
                "sketch": jmespath.search("sketch", data),
                "input": jmespath.search("input", data),
                "raw_message": jmespath.search("raw_message", data),
                "location_zoom": location_zoom,
                "location_id": location_id,
                "location_center": location_center,
                "images": images,
                "first_name": jmespath.search("first_name", data),
                "username": jmespath.search("username", data),
                "created_at": jmespath.search("created_at", data),
                "id_case": jmespath.search("id_case", data),
            }
            mapped["id"] = generate_id_str(jmespath.search("input", data))
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_fusion_report_mapper(include_in_field: str) -> FusionReportMapper:
    return FusionReportMapper(include_in_field)
