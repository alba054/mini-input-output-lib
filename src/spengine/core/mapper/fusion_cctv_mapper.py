import jmespath
from loguru import logger
from base.mapper import BaseMapper
from core.context import Context
from helper.helper import generate_id_str

location_id = "57b778871af892f6a4b3dabf735ca60e"

locations = {
    "outer gate": [-6.267942587610273, 106.68727438576163],
    "jam lobby": [-6.2680918934026835, 106.68706986732231],
    "parking cam": [-6.267957251573916, 106.68703432805255],
    "inner gate": [-6.268035903735537, 106.6871912372814],
}


class FusionCCTVMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            loc_id = None
            raw_coordinate = None
            for k, v in locations.items():
                if (
                    jmespath.search("stream_name", data) is not None
                    and k in jmespath.search("stream_name", data).lower()
                ):
                    loc_id = location_id
                    raw_coordinate = f"POINT({v[1] } {v[0]})"
                    break

            mapped = {
                "file_url": jmespath.search("file_url", data),
                "file_size": jmespath.search("file_size", data),
                "raw_analysis": jmespath.search("raw_analysis", data),
                "matched_elements": jmespath.search("analysis_result.matched_elements", data),
                "unmatched_elements": jmespath.search("analysis_result.unmatched_elements", data),
                "image_threat_confidence": jmespath.search("analysis_result.image_threat_confidence", data),
                "threat_description": jmespath.search("analysis_result.threat_description", data),
                "remarks": jmespath.search("analysis_result.remarks", data),
                "stream_name": jmespath.search("stream_name", data),
                "class": jmespath.search("detection.class", data),
                "confidence": jmespath.search("detection.confidence", data),
                "timestamp_utc": jmespath.search("timestamp_utc", data),
                "date": data.get(""),
                "location_id": loc_id,
                "raw_coordinate": raw_coordinate,
                "id_case": data.get("id_case"),
            }
            mapped["id"] = generate_id_str(data["file_url"])
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_fusion_cctv_mapper(include_in_field: str) -> FusionCCTVMapper:
    return FusionCCTVMapper(include_in_field)
