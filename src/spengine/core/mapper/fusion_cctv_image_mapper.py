from loguru import logger
from base.mapper import BaseMapper
from core.context import Context


class FusionCCTVImageMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            mapped = {
                "filename": f'test-intel-tactical-detection-raw/{data.get("stream_name")}_{data.get("timestamp_utc")}.jpg',
                "encoding": "base64",
                "data": data.get("image_base64"),
            }
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_cctv_image_mapper(include_in_field: str) -> FusionCCTVImageMapper:
    return FusionCCTVImageMapper(include_in_field)
