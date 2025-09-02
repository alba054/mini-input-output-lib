from loguru import logger
from base.mapper import BaseMapper
from core.context import Context


class OuterMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            ctx.set("parent_id", data.get("checksum", dict()).get("stitched_image_md5"))
            data["output_tiff"] = data["output_image"].replace("res-stitched", "tiff/geotiff").replace(".jpg", ".tif")
            mapped = {
                "id": data.get("checksum", dict()).get("stitched_image_md5"),
                "level": data.get("level"),
                "country_name": data.get("country_name"),
                "country_code": data.get("country_code"),
                "province_name": data.get("province_name"),
                "province_code": data.get("province_code"),
                "city_name": data.get("city_name"),
                "city_code": data.get("city_code"),
                "district_name": data.get("district_name"),
                "district_code": data.get("district_code"),
                "subdistrict_name": data.get("subdistrict_name"),
                "subdistrict_code": data.get("subdistrict_code"),
                "name": data.get("name"),
                "zoom": data.get("zoom"),
                "tile_width": data.get("tile_width"),
                "tile_height": data.get("tile_height"),
                "x_range": data.get("x_range"),
                "y_range": data.get("y_range"),
                "grid_width": data.get("grid_width"),
                "grid_height": data.get("grid_height"),
                "image_size": data.get("image_size"),
                "tiles_downloaded": data.get("tiles_downloaded"),
                "tile_source_template": data.get("tile_source_template"),
                "bounds": data.get("bounds"),
                "output_image": data.get("output_image"),
                "output_tiff": data.get("output_tiff"),
            }
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_outer_mapper(include_in_field: str) -> OuterMapper:
    return OuterMapper(include_in_field)
