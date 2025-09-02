from loguru import logger
import requests
from base.mapper import BaseMapper
from core.context import Context
from helper.helper import generate_id


class AtmMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        lat = data.get("latitude")
        lon = data.get("longitude")
        address = data.get("alamat_lengkap")
        coordinate = None

        resp = None
        pc = None
        pn = None
        cc = None
        cn = None
        dc = None
        dn = None
        sc = None
        sn = None

        if lat is not None and lon is not None:
            resp = requests.get(
                f"http://192.168.107.91:8939/v2/coordinates/single/kemendagri/2024/1?latitude={lat}&longitude={lon}"
            )
            if resp.status_code == 200:
                resp = resp.json()
                pc = resp.get("province_code")
                pn = resp.get("province_name")
                cc = resp.get("city_code")
                cn = resp.get("city_name")
                dc = resp.get("district_code")
                dn = resp.get("district_name")
                sc = resp.get("subdistrict_code")
                sn = resp.get("subdistrict_name")

            coordinate = f"POINT({lon} {lat})"

        else:
            resp = requests.get(f"http://192.168.107.91:8939/nominatim?key={address}")
            if resp.status_code == 200:
                resp = resp.json()
                try:
                    if resp is not None:
                        pc = resp.get("result", dict()).get("province_code")
                        pn = resp.get("result", dict()).get("province_name")
                        cc = resp.get("result", dict()).get("city_code")
                        cn = resp.get("result", dict()).get("city_name")
                        dc = resp.get("result", dict()).get("district_code")
                        dn = resp.get("result", dict()).get("district_name")
                        sc = resp.get("result", dict()).get("subdistrict_code")
                        sn = resp.get("result", dict()).get("subdistrict_name")
                except:
                    pass

        try:
            mapped = {
                "link": data.get("link"),
                "judul": data.get("judul"),
                "bank": data.get("bank"),
                "provinsi": data.get("provinsi"),
                "kabupaten_kota": data.get("kabupaten_kota"),
                "kode_atm": data.get("kode_atm"),
                "kategori": data.get("kategori"),
                "lokasi_kantor_cabang": data.get("lokasi_kantor_cabang"),
                "area": data.get("area"),
                "kanwil": data.get("kanwil"),
                "alamat_lengkap": address,
                "jenis_layanan": data.get("jenis_layanan"),
                "nomor_telepon": data.get("nomor_telepon"),
                "fax": data.get("fax"),
                "link_google_maps": data.get("link_google_maps"),
                "latitude": lat,
                "longitude": lon,
                "coordinate": coordinate,
                "province_code": pc,
                "province_name": pn,
                "city_code": cc,
                "city_name": cn,
                "district_code": dc,
                "district_name": dn,
                "subdistrict_code": sc,
                "subdistrict_name": sn,
            }
            mapped["id"] = generate_id(mapped)
            return mapped
        except Exception as e:
            logger.error(e)
            return None


def create_atm_mapper(include_in_field: str) -> AtmMapper:
    return AtmMapper(include_in_field)
