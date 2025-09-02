import json
import time
import jmespath
from loguru import logger
import requests
from base.mapper import BaseMapper
from core.context import Context


class ReviewSentimentEnrichmentMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        logger.info(data.get("review_id"))
        start = time.time()
        resp = requests.post("http://10.12.1.148:9908/analyze", json=data)
        logger.info(f"enrichment takes {time.time() - start} sec(s)")

        if resp.status_code != 200:
            logger.error(resp.text)
            raise

        try:
            jsonify_resp = resp.json()

            ctx.set("reviews", jmespath.search("result.review", jsonify_resp))
            ctx.set("review_id", data.get("review_id"))
            ctx.set("raw", jsonify_resp)
            return {
                "ann_emotion": jmespath.search("result.ann_emotion", jsonify_resp),
                "emotion_reasoning": jmespath.search("result.emotion_reasoning", jsonify_resp),
                "ann_complaint_type": jmespath.search("result.ann_complaint_type", jsonify_resp),
                "complaint_type_reasoning": jmespath.search("result.complaint_type_reasoning", jsonify_resp),
                "score": jmespath.search("result.score", jsonify_resp),
                "score_reasoning": jmespath.search("result.score_reasoning", jsonify_resp),
                "sentiment": jmespath.search("result.sentiment", jsonify_resp),
                "sentiment_reasoning": jmespath.search("result.sentiment_exp", jsonify_resp),
                "image_verification": None,
                "elapsed_time": jmespath.search("elapsed_time", jsonify_resp),
                "specific_unit": jmespath.search("result.specific_unit", jsonify_resp),
                "recommendations": jmespath.search("result.recommendations", jsonify_resp),
                "review_id": data.get("review_id"),
                "raw_comment": data.get("comment"),
            }
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(e)
            raise


def create_review_sentiment_enrichment_mapper(include_in_field: str) -> ReviewSentimentEnrichmentMapper:
    return ReviewSentimentEnrichmentMapper(include_in_field)


class ReviewSentimentCategoryMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            if ctx.get("review_id") is None:
                return None
            mapped = []
            reviews = ctx.get("reviews") or []

            for review in reviews:
                mapped.append(
                    {
                        "review_id": ctx.get("review_id"),
                        "category": review.get("category"),
                        "score": review.get("score"),
                        "reason": review.get("reason"),
                    }
                )

            return mapped
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(e)
            return None


def create_review_sentiment_category_mapper(include_in_field: str) -> ReviewSentimentCategoryMapper:
    return ReviewSentimentCategoryMapper(include_in_field)


class ReviewSentimentRawMapper(BaseMapper):
    def map(self, data: dict, ctx: Context) -> list[dict] | dict:
        try:
            resp = ctx.get("raw")
            if resp is None:
                return None
            return {**resp, "review_id": ctx.get("review_id")}
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(e)
            return None


def create_review_sentiment_raw_mapper(include_in_field: str) -> ReviewSentimentRawMapper:
    return ReviewSentimentRawMapper(include_in_field)
