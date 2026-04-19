"""
Lambda 4: Results Writer
------------------------
Writes the complete analysis results to:
  1. DynamoDB  — one item per video, containing all analysis data
  2. S3        — a clean JSON summary file at results/{video_id}/analysis.json

DynamoDB table schema:
  Partition key: video_key (String)   — e.g. "uploads/my-video.mp4"
  Attributes:
    video_id           (S)
    bucket             (S)
    processed_at       (S)  — ISO-8601 timestamp
    duration_seconds   (N)
    num_frames         (N)
    frame_analyses     (L)  — list of { frame_index, description, embedding, ... }
    marengo_embedding  (L)  — video-level embedding vector (may be NULL)
    summary            (S)  — auto-generated one-paragraph summary (from Nova)

Environment variables:
  DYNAMODB_TABLE   - DynamoDB table name
  RESULTS_BUCKET   - S3 bucket for result JSON files (can be same as video bucket)
  AWS_REGION       - AWS region
"""

import json
import os
import decimal
import datetime
import boto3
import logging
from boto3.dynamodb.types import TypeSerializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "us-east-1"))
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "video-analysis-results")
RESULTS_BUCKET = os.environ.get("RESULTS_BUCKET")
NOVA_MODEL_ID = os.environ.get("NOVA_MODEL_ID", "amazon.nova-pro-v1:0")


def lambda_handler(event, context):
    """
    Expected event: full state from previous Lambdas, merged by Step Functions.
    Contains: bucket, key, video_id, frame_analyses, marengo_embedding, etc.
    """
    logger.info(f"Writing results for video_id: {event.get('video_id')}")

    video_id = event["video_id"]
    bucket = event["bucket"]
    key = event["key"]
    results_bucket = RESULTS_BUCKET or bucket

    # Generate a human-readable summary using Nova
    summary = generate_summary(event.get("frame_analyses", []))
    logger.info(f"Summary: {summary[:150]}...")

    # Prepare the DynamoDB item
    processed_at = datetime.datetime.utcnow().isoformat() + "Z"
    item = {
        "video_key": key,                        # Partition key
        "video_id": video_id,
        "bucket": bucket,
        "processed_at": processed_at,
        "duration_seconds": decimal.Decimal(str(event.get("duration_seconds", 0))),
        "num_frames": event.get("num_frames", 0),
        "summary": summary,
        "frame_analyses": sanitize_for_dynamodb(event.get("frame_analyses", [])),
        "marengo_embedding": sanitize_for_dynamodb(event.get("marengo_embedding")),
        "marengo_model": event.get("marengo_model", ""),
    }

    # Write to DynamoDB
    table = dynamodb.Table(DYNAMODB_TABLE)
    table.put_item(Item=item)
    logger.info(f"Saved to DynamoDB table '{DYNAMODB_TABLE}', key='{key}'")

    # Write JSON summary to S3
    result_key = f"results/{video_id}/analysis.json"
    summary_doc = {
        "video_key": key,
        "video_id": video_id,
        "processed_at": processed_at,
        "duration_seconds": event.get("duration_seconds"),
        "num_frames": event.get("num_frames"),
        "summary": summary,
        "frames": [
            {
                "frame_index": fa["frame_index"],
                "frame_key": fa["frame_key"],
                "description": fa["description"],
                # Omit embeddings from the readable summary to keep it small
            }
            for fa in event.get("frame_analyses", [])
        ],
    }

    s3.put_object(
        Bucket=results_bucket,
        Key=result_key,
        Body=json.dumps(summary_doc, indent=2),
        ContentType="application/json",
    )
    logger.info(f"Saved summary to s3://{results_bucket}/{result_key}")

    return {
        "status": "SUCCESS",
        "video_id": video_id,
        "dynamodb_table": DYNAMODB_TABLE,
        "result_s3_key": result_key,
        "summary": summary,
        "processed_at": processed_at,
    }


def generate_summary(frame_analyses: list) -> str:
    """Ask Nova Pro to synthesize a one-paragraph summary from all frame descriptions."""
    if not frame_analyses:
        return "No frames were analyzed."

    descriptions = "\n".join(
        f"Frame {fa['frame_index'] + 1}: {fa['description']}"
        for fa in frame_analyses
    )

    prompt = (
        "Below are descriptions of evenly spaced frames from a video.\n"
        "Write a single cohesive paragraph (4-6 sentences) summarizing "
        "the overall content, narrative, and mood of the video.\n\n"
        f"{descriptions}"
    )

    body = {
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"maxTokens": 300, "temperature": 0.4},
    }

    response = bedrock.invoke_model(
        modelId=NOVA_MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    result = json.loads(response["body"].read())
    return result["output"]["message"]["content"][0]["text"]


def sanitize_for_dynamodb(value):
    """
    DynamoDB does not support float values — convert them to Decimal.
    Also handles nested lists and dicts.
    """
    if value is None:
        return None
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, list):
        return [sanitize_for_dynamodb(v) for v in value]
    if isinstance(value, dict):
        return {k: sanitize_for_dynamodb(v) for k, v in value.items()}
    return value
