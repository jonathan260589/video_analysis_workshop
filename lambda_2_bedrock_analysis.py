"""
Lambda 2: Bedrock Analyzer
--------------------------
Reads extracted frames from S3, then for each frame:
  1. Calls Amazon Nova Pro (multimodal) to generate a text description
  2. Calls Amazon Titan Multimodal Embeddings to generate a vector embedding

Environment variables:
  AWS_REGION           - AWS region (auto-set by Lambda)
  NOVA_MODEL_ID        - Amazon Nova model ID (default: amazon.nova-pro-v1:0)
  EMBEDDINGS_MODEL_ID  - Titan embeddings model ID (default: amazon.titan-embed-image-v1)
"""

import json
import os
import base64
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "us-east-1"))

NOVA_MODEL_ID = os.environ.get("NOVA_MODEL_ID", "amazon.nova-pro-v1:0")
EMBEDDINGS_MODEL_ID = os.environ.get("EMBEDDINGS_MODEL_ID", "amazon.titan-embed-image-v1")

NOVA_PROMPT = (
    "You are analyzing a video frame. "
    "Describe what you see in detail: the scene, objects, people (if any), "
    "actions, lighting, and mood. Be concise but thorough (3-5 sentences)."
)


def lambda_handler(event, context):
    """
    Expected event shape (output of Frame Extractor Lambda):
    {
        "bucket": "my-video-bucket",
        "key": "uploads/my-video.mp4",
        "video_id": "abc123",
        "frames_bucket": "my-video-bucket",
        "frame_keys": ["frames/abc123/frame_000.jpg", ...],
        "num_frames": 5,
        "duration_seconds": 42.5
    }
    """
    logger.info(f"Event: {json.dumps(event)}")

    frames_bucket = event["frames_bucket"]
    frame_keys = event["frame_keys"]
    video_id = event["video_id"]

    frame_analyses = []

    for i, frame_key in enumerate(frame_keys):
        logger.info(f"Analyzing frame {i+1}/{len(frame_keys)}: {frame_key}")

        # Download frame bytes
        response = s3.get_object(Bucket=frames_bucket, Key=frame_key)
        image_bytes = response["Body"].read()
        image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

        # Call Nova Pro for description
        description = describe_frame_with_nova(image_b64)
        logger.info(f"Nova description: {description[:100]}...")

        # Call Titan for embedding
        embedding = embed_frame_with_titan(image_b64)
        logger.info(f"Titan embedding dimensions: {len(embedding)}")

        frame_analyses.append({
            "frame_index": i,
            "frame_key": frame_key,
            "description": description,
            "embedding": embedding,      # 1024-dim vector
            "embedding_model": EMBEDDINGS_MODEL_ID,
        })

    return {
        **event,
        "frame_analyses": frame_analyses,
    }


def describe_frame_with_nova(image_b64: str) -> str:
    """Call Amazon Nova Pro multimodal model to describe a frame."""
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "image": {
                            "format": "jpeg",
                            "source": {
                                "bytes": image_b64,  # Nova accepts base64 inline
                            },
                        }
                    },
                    {
                        "text": NOVA_PROMPT
                    },
                ],
            }
        ],
        "inferenceConfig": {
            "maxTokens": 512,
            "temperature": 0.3,
        },
    }

    response = bedrock.invoke_model(
        modelId=NOVA_MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )

    result = json.loads(response["body"].read())
    return result["output"]["message"]["content"][0]["text"]


def embed_frame_with_titan(image_b64: str) -> list[float]:
    """Call Amazon Titan Multimodal Embeddings to get a vector for a frame."""
    body = {
        "inputImage": image_b64,
        "embeddingConfig": {
            "outputEmbeddingLength": 1024,  # Options: 256, 384, 1024
        },
    }

    response = bedrock.invoke_model(
        modelId=EMBEDDINGS_MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )

    result = json.loads(response["body"].read())
    return result["embedding"]  # List of 1024 floats
