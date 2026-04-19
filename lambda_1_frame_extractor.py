"""
Lambda 1: Frame Extractor
-------------------------
Triggered by Step Functions. Downloads a video from S3, extracts N keyframes
using ffmpeg (provided as a Lambda Layer), and saves frames back to S3.

Environment variables:
  FRAMES_BUCKET  - S3 bucket for storing extracted frames (can be same as input bucket)
  NUM_FRAMES     - Number of frames to extract (default: 5)
  FFMPEG_PATH    - Path to ffmpeg binary (default: /opt/bin/ffmpeg from Layer)
"""

import json
import os
import subprocess
import tempfile
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

FFMPEG_PATH = os.environ.get("FFMPEG_PATH", "/opt/bin/ffmpeg")
NUM_FRAMES = int(os.environ.get("NUM_FRAMES", "5"))
FRAMES_BUCKET = os.environ.get("FRAMES_BUCKET")


def lambda_handler(event, context):
    """
    Expected event shape (passed by Step Functions):
    {
        "bucket": "my-video-bucket",
        "key": "uploads/my-video.mp4",
        "video_id": "abc123"   # optional, derived from key if absent
    }
    """
    logger.info(f"Event: {json.dumps(event)}")

    bucket = event["bucket"]
    key = event["key"]
    video_id = event.get("video_id", key.replace("/", "_").replace(".", "_"))
    frames_bucket = FRAMES_BUCKET or bucket

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Download video from S3
        local_video = os.path.join(tmpdir, "input_video")
        logger.info(f"Downloading s3://{bucket}/{key}")
        s3.download_file(bucket, key, local_video)

        # 2. Get video duration with ffprobe
        duration = get_video_duration(local_video)
        logger.info(f"Video duration: {duration:.2f}s")

        # 3. Calculate timestamps for evenly spaced frames
        timestamps = [
            duration * i / (NUM_FRAMES + 1)
            for i in range(1, NUM_FRAMES + 1)
        ]

        # 4. Extract each frame with ffmpeg
        frame_keys = []
        for i, ts in enumerate(timestamps):
            frame_filename = f"frame_{i:03d}.jpg"
            local_frame = os.path.join(tmpdir, frame_filename)
            extract_frame(local_video, ts, local_frame)

            # 5. Upload frame to S3
            s3_key = f"frames/{video_id}/{frame_filename}"
            s3.upload_file(
                local_frame,
                frames_bucket,
                s3_key,
                ExtraArgs={"ContentType": "image/jpeg"},
            )
            frame_keys.append(s3_key)
            logger.info(f"Uploaded frame to s3://{frames_bucket}/{s3_key}")

    return {
        "bucket": bucket,
        "key": key,
        "video_id": video_id,
        "frames_bucket": frames_bucket,
        "frame_keys": frame_keys,
        "num_frames": len(frame_keys),
        "duration_seconds": round(duration, 2),
    }


def get_video_duration(video_path: str) -> float:
    """Use ffprobe to get video duration in seconds."""
    cmd = [
        FFMPEG_PATH.replace("ffmpeg", "ffprobe"),
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return float(result.stdout.strip())


def extract_frame(video_path: str, timestamp: float, output_path: str):
    """Extract a single frame at the given timestamp using ffmpeg."""
    cmd = [
        FFMPEG_PATH,
        "-ss", str(timestamp),
        "-i", video_path,
        "-frames:v", "1",
        "-q:v", "2",           # JPEG quality (2 = high)
        "-vf", "scale=1280:-1", # Resize to max 1280px wide, keep aspect ratio
        "-y",                   # Overwrite output
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr}")
