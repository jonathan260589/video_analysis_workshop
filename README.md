# Serverless Video Analysis Pipeline

Analyzes videos uploaded to S3 using AWS Step Functions, Lambda, and Amazon Bedrock.

## Architecture

```
S3 upload (uploads/)
    └─► EventBridge
            └─► Step Functions
                    ├─► Lambda: frame-extractor
                    ├─► Lambda: bedrock-analyzer
                    ├─► Lambda: marengo-embeddings
                    └─► Lambda: results-writer ─► DynamoDB
```

## Lambdas

**`frame-extractor`**
Downloads the video from S3 and uses ffmpeg (Lambda Layer) to extract 5 evenly-spaced keyframes, saved back to S3 under `frames/`.

**`bedrock-analyzer`**
For each frame: calls Nova Pro (multimodal) for a text description, and Titan Multimodal Embeddings for a 1024-dim vector.

**`results-writer`**
Aggregates all outputs, generates a one-paragraph summary via Nova Pro, writes the full result to DynamoDB, and saves a readable JSON summary to S3 under `results/`.

## Trigger

Upload any video to `s3://YOUR_BUCKET/uploads/` — the pipeline starts automatically via EventBridge.

## Configuration

Each Lambda reads its settings from environment variables. Key ones:

| Variable | Used by | Description |
|---|---|---|
| `FRAMES_BUCKET` | frame-extractor | S3 bucket for frames output |
| `NUM_FRAMES` | frame-extractor | Number of frames to extract (default: 5) |
| `NOVA_MODEL_ID` | bedrock-analyzer, results-writer | `amazon.nova-pro-v1:0` |
| `EMBEDDINGS_MODEL_ID` | bedrock-analyzer | `amazon.titan-embed-image-v1` |
| `DYNAMODB_TABLE` | results-writer | `video-analysis-results` |

## Prerequisites

- Bedrock model access enabled for Nova Pro, Titan Embeddings, and Marengo
- ffmpeg Lambda Layer attached to `frame-extractor`
- IAM role with permissions for S3, Bedrock, and DynamoDB (see `iam/policies.json`)
