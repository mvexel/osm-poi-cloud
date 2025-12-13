"""
Helper lambda to get the shards manifest from S3
"""

import json
import boto3
import os

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["DATA_BUCKET_NAME"]

def handler(event, context):
    run_id = event.get("run_id")
    if not run_id:
        raise ValueError("Missing 'run_id' in input event")

    manifest_key = f"runs/{run_id}/shards/manifest.json"

    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=manifest_key)
        manifest_content = json.loads(response["Body"].read())

        shards = []
        for feature in manifest_content.get("features", []):
            props = feature.get("properties", {})
            if "shard_id" in props and "z" in props and "x" in props and "y" in props:
                shards.append(
                    {
                        "shard_id": str(props["shard_id"]),
                        "z": int(props["z"]),
                        "x": int(props["x"]),
                        "y": int(props["y"]),
                    }
                )

        return {"status": "SUCCESS", "shards": shards}
    except Exception as e:
        print(f"Error reading manifest from s3://{BUCKET_NAME}/{manifest_key}: {e}")
        raise e
