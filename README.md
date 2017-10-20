# S3 Restore tool
Restores a versioned S3 bucket to a previous point in time.

## Usage
```bash
AWS_ACCESS_KEY=access AWS_REGION=us-west-2 AWS_SECRET_ACCESS_KEY=secret ./s3-restore --bucket <bucket> --time "2017-10-09 20:58:41 +0000 UTC" restore
```

## AWS Credentials
See the aws docs here for information on advanced credentials configuration
http://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html

## Options
- `--bucket <name>` processes the named bucket
- `--time <time>` accepts any format accepted by https://github.com/araddon/dateparse
- `--endpoint-url <url>` for connecting to third party S3 compatible APIs like EMC ECS
- `--debug` prints debug logging

## Known Issues
- On EMC ECS, the restore will fail with "No Such Version" if you attempt to run it a bucket that was versioned after keys already existed in it. EMC ECS does not support reverting to `null` versionId. This limitation means the bucket you are attempting to restore needs to be versioned from the beginning.  To work around this issue you can copy all of the objects to a new bucket, delete the original bucket, recreate it with versioning on, and move the keys back into the new versioned bucket with the same name as the original.
