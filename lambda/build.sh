#!/usr/bin/env bash
# Package each Lambda as a plain zip (handler only — both functions are
# stdlib + boto3, which the Lambda runtime already provides, so no deps to
# vendor and no layer needed). Output: dist/<function>.zip
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
dist="$here/dist"
mkdir -p "$dist"

for fn in imdb_mirror html_render; do
  out="$dist/streaming_${fn}.zip"
  rm -f "$out"
  ( cd "$here/$fn" && zip -q -r "$out" handler.py )
  echo "built $out"
done

echo "Done. Deploy with, e.g.:"
echo "  aws lambda update-function-code \\"
echo "    --function-name streaming_imdb_mirror \\"
echo "    --zip-file fileb://$dist/streaming_imdb_mirror.zip --region us-east-1"
