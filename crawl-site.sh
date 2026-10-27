#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-https://web-scraper-lightweight.onrender.com}"
URL="${1:?Usage: ./crawl-site.sh https://example.com [maxPages]}"
MAX_PAGES="${2:-20}"
OUT="${OUT:-$PWD/website-crawl-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"

printf 'Submitting crawl for %s (maxPages=%s)\n' "$URL" "$MAX_PAGES"
curl -fsS --max-time 30 \
  -X POST "$BASE/crawl" \
  -H 'Content-Type: application/json' \
  -d "$(python3 - "$URL" "$MAX_PAGES" <<'PY'
import json,sys
print(json.dumps({"url":sys.argv[1],"maxPages":int(sys.argv[2]),"maxDepth":3}))
PY
)" -o "$OUT/submission.json"

JOB_ID="$(python3 - "$OUT/submission.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))["job_id"])
PY
)"
echo "Job ID: $JOB_ID"

i=0
while [ "$i" -lt 120 ]; do
  i=$((i+1))
  curl -fsS --max-time 30 "$BASE/crawl/$JOB_ID" -o "$OUT/job-status.json" || {
    echo "Could not poll job. Last status saved to $OUT/job-status.json"
    exit 1
  }
  STATUS="$(python3 - "$OUT/job-status.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1])).get("status","unknown"))
PY
)"
  echo "Poll $i: $STATUS"
  case "$STATUS" in
    completed|partial)
      python3 - "$OUT/job-status.json" "$OUT/website-profile.json" <<'PY'
import json,sys
job=json.load(open(sys.argv[1]))
result=job.get("result")
if not result:
    print(json.dumps(job,indent=2))
    raise SystemExit("No result object in completed job")
with open(sys.argv[2],"w") as f:
    json.dump(result,f,ensure_ascii=False,indent=2)
print("Saved full crawl result:",sys.argv[2])
print("Pages succeeded:",result.get("crawl",{}).get("pages_succeeded"))
print("Pages failed:",result.get("crawl",{}).get("pages_failed"))
print("Text characters:",result.get("crawl",{}).get("total_text_characters"))
print("Page types:",result.get("site_profile",{}).get("page_counts_by_type"))
PY
      echo "Full job response: $OUT/job-status.json"
      exit 0
      ;;
    failed)
      cat "$OUT/job-status.json"
      exit 1
      ;;
  esac
  sleep 3
done

echo "Timed out waiting for crawl. Job can still be checked at $BASE/crawl/$JOB_ID"
exit 1
