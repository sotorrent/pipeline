#!/bin/bash
set -Eeuo pipefail

echo "Compressing JSONL files..."
for file in ./*.jsonl; do
	bzip2 "$file" && rm "$file";
done
