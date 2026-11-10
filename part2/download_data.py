"""Download and verify the exact public SNAP snapshot used for Part 2."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import requests

URL = 'https://snap.stanford.edu/data/wiki-Vote.txt.gz'
SHA256 = '7d3e53626e14b8b09fb3b396bece9d481ad606bd64ceab066349ff57d4ada7fc'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', required=True)
    p.add_argument('--metadata', required=True)
    p.add_argument('--url', default=URL)
    p.add_argument('--expected-sha256', default=SHA256)
    a = p.parse_args()
    output = Path(a.output)
    metadata = Path(a.metadata)
    output.parent.mkdir(parents=True, exist_ok=True)
    metadata.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + '.partial')
    try:
        with requests.get(a.url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with temporary.open('wb') as file:
                for block in response.iter_content(1024 * 1024):
                    file.write(block)
        with temporary.open('rb') as file:
            digest = hashlib.file_digest(file, 'sha256').hexdigest()
        if digest.lower() != a.expected_sha256.lower():
            raise ValueError(f'SHA-256 mismatch: expected {a.expected_sha256}, received {digest}')
        with gzip.open(temporary, 'rb') as file:
            while file.read(1024 * 1024):
                pass  # Reading to EOF verifies the gzip trailer/CRC.
        size = temporary.stat().st_size
        temporary.replace(output)
        metadata.write_text(json.dumps({
            'downloaded_at': datetime.now(timezone.utc).isoformat(), 'source_url': a.url,
            'dataset_page': 'https://snap.stanford.edu/data/wiki-Vote.html',
            'sha256': digest, 'compressed_bytes': size, 'gzip_crc_verified': True,
            'checksum_provenance': 'Pinned observed snapshot hash; not a publisher-signed checksum',
            'published_statistics': {'vertices': 7115, 'directed_edges': 103689,
                                     'largest_weak_component_vertices': 7066, 'undirected_triangles': 608389}
        }, indent=2) + '\n')
        print(f'Download verified: {size} bytes, SHA-256 {digest}')
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
