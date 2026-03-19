# Search / Query Data Reference

Query data in Weaviate collections using various search strategies.

## Fetch (no search, just retrieve objects)
```bash
weaviate-cli query data --collection "Movies" --search_type fetch --limit 10 --json
```

## Vector Search (near_text)
```bash
weaviate-cli query data --collection "Movies" --search_type vector --query "Action movie" --limit 10 --json
```
Requires a vectorizer configured on the collection.

## Keyword Search (BM25)
```bash
weaviate-cli query data --collection "Movies" --search_type keyword --query "Action movie" --limit 10 --json
```

## Hybrid Search (vector + keyword)
```bash
weaviate-cli query data --collection "Movies" --search_type hybrid --query "Action movie" --limit 10 --json
```

## UUID Search
```bash
weaviate-cli query data --collection "Movies" --search_type uuid --query "abc-123-def" --limit 1 --json
```

## Options

- `--collection` -- Collection to query (default: "Movies")
- `--search_type` -- fetch, vector, keyword, hybrid, uuid (default: fetch)
- `--query` -- Search query text (default: "Action movie")
- `--limit` -- Number of results (default: 10)
- `--properties` -- Properties to display (default: "title,keywords")
- `--consistency_level` -- quorum, all, one (default: quorum)
- `--tenants` -- Comma-separated tenants to query
- `--target_vector` -- Named vector to target

## Search Type Selection Guide

| Goal | Search Type |
|------|-------------|
| Browse/list objects | `fetch` |
| Find conceptually similar content | `vector` |
| Find exact terms or IDs | `keyword` |
| Best general-purpose search | `hybrid` |
| Retrieve specific object by UUID | `uuid` |

## Prerequisites

1. Collection must exist with data
2. For `vector` and `hybrid`: collection needs a vectorizer (or vectors must be stored)
3. For multi-tenant queries: specify `--tenants`

## Notes

- `--json` returns objects with uuid, properties, distance, certainty, and score fields
- `fetch` does not use the `--query` parameter
- `uuid` search ignores `--limit` (always returns 0 or 1 result)
