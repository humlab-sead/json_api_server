curl -X POST http://localhost:8484/dendro/dynamicchart \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "example-request-123",
    "sites": [4149, 4152, 3973],
    "variable": "Tree species"
  }'
