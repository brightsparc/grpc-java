# Generate proto

Run the following command to from the root to generate golang protobuf

```
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    src/main/proto/pql.proto
```