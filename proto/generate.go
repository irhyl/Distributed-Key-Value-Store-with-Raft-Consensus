//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative raftkv.proto

// Package proto contains the generated gRPC/protobuf types used by all
// layers of the raftkv stack. Run `make proto` or `go generate ./proto/`
// to regenerate after editing raftkv.proto.
//
// Prerequisites:
//
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
package proto
