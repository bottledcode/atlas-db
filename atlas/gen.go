package atlas

//go:generate protoc --go_out=. --go-grpc_out=. bootstrap/bootstrap.proto
//go:generate protoc --go_out=. --go-grpc_out=. consensus/consensus.proto
