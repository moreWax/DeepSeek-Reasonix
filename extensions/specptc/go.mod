module github.com/esengine/DeepSeek-Reasonix/extensions/specptc

go 1.25

require github.com/esengine/DeepSeek-Reasonix/sdk/go v0.0.0

require (
	github.com/XiaoConstantine/rlm-go v0.1.1-0.20260512202825-43905b967530
	github.com/google/uuid v1.6.0 // indirect
	github.com/traefik/yaegi v0.16.1 // indirect
)

replace github.com/XiaoConstantine/rlm-go => ./third_party/rlm-go

replace github.com/esengine/DeepSeek-Reasonix/sdk/go => ../../sdk/go
