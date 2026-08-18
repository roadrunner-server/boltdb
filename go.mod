module github.com/roadrunner-server/boltdb/v6

go 1.26

toolchain go1.26.6

require (
	github.com/roadrunner-server/api-plugins/v6 v6.0.0-beta.2
	github.com/roadrunner-server/endure/v2 v2.6.2
	github.com/roadrunner-server/errors v1.5.0
	github.com/stretchr/testify v1.12.0
	go.etcd.io/bbolt v1.5.0
	go.opentelemetry.io/contrib/propagators/jaeger v1.45.0
	go.opentelemetry.io/otel v1.45.0
	go.opentelemetry.io/otel/sdk v1.45.0
	go.opentelemetry.io/otel/trace v1.45.0
	google.golang.org/genproto v0.0.0-20260817212433-ac3dfec99bb1
)

exclude (
	github.com/spf13/viper v1.18.0
	github.com/spf13/viper v1.18.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
