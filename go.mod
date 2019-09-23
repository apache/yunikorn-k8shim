module github.com/cloudera/yunikorn-k8shim

go 1.12

require (
	cloud.google.com/go v0.46.3 // indirect
	github.com/alecthomas/units v0.0.0-20190910110746-680d30ca3117 // indirect
	github.com/cloudera/yunikorn-core v0.0.0-20190923163201-bda6a48a6bbb
	github.com/cloudera/yunikorn-scheduler-interface v0.0.0-20190909100534-0557527533f6
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190620071333-e64a0ec8b42a // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/googleapis/gnostic v0.3.0 // indirect
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.2 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/looplab/fsm v0.1.0
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pborman/uuid v1.2.0 // indirect
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.7.0 // indirect
	github.com/prometheus/procfs v0.0.5 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190923035154-9ee001bba392 // indirect
	golang.org/x/exp v0.0.0-20190919035709-81c71964d733 // indirect
	golang.org/x/image v0.0.0-20190910094157-69e4b8554b2a // indirect
	golang.org/x/mobile v0.0.0-20190910184405-b558ed863381 // indirect
	golang.org/x/net v0.0.0-20190923162816-aa69164e4478 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0 // indirect
	golang.org/x/tools v0.0.0-20190920225731-5eefd052ad72 // indirect
	google.golang.org/appengine v1.6.3 // indirect
	google.golang.org/genproto v0.0.0-20190916214212-f660b8655731 // indirect
	google.golang.org/grpc v1.23.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20190624085159-95846d7ef82a
	k8s.io/apiextensions-apiserver v0.0.0-20190516231611-bf6753f2aa24 // indirect
	k8s.io/apimachinery v0.0.0-20190624085041-961b39a1baa0
	k8s.io/apiserver v0.0.0-20190516230822-f89599b3f645 // indirect
	k8s.io/client-go v0.0.0-20190624085356-2c6e35a5b9cf
	k8s.io/csi-translation-lib v0.0.0-20190624131023-0cad93d77298 // indirect
	k8s.io/klog v0.3.3 // indirect
	k8s.io/kube-openapi v0.0.0-20190603182131-db7b694dc208 // indirect
	k8s.io/kubernetes v1.14.3
	k8s.io/utils v0.0.0-20190607212802-c55fbcfc754a // indirect
)

replace k8s.io/cloud-provider v0.0.0-20190624091323-9dc79cf4f9c7 => k8s.io/cloud-provider v0.0.0-20190516232619-2bf8e45c8454
