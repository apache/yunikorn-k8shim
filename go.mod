module github.com/cloudera/k8s-shim

go 1.12

require (
	github.com/cloudera/scheduler-interface v0.0.0-20190629053425-455c193da6b1
	github.com/cloudera/yunikorn-core v0.0.0-20190629041148-266dbf69b666
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190620071333-e64a0ec8b42a // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/googleapis/gnostic v0.3.0 // indirect
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.2 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/looplab/fsm v0.1.0
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pborman/uuid v1.2.0 // indirect
	github.com/prometheus/client_golang v1.0.0 // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	go.uber.org/atomic v1.4.0 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190611184440-5c40567a22f8 // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	google.golang.org/appengine v1.6.1 // indirect
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
