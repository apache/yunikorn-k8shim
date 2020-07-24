package helpers

import (
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	crdclientset "github.com/apache/incubator-yunikorn-k8shim/pkg/client/clientset/versioned"
	"github.com/apache/incubator-yunikorn-k8shim/test/e2e/framework/configmanager"
)

func GetApplicationObj(yamlPath string) (*v1alpha1.Application, error) {
	manifest, err := os.Open(yamlPath)
	if err != nil {
		return nil, err
	}
	appObj := v1alpha1.Application{}
	if err := yaml.NewYAMLOrJSONDecoder(manifest, 100).Decode(&appObj); err != nil {
		return nil, err
	}
	return &appObj, nil
}

func CreateApplication(crdclientset crdclientset.Interface, app *v1alpha1.Application, namespace string) error {
	_, err := crdclientset.ApacheV1alpha1().Applications(namespace).Create(app)
	if err != nil {
		return err
	}
	return nil
}

func UpdateApplication(crdclientset crdclientset.Interface, app *v1alpha1.Application, namespace string) error {
	_, err := crdclientset.ApacheV1alpha1().Applications(namespace).Update(app)
	if err != nil {
		return err
	}
	return nil
}

func GetApplication(crdclientset crdclientset.Interface, namespace, name string) (*v1alpha1.Application, error) {
	app, err := crdclientset.ApacheV1alpha1().Applications(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return app, nil
}

func DeleteApplication(crdclientset crdclientset.Interface, namespace, name string) error {
	err := crdclientset.ApacheV1alpha1().Applications(namespace).Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func NewApplicationClient() (*crdclientset.Clientset, error) {
	kubeconfig := configmanager.YuniKornTestConfig.KubeConfig
	appClient, err := NewClient(kubeconfig)
	if err != nil {
		return nil, err
	}
	return appClient, nil
}

func NewClient(kubeconfig string) (*crdclientset.Clientset, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	appClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return appClient, nil
}
