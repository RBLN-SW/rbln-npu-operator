package main

import (
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rblnv1beta1.AddToScheme(scheme))
	utilruntime.Must(rebellionsaiv1alpha1.AddToScheme(scheme))
}
