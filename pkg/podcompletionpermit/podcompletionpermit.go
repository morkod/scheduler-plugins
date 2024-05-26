package podcompletionpermit

import (
	"context"
	"fmt"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/scheduler-plugins/apis/config"

	//"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"time"
)

const Name = "PodCompletionPermit"

type PodCompletionPermit struct {
	handle                   framework.Handle
	dependencyIndicatorLabel string
	podAppSelector           string
	versionSelector          string
	postfixSelector          string
	dependencyKind           string
	dependencyName           string
}

var _ = framework.PermitPlugin(&PodCompletionPermit{})

func CheckDependencyCompletion(namespace string, app string, postfix string, version string) bool {
	config, err1 := rest.InClusterConfig()
	if err1 != nil {
		panic(err1.Error())
	}
	clientset, err2 := kubernetes.NewForConfig(config)
	if err2 != nil {
		panic(err2.Error())
	}
	var podlist, err3 = clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err3 != nil {
		panic(err3.Error())
	}
	for _, pod := range podlist.Items {
		if postfix != "" {
			if pod.Labels["postfix"] != postfix {
				continue
			}
		}
		if pod.Labels["app-name"] == app && pod.Labels["version"] == version && pod.Labels["pod-kind"] == "migrator" {
			return pod.Status.Phase == v1.PodSucceeded
		}

	}
	return false
}

func (pcp *PodCompletionPermit) Name() string {
	return Name
}

func (pcp *PodCompletionPermit) Permit(_ context.Context, _ *framework.CycleState, p *v1.Pod, _ string) (*framework.Status, time.Duration) {
	if val, ok := (*p).Labels["dependsOnMigrator"]; ok && val == "true" {
		if ver, okk := (*p).Labels["version"]; okk {
			var namespace = (*p).Namespace
			var postfix = ""
			if pstfx, okkk := (*p).Labels["postfix"]; okkk {
				postfix = pstfx
			}
			if !CheckDependencyCompletion(namespace, (*p).Labels["app-name"], postfix, ver) {
				return framework.NewStatus(framework.Wait, "Waiting for dependency to complete"), 90 * time.Second
			} else {
				return framework.NewStatus(framework.Success, "Dependency completed"), 0 * time.Second
			}
		} else {
			return framework.NewStatus(framework.Error, "Expected version label"), 0 * time.Second
		}

	} else {
		return framework.NewStatus(framework.Success, "No dependencies to wait upon; Approving straight away"), 0 * time.Second
	}
}

func New(_ context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.PodCompletionPermitArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NetworkTrafficArgs, got %T", obj)
	}

	return &PodCompletionPermit{
		handle:     h,
		dependencyIndicatorLabel: args.dependencyIndicatorLabel,
		podAppSelector: args.podAppSelector,
		versionSelector: args.versionSelector,
		postfixSelector: args.postfixSelector,
		dependencyKind: args.dependencyKind,
		dependencyName: args.dependencyName
	}, nil
	return &PodCompletionPermit{handle: h}, nil
}
