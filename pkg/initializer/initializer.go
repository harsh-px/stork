package initializer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	"github.com/sirupsen/logrus"
	"k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	storkInitializerName = "stork.initializer.kubernetes.io"
	storkSchedulerName   = "stork"
)

// Initializer Kubernetes object initializer
type Initializer struct {
	Driver      volume.Driver
	lock        sync.Mutex
	started     bool
	stopChannel chan struct{}
}

// Start Starts the Initializer
func (i *Initializer) Start() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.started {
		return fmt.Errorf("Initializer has already been started")
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		logrus.Errorf("Error getting cluster config: %v", err)
		return err
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		logrus.Errorf("Error getting client, %v", err)
		return err
	}

	restClient := k8sClient.AppsV1beta1().RESTClient()
	resyncPeriod := 30 * time.Second

	deploymentsWatchlist := cache.NewListWatchFromClient(restClient, "deployments", v1.NamespaceAll, fields.Everything())
	includeUninitializedWatchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return deploymentsWatchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return deploymentsWatchlist.Watch(options)
		},
	}

	_, initController := cache.NewInformer(includeUninitializedWatchlist, &v1beta1.Deployment{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				err := i.initializeObject(obj, k8sClient)
				if err != nil {
					logrus.Errorf("Error initializing deployment: %v", err)
				}
			},
		},
	)

	statefulsetsWatchlist := cache.NewListWatchFromClient(restClient, "statefulsets", v1.NamespaceAll, fields.Everything())
	includeUninitializedWatchlist2 := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return statefulsetsWatchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return statefulsetsWatchlist.Watch(options)
		},
	}

	_, initController2 := cache.NewInformer(includeUninitializedWatchlist2, &v1beta1.StatefulSet{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				err := i.initializeObject(obj, k8sClient)
				if err != nil {
					logrus.Errorf("Error initializing deployment: %v", err)
				}
			},
		},
	)

	i.stopChannel = make(chan struct{})
	go initController.Run(i.stopChannel)
	go initController2.Run(i.stopChannel)
	i.started = true
	return nil
}

// Stop Stops the initializer
func (i *Initializer) Stop() error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if !i.started {
		return fmt.Errorf("Initializer has not been started")
	}

	close(i.stopChannel)
	i.started = false
	return nil
}

func (i *Initializer) initializeStatefulSet(ss *v1beta1.StatefulSet, clientset *kubernetes.Clientset) error {
	if ss.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := ss.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	o, err := runtime.NewScheme().DeepCopy(ss)
	if err != nil {
		return err
	}
	updatedStatefulSet := o.(*v1beta1.StatefulSet)

	if len(pendingInitializers) == 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Remove the initializer even if we get errors in this step
	driverVolumeTemplates, err := i.Driver.GetStatefulSetTemplates(ss)
	if err != nil {
		storklog.StatefulSetLog(ss).Infof("Error getting volume templates for statefulset: %v", err)
	} else if len(driverVolumeTemplates) > 0 {
		updatedStatefulSet.Spec.Template.Spec.SchedulerName = "stork"
	}

	newData, err := json.Marshal(updatedStatefulSet)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1beta1.StatefulSet{})
	if err != nil {
		return err
	}

	_, err = clientset.AppsV1beta1().StatefulSets(ss.Namespace).Patch(ss.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (i *Initializer) initializeDeployment(deployment *v1beta1.Deployment, clientset *kubernetes.Clientset) error {
	if deployment.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := deployment.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(deployment)
	if err != nil {
		return err
	}

	o, err := runtime.NewScheme().DeepCopy(deployment)
	if err != nil {
		return err
	}
	updatedDeployment := o.(*v1beta1.Deployment)

	if len(pendingInitializers) == 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Remove the initializer even if we get errors in this step
	driverVolumes, err := i.Driver.GetPodVolumes(&deployment.Spec.Template.Spec, deployment.Namespace)
	if err != nil {
		if _, ok := err.(*volume.ErrPVCPending); ok {
			updatedDeployment.Spec.Template.Spec.SchedulerName = "stork"
		}
		storklog.DeploymentLog(deployment).Infof("Error getting volumes for pod: %v", err)
	} else if len(driverVolumes) != 0 {
		updatedDeployment.Spec.Template.Spec.SchedulerName = "stork"
	}

	newData, err := json.Marshal(updatedDeployment)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1beta1.Deployment{})
	if err != nil {
		return err
	}

	_, err = clientset.ExtensionsV1beta1().Deployments(deployment.Namespace).Patch(deployment.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}
func (i *Initializer) initializeObject(obj interface{}, clientset *kubernetes.Clientset) error {
	switch obj.(type) {
	case *v1beta1.StatefulSet:
		return i.initializeStatefulSet(obj.(*v1beta1.StatefulSet), clientset)
	case *v1beta1.Deployment:
		return i.initializeDeployment(obj.(*v1beta1.Deployment), clientset)
	default:
		return fmt.Errorf("unsupported app type: %v", obj)
	}
}
