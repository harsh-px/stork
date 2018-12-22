package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/heptio/ark/pkg/discovery"
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	stork "github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	resyncPeriod                      = 15 * time.Second

	updateCRD     = true
	dontUpdateCRD = false
)

// GroupSnapshotController groupSnapshotcontroller
type GroupSnapshotController struct {
	Driver             volume.Driver
	Recorder           record.EventRecorder
	discoveryHelper    discovery.Helper
	dynamicInterface   dynamic.Interface
	bgChannelsForRules map[string]chan bool
	minResourceVersion string
}

// Init Initialize the groupSnapshot controller
func (m *GroupSnapshotController) Init() error {
	logrus.Infof("[debug] Initializaing group snapshot controller")
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("Error getting cluster config: %v", err)
	}

	aeclient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Error getting apiextention client, %v", err)
	}

	err = m.createCRD()
	if err != nil {
		return err
	}

	discoveryClient := aeclient.Discovery()
	m.discoveryHelper, err = discovery.NewHelper(discoveryClient, logrus.New())
	if err != nil {
		return err
	}
	err = m.discoveryHelper.Refresh()
	if err != nil {
		return err
	}
	m.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	m.bgChannelsForRules = make(map[string]chan bool)

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.GroupVolumeSnapshot{}).Name(),
		},
		"",
		resyncPeriod,
		m)
}

// Handle updates for GroupSnapshot objects
func (m *GroupSnapshotController) Handle(ctx context.Context, event sdk.Event) error {
	var (
		groupSnapshot         *stork_api.GroupVolumeSnapshot
		updateCRDForThisEvent bool
		err                   error
	)

	switch o := event.Object.(type) {
	case *stork_api.GroupVolumeSnapshot:
		groupSnapshot = o

		if groupSnapshot.ResourceVersion < m.minResourceVersion {
			logrus.Infof("already processed groupSnapshot version higher than: %s. Skipping event.",
				groupSnapshot.ResourceVersion)
			return nil
		}

		if event.Deleted {
			return m.handleDelete(groupSnapshot)
		}

		switch groupSnapshot.Status.Stage {
		case stork_api.GroupSnapshotStageInitial,
			stork_api.GroupSnapshotStagePrechecks:
			updateCRDForThisEvent, err = m.handleInitial(groupSnapshot)
		case stork_api.GroupSnapshotStagePreSnapshot:
			updateCRDForThisEvent, err = m.handlePreSnap(groupSnapshot)
		case stork_api.GroupSnapshotStageSnapshot:
			updateCRDForThisEvent, err = m.handleSnap(groupSnapshot)

			// Terminate background commands regardless of failure if the snapshots are
			// triggered
			snapUID := string(groupSnapshot.ObjectMeta.UID)
			if areAllSnapshotsStarted(groupSnapshot.Status.VolumeSnapshots) {
				backgroundChannel, present := m.bgChannelsForRules[snapUID]
				if present {
					backgroundChannel <- true
					delete(m.bgChannelsForRules, snapUID)
				}
			}
		case stork_api.GroupSnapshotStagePostSnapshot:
			updateCRDForThisEvent, err = m.handlePostSnap(groupSnapshot)
		case stork_api.GroupSnapshotStageFinal:
			return m.handleFinal(groupSnapshot)
		default:
			log.GroupSnapshotLog(groupSnapshot).Errorf("Invalid stage for groupSnapshot: %v", groupSnapshot.Status.Stage)
		}
	}

	if err != nil {
		log.GroupSnapshotLog(groupSnapshot).Errorf("Error handling event: %v err: %v", event, err.Error())

		m.Recorder.Event(groupSnapshot,
			v1.EventTypeWarning,
			string(stork_api.GroupSnapshotFailed),
			err.Error())
		// Don't return err since that translates to a sync error
	}

	if updateCRDForThisEvent {
		//resourceVersion, _ := strconv.Atoi(groupSnapshot.ResourceVersion)

		updateErr := sdk.Update(groupSnapshot)
		if updateErr != nil {
			return updateErr
		}

		// Since we updated, bump the minimum resource version
		//m.minResourceVersion = strconv.FormatInt(int64(resourceVersion+1), 10)
		m.minResourceVersion = groupSnapshot.ResourceVersion
		logrus.Infof("[debug] new resource version: %s", m.minResourceVersion)
	}

	return nil
}

func (m *GroupSnapshotController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.GroupSnapshotResourceName,
		Plural:  stork_api.GroupSnapshotResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.GroupVolumeSnapshot{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}

func (m *GroupSnapshotController) handleInitial(groupSnap *stork_api.GroupVolumeSnapshot) (bool, error) {
	log.GroupSnapshotLog(groupSnap).Infof("[debug] handle initial. Version: %s", groupSnap.ResourceVersion)
	var err error

	// Pre checks
	if len(groupSnap.Spec.PVCSelector.MatchExpressions) > 0 {
		err = fmt.Errorf("matchExpressions are currently not supported in the spec. Use matchLabels")
	}

	if len(groupSnap.Spec.PVCSelector.MatchLabels) == 0 {
		err = fmt.Errorf("matchLabels are required for group snapshots. Refer to spec examples")
	}

	if err != nil {
		groupSnap.Status.Status = stork_api.GroupSnapshotFailed
		groupSnap.Status.Stage = stork_api.GroupSnapshotStageFinal
		// Don't return original err as that means a sync failure for the controller
		return updateCRD, nil
	}

	_, err = k8sutils.GetPVCsForGroupSnapshot(groupSnap.Namespace, groupSnap.Spec.PVCSelector.MatchLabels)
	if err != nil {
		if groupSnap.Status.Status == stork_api.GroupSnapshotPending {
			return dontUpdateCRD, err
		}

		groupSnap.Status.Status = stork_api.GroupSnapshotPending
		groupSnap.Status.Stage = stork_api.GroupSnapshotStagePrechecks
	} else {
		groupSnap.Status.Status = stork_api.GroupSnapshotInProgress
		// done with pre-checks, move to pre-snapshot stage
		groupSnap.Status.Stage = stork_api.GroupSnapshotStagePreSnapshot
	}

	return updateCRD, err
}

func (m *GroupSnapshotController) handlePreSnap(groupSnap *stork_api.GroupVolumeSnapshot) (bool, error) {
	logrus.Infof("[debug] handle pre snapshot")
	ruleName := groupSnap.Spec.PreSnapshotRule
	if len(ruleName) == 0 {
		groupSnap.Status.Status = stork_api.GroupSnapshotInProgress
		// No rule, move to snapshot stage
		groupSnap.Status.Stage = stork_api.GroupSnapshotStageSnapshot
		return updateCRD, nil
	}

	logrus.Infof("Running pre-snapshot rule: %s", ruleName)
	r, err := k8s.Instance().GetRule(ruleName, groupSnap.Namespace)
	if err != nil {
		return dontUpdateCRD, err
	}

	pvcs, err := k8sutils.GetPVCsForGroupSnapshot(groupSnap.Namespace, groupSnap.Spec.PVCSelector.MatchLabels)
	if err != nil {
		return dontUpdateCRD, err
	}

	backgroundCommandTermChan, err := rule.ExecuteRule(r, rule.PreExecRule, groupSnap, pvcs)
	if err != nil {
		if backgroundCommandTermChan != nil {
			backgroundCommandTermChan <- true // terminate background commands if running
		}

		return dontUpdateCRD, err
	}

	if backgroundCommandTermChan != nil {
		snapUID := string(groupSnap.ObjectMeta.UID)
		m.bgChannelsForRules[snapUID] = backgroundCommandTermChan
	}

	// done with pre-snapshot, move to snapshot stage
	groupSnap.Status.Stage = stork_api.GroupSnapshotStageSnapshot
	return updateCRD, nil
}

func (m *GroupSnapshotController) handleSnap(groupSnap *stork_api.GroupVolumeSnapshot) (bool, error) {
	log.GroupSnapshotLog(groupSnap).Infof("[debug] handle snap. VERSION: *** %s ***", groupSnap.ResourceVersion)
	var err error
	var snapshots []*crdv1.VolumeSnapshot
	var stage stork_api.GroupVolumeSnapshotStageType
	var status stork_api.GroupVolumeSnapshotStatusType
	if len(groupSnap.Status.VolumeSnapshots) > 0 {
		logrus.Infof("[debug] group snapshot already triggered. CHECK status")
		snapshots, err = m.Driver.GetGroupSnapshotStatus(groupSnap)
		if err != nil {
			logrus.Errorf("group snapshot status returned err: %v", err)
			m.Recorder.Event(groupSnap,
				v1.EventTypeWarning,
				string(stork_api.GroupSnapshotFailed),
				err.Error())

			return dontUpdateCRD, err
		}
	} else {
		logrus.Infof("[debug] CREATE group snapshot")
		snapshots, err = m.Driver.CreateGroupSnapshot(groupSnap)
		if err != nil {
			m.Recorder.Event(groupSnap,
				v1.EventTypeWarning,
				string(stork_api.GroupSnapshotFailed),
				err.Error())

			return dontUpdateCRD, err
		}
	}

	if len(snapshots) == 0 {
		err = fmt.Errorf("group snapshot call returned 0 snapshots from driver")
		m.Recorder.Event(groupSnap,
			v1.EventTypeWarning,
			string(stork_api.GroupSnapshotFailed),
			err.Error())
		return dontUpdateCRD, err
	}

	if isAnySnapshotFailed(snapshots) {
		logrus.Infof("[debug] some snapshots have failed")
		snapshots = nil // so that snapshots are retried
		stage = stork_api.GroupSnapshotStageSnapshot
		status = stork_api.GroupSnapshotPending
	} else if areAllSnapshotsDone(snapshots) {
		logrus.Infof("[debug] are snapshots are done")
		// done with snapshot, move to post-snapshot stage
		stage = stork_api.GroupSnapshotStagePostSnapshot
		status = stork_api.GroupSnapshotInProgress
	} else {
		// snapshot are active and all are not done
		logrus.Infof("[debug] some snapshots still in progress")
		stage = stork_api.GroupSnapshotStageSnapshot
		status = stork_api.GroupSnapshotInProgress
	}

	groupSnap.Status.VolumeSnapshots = snapshots
	groupSnap.Status.Status = status
	groupSnap.Status.Stage = stage

	for _, snapInResp := range groupSnap.Status.VolumeSnapshots {
		logrus.Infof("[debug] updated snapshots: %s conditions: %v", snapInResp.Metadata.Name, snapInResp.Status.Conditions)
		logrus.Infof("[debug]     annotations: %v", snapInResp.Metadata.Annotations)
	}

	return updateCRD, nil
}

func (m *GroupSnapshotController) handlePostSnap(groupSnap *stork_api.GroupVolumeSnapshot) (bool, error) {
	logrus.Infof("[debug] handle post snapshot")
	ruleName := groupSnap.Spec.PostSnapshotRule
	if len(ruleName) == 0 { // No rule, move to final stage
		groupSnap.Status.Status = stork_api.GroupSnapshotSuccessful
		groupSnap.Status.Stage = stork_api.GroupSnapshotStageFinal
		return updateCRD, nil
	}

	logrus.Infof("Running post-snapshot rule: %s", ruleName)
	r, err := k8s.Instance().GetRule(ruleName, groupSnap.Namespace)
	if err != nil {
		return dontUpdateCRD, err
	}

	pvcs, err := k8sutils.GetPVCsForGroupSnapshot(groupSnap.Namespace, groupSnap.Spec.PVCSelector.MatchLabels)
	if err != nil {
		return dontUpdateCRD, err
	}

	_, err = rule.ExecuteRule(r, rule.PostExecRule, groupSnap, pvcs)
	if err != nil {
		return dontUpdateCRD, err
	}

	// done with post-snapshot, move to final stage
	groupSnap.Status.Status = stork_api.GroupSnapshotSuccessful
	groupSnap.Status.Stage = stork_api.GroupSnapshotStageFinal
	return updateCRD, nil
}

func (m *GroupSnapshotController) handleFinal(groupSnap *stork_api.GroupVolumeSnapshot) error {
	// currently nothing to do
	return nil
}

func (m *GroupSnapshotController) handleDelete(groupSnap *stork_api.GroupVolumeSnapshot) error {
	err := m.Driver.DeleteGroupSnapshot(groupSnap)
	if err != nil {
		m.Recorder.Event(groupSnap,
			v1.EventTypeWarning,
			string(stork_api.GroupSnapshotFailed),
			err.Error())

		return err
	}

	return nil
}

// isAnySnapshotFailed checks if any of the given snapshots is in error state
func isAnySnapshotFailed(snapshots []*crdv1.VolumeSnapshot) bool {
	failed := false

	for _, snapshot := range snapshots {
		conditions := snapshot.Status.Conditions
		if len(conditions) > 0 {
			lastCondition := conditions[0]
			if lastCondition.Status == v1.ConditionTrue && lastCondition.Type == crdv1.VolumeSnapshotConditionError {
				failed = true
				break
			}
		}
	}

	return failed
}

func areAllSnapshotsStarted(snapshots []*crdv1.VolumeSnapshot) bool {
	if len(snapshots) == 0 {
		return false
	}

	for _, snapshot := range snapshots {
		if len(snapshot.Status.Conditions) == 0 {
			// no conditions so assuming not started as rest all conditions indicate the
			// snapshot is either terminal (done, failed) or active.
			return false
		}
	}

	return true
}

func areAllSnapshotsDone(snapshots []*crdv1.VolumeSnapshot) bool {
	allDone := true

	for _, snapshot := range snapshots {
		conditions := snapshot.Status.Conditions
		if len(conditions) > 0 {
			lastCondition := conditions[0]
			if lastCondition.Status == v1.ConditionTrue && lastCondition.Type != crdv1.VolumeSnapshotConditionReady {
				allDone = false
				break
			}
		} else { // no conditions. So not done.
			allDone = false
			break
		}
	}
	return allDone
}
