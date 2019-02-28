package schedule

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute
	mockTimeFilePath                  = "/etc/mock-time"
	// StorkTestModeEnvVariable is env variable to enable test mode features in stork
	StorkTestModeEnvVariable = "TEST_MODE"
	// MockTimeConfigMapName is the name of the config map used to mock times
	MockTimeConfigMapName = "stork-mock-time"
	// MockTimeConfigMapNamespace is the namespace of the config map used to mock times
	MockTimeConfigMapNamespace = "kube-system"
)

var mockTime *time.Time

// setMockTime is used in tests to update the time
func setMockTime(mt time.Time) {
	mockTime = &mt
}

func getCurrentTime() (time.Time, error) {
	// Check if mock-time file exists
	if dat, err := ioutil.ReadFile(mockTimeFilePath); err == nil {
		data := strings.TrimSpace(string(dat))
		if len(data) > 0 {
			logrus.Infof("[debug] found mock time: %s", data)
			layout := "Mon, 01/02/06, 03:04PM"
			t, err := time.Parse(layout, data)
			if err != nil {
				logrus.Errorf("failed to parse mock time: %s in file due to: %v", data, err)
				return time.Now(), err
			}

			logrus.Infof("[debug] parsed mock time: %v", t)
			return t, nil
		}
	}

	if mockTime != nil {
		return *mockTime, nil
	}
	return time.Now(), nil
}

// TriggerRequired Check if a trigger is required for a policy given the last
// trigger time
func TriggerRequired(
	policyName string,
	policyType stork_api.SchedulePolicyType,
	lastTrigger meta.Time,
) (bool, error) {
	schedulePolicy, err := k8s.Instance().GetSchedulePolicy(policyName)
	if err != nil {
		return false, err
	}

	if err := ValidateSchedulePolicy(schedulePolicy); err != nil {
		return false, err
	}

	now, err := getCurrentTime()
	if err != nil {
		return false, err
	}

	switch policyType {
	case stork_api.SchedulePolicyTypeInterval:
		if schedulePolicy.Policy.Interval == nil {
			return false, nil
		}
		duration := time.Duration(schedulePolicy.Policy.Interval.IntervalMinutes) * time.Minute
		// Trigger if more than intervalMinutes has passed since
		// last trigger
		if lastTrigger.Add(duration).Before(now) {
			return true, nil
		}
		return false, nil

	case stork_api.SchedulePolicyTypeDaily:
		if schedulePolicy.Policy.Daily == nil {
			return false, nil
		}

		policyHour, policyMinute, err := schedulePolicy.Policy.Daily.GetHourMinute()
		if err != nil {
			return false, err
		}

		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)
		// Go to next day if the trigger time has already
		// passed for today
		if nextTrigger.Before(now) {
			nextTrigger.Add(24 * time.Hour)
		}

		return checkTrigger(lastTrigger.Time, nextTrigger, now)

	case stork_api.SchedulePolicyTypeWeekly:
		if schedulePolicy.Policy.Weekly == nil {
			return false, nil
		}
		currentDay := now.Weekday()
		scheduledDay := stork_api.Days[schedulePolicy.Policy.Weekly.Day]
		policyHour, policyMinute, err := schedulePolicy.Policy.Weekly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), now.Day(), policyHour, policyMinute, 0, 0, time.Local)
		// Figure out how many days to add to get to the next
		// trigger week day
		if currentDay < scheduledDay {
			nextTrigger = nextTrigger.Add(time.Hour * time.Duration((scheduledDay-currentDay)*24))
		} else if currentDay > scheduledDay {
			nextTrigger = nextTrigger.Add(time.Duration((7-(currentDay-scheduledDay))*24) * time.Hour)
		}

		return checkTrigger(lastTrigger.Time, nextTrigger, now)
	case stork_api.SchedulePolicyTypeMonthly:
		if schedulePolicy.Policy.Monthly == nil {
			return false, nil
		}
		policyHour, policyMinute, err := schedulePolicy.Policy.Monthly.GetHourMinute()
		if err != nil {
			return false, err
		}
		nextTrigger := time.Date(now.Year(), now.Month(), schedulePolicy.Policy.Monthly.Date, policyHour, policyMinute, 0, 0, time.Local)

		return checkTrigger(lastTrigger.Time, nextTrigger, now)
	}
	return false, nil
}

func checkTrigger(
	lastTrigger time.Time,
	nextTrigger time.Time,
	now time.Time,
) (bool, error) {
	// If we had triggered after the scheduled time this month, don't
	// triggered again
	if lastTrigger.After(nextTrigger) || lastTrigger.Equal(nextTrigger) {
		return false, nil
	}

	// If we are within one hour after the next trigger time, trigger a new
	// schedule
	if now.After(nextTrigger) && now.Sub(nextTrigger).Hours() < 1 {
		return true, nil
	}
	return false, nil
}

// ValidateSchedulePolicy Validate if a given schedule policy is valid
func ValidateSchedulePolicy(policy *stork_api.SchedulePolicy) error {
	if policy == nil {
		return nil
	}
	if policy.Policy.Daily != nil {
		if err := policy.Policy.Daily.Validate(); err != nil {
			return err
		}
	}
	if policy.Policy.Weekly != nil {
		if err := policy.Policy.Weekly.Validate(); err != nil {
			return err
		}
	}
	if policy.Policy.Monthly != nil {
		if err := policy.Policy.Monthly.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Init initializes the schedule module
func Init() error {
	err := createCRD()
	if err != nil {
		return err
	}

	testMode := os.Getenv(StorkTestModeEnvVariable)
	if testMode == "true" {

		fn := func(object runtime.Object) error {
			logrus.Infof("[debug] watch triggered for configmap: %v", object)
			return nil
		}

		logrus.Infof("Stork test mode enabled. Watching for config map: %s for mock times", MockTimeConfigMapName)
		cm, err := k8s.Instance().GetConfigMap(MockTimeConfigMapName, MockTimeConfigMapNamespace)
		if err != nil {
			if errors.IsNotFound(err) {
				logrus.Infof("Stork in test mode, however no config map present to mock time. Skipping.")
				return nil
			}

			logrus.Errorf("failed to get config map: %s due to: %v", MockTimeConfigMapName, err)
			return err
		}

		cm = cm.DeepCopy()

		err = k8s.Instance().WatchConfigMap(cm, fn)
		if err != nil {
			logrus.Errorf("failed to watch on config map: %s due to: %v", MockTimeConfigMapName, err)
			return err
		}
	}

	return nil
}

func createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.SchedulePolicyResourceName,
		Plural:  stork_api.SchedulePolicyResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.ClusterScoped,
		Kind:    reflect.TypeOf(stork_api.SchedulePolicy{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
