package vm

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/golang/glog"
	api "github.com/rancher/vm/pkg/apis/ranchervm/v1alpha1"
	"github.com/rancher/vm/pkg/common"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"reflect"
	"strconv"
	"time"
)

func (ctrl *VirtualMachineController) machineWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.machineQueue.Get()
		if quit {
			return true
		}
		defer ctrl.machineQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("machineWorker[%s]", key)

		_, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(2).Infof("machineWorker[%s] error parsing key: %v", key, err)
			return false
		}

		machine, err := ctrl.machineLister.Get(name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				glog.V(2).Infof("machineWorker[%s] error getting machine: %v", key, err)
				ctrl.machineQueue.AddRateLimited(key)
				glog.V(4).Infof("machineWorker[%s] requeued: %v", key, err)
			}
			return false
		}

		if err := ctrl.process(machine, keyObj); err != nil {
			ctrl.machineQueue.AddRateLimited(key)
			glog.V(4).Infof("machineWorker[%s] requeued: %v", key, err)
		}
		return false
	}
	for {
		if quit := workFunc(); quit {
			glog.Infof("machineWorker: shutting down")
			return
		}
	}
}

func (ctrl *VirtualMachineController) process(machine *api.VirtualMachine, keyObj interface{}) error {
	if machine.DeletionTimestamp == nil {
		if updated, err := ctrl.prepareMachine(machine); updated {
			return err
		}

		var err error
		switch machine.Spec.Action {
		case api.ActionStart:
			if machine.Spec.Volume.Longhorn != nil {
				if err := ctrl.createLonghornVolume(machine); err != nil {
					return err
				}
			}
			err = ctrl.start(machine)
		case api.ActionStop:
			err = ctrl.stop(machine)
		default:
			glog.Warningf("detected machine %s/%s with invalid action \"%s\"", common.NamespaceVM, machine.Name, machine.Spec.Action)
			// TODO change VM state to ERROR, return no error (don't requeue)
			return nil
		}
		return err
	}

	if machine.Status.State != api.StateTerminating {
		return ctrl.setTerminating(machine)
	}

	err1 := ctrl.deleteMachinePods(machine)
	err2 := ctrl.deleteConsolePod(machine)
	err3 := ctrl.deleteConsoleService(machine)

	if machine.Spec.Volume.Longhorn != nil {
		if err := ctrl.deleteLonghornVolume(machine); err != nil {
			return err
		}
	}

	if apierrors.IsNotFound(err1) &&
		apierrors.IsNotFound(err2) &&
		apierrors.IsNotFound(err3) {
		cleaned, err := ctrl.checkCleanupJob(machine)
		glog.V(5).Infof("cleaned was %v", cleaned)
		if err != nil {
			if apierrors.IsNotFound(err) {
				glog.V(5).Infof("creating cleanup job")
				ctrl.createCleanupJob(machine)
			} else {
				glog.Error(err)
			}
			return fmt.Errorf("hello world")
		}
		if cleaned {
			glog.V(5).Infof("deleting cleanup job")
			ctrl.deleteCleanupJob(machine)
			return ctrl.removeFinalizer(machine)
		}
	}
	glog.V(5).Infof("end of the road")
	return fmt.Errorf("returning here is supposed to happen if we don't delete all the pods and stuff")
}

func (ctrl *VirtualMachineController) prepareMachine(machine *api.VirtualMachine) (bool, error) {
	// if not present - set instance id, mac address, finalizer
	if machine.Status.ID == "" && machine.Status.MAC == "" && len(machine.Finalizers) == 0 {
		uid := string(machine.UID)
		mutable := machine.DeepCopy()
		mutable.Status.ID = fmt.Sprintf("i-%s", uid[:8])
		mutable.Status.MAC = fmt.Sprintf("%s:%s:%s:%s:%s", common.RancherOUI, uid[:2], uid[2:4], uid[4:6], uid[6:8])
		mutable.Finalizers = append(mutable.Finalizers, common.FinalizerDeletion)
		glog.V(5).Infof("preparing machine %s", machine.Name)
		mutable, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(mutable)
		if err := ctrl.verifyMachine(mutable); err != nil {
			glog.Errorf("error while verifying machine!!! %s", mutable.Name)
		}
		return true, err
	}
	return false, nil
}

func (ctrl *VirtualMachineController) start(machine *api.VirtualMachine) error {
	_, pod, err := ctrl.updateMachinePod(machine)
	if err != nil {
		glog.Warningf("error updating machine pod %s/%s: %v", common.NamespaceVM, machine.Name, err)
		return err
	}

	if pod != nil && pod.Name != "" {
		if err = ctrl.updateNovnc(machine, pod.Name); err != nil {
			glog.Warningf("error updating novnc %s/%s: %v", common.NamespaceVM, machine.Name, err)
		}
	}

	// If machine is in pending state and pod is unschedulable, check to see if the
	// requested node name matches the pod node affinity. If they are mismatched,
	// delete the pod and allow the process to start over.
	if machine.Status.State == api.StatePending && pod != nil && IsPodUnschedulable(pod) {
		if pod.Spec.Affinity != nil &&
			pod.Spec.Affinity.NodeAffinity != nil &&
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil &&
			len(pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 1 {

			nodeSelector := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			if len(nodeSelector.NodeSelectorTerms) == 1 &&
				len(nodeSelector.NodeSelectorTerms[0].MatchExpressions) == 1 {

				requirement := nodeSelector.NodeSelectorTerms[0].MatchExpressions[0]
				if requirement.Key == common.LabelNodeHostname &&
					requirement.Operator == corev1.NodeSelectorOpIn &&
					len(requirement.Values) == 1 &&
					machine.Spec.NodeName != requirement.Values[0] {

					glog.V(2).Infof("User modified selector for unschedulable machine %s", machine.Name)
					return ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).Delete(pod.Name, &metav1.DeleteOptions{})
				}
			}
		}
	}

	if pod != nil && machine.Spec.NodeName != "" &&
		machine.Spec.NodeName != pod.Spec.NodeName &&
		machine.Status.State == api.StateRunning ||
		machine.Status.State == api.StateMigrating {
		return ctrl.migrateMachine(machine)
	}

	return err
}

func (ctrl *VirtualMachineController) stop(machine *api.VirtualMachine) (err error) {
	machine2 := machine.DeepCopy()

	if err = ctrl.deleteMachinePods(machine); err == nil {
		machine2.Status.State = api.StateStopping
	} else if apierrors.IsNotFound(err) {
		machine2.Status.State = api.StateStopped
		machine2.Status.NodeName = ""
	} else {
		machine2.Status.State = api.StateError
	}

	if err = ctrl.deleteMigrationJob(machine); err == nil {
		machine2.Status.State = api.StateStopping
	} else if !apierrors.IsNotFound(err) {
		machine2.Status.State = api.StateError
	}

	if err = ctrl.deleteConsolePod(machine); err == nil {
		// if either the machine or novnc pod had to be deleted, we are stopping
		machine2.Status.State = api.StateStopping
	} else if !apierrors.IsNotFound(err) {
		machine2.Status.State = api.StateError
	}
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(machine2.Name, metav1.GetOptions{})
		toUpdate, err := ctrl.machineLister.Get(machine.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				glog.Errorf("unknown error encountered when stopping %v", err)
			}
		}
		if toUpdate.Status.State == machine2.Status.State && toUpdate.Status.NodeName == machine2.Status.NodeName {
			return nil
		}
		toUpdate.Status.State = machine2.Status.State
		toUpdate.Status.NodeName = machine2.Status.NodeName
		glog.V(5).Infof("stopping machine %s", machine.Name)
		toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
		if err := ctrl.verifyMachine(toUpdate); err != nil {
			glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
		}
		return updateErr
	})

	if retryErr != nil {
		glog.Errorf("error while updating vm object while stopping %v", retryErr)
	}
	return retryErr
}

func (ctrl *VirtualMachineController) updateMachinePod(machine *api.VirtualMachine) (machine2 *api.VirtualMachine, pod *corev1.Pod, err error) {
	image, err := ctrl.machineImageLister.Get(machine.Spec.MachineImage)
	if err != nil {
		return nil, nil, err
	}

	if image.Status.State != api.MachineImageReady {
		return nil, nil, fmt.Errorf("Machine image state: %s", image.Status.State)
	}

	publicKeys, err := ctrl.getPublicKeys(machine)
	if err != nil {
		return nil, nil, err
	}

	machine2 = machine
	pods, err := ctrl.podLister.Pods(common.NamespaceVM).List(labels.Set{
		"app":  common.LabelApp,
		"name": machine.Name,
		"role": common.LabelRoleVM,
	}.AsSelector())

	if err != nil && !apierrors.IsNotFound(err) {
		glog.V(2).Infof("Error getting machine pod(s) %s/%s: %v", common.NamespaceVM, machine.Name, err)
		return
	}

	alivePods := GetAlivePods(pods)
	switch len(alivePods) {
	case 0:
		if machine.Spec.Volume.Longhorn != nil {
			pod = ctrl.createLonghornMachinePod(machine, publicKeys, false)
		} else {
			pod = ctrl.createMachinePod(machine, publicKeys, image, false)
		}
		pod, err = ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).Create(pod)
		if err != nil {
			glog.V(2).Infof("Error creating machine pod %s/%s: %v", common.NamespaceVM, machine.Name, err)
			return
		}
	case 1:
		pod = alivePods[0]
	default:
		return
	}

	err = ctrl.updateMachineStatusWithPod(machine, pod)
	return
}

func (ctrl *VirtualMachineController) updateMachineStatusWithPod(machine *api.VirtualMachine, pod *corev1.Pod) error {
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(machine.Name, metav1.GetOptions{})
		toUpdate, err := ctrl.machineLister.Get(machine.Name)
		original := toUpdate.DeepCopy()
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				glog.Errorf("unknown error encountered when updating machine status with pod %v", err)
			}
		}
		if pod.Spec.NodeName != "" {
			toUpdate.Status.NodeName = pod.Spec.NodeName
		}
		if pod.Status.HostIP != "" {
			toUpdate.Status.NodeIP = pod.Status.HostIP
		}
		switch {
		case pod.DeletionTimestamp != nil:
			toUpdate.Status.State = api.StateStopping
		case common.IsPodReady(pod):
			toUpdate.Status.State = api.StateRunning
		default:
			toUpdate.Status.State = api.StatePending
		}
		if !reflect.DeepEqual(original.Status, toUpdate.Status) ||
			!reflect.DeepEqual(original.Finalizers, toUpdate.Finalizers) ||
			!reflect.DeepEqual(original.Spec, toUpdate.Spec) {
			glog.V(5).Infof("updating machine status with pod for %s", machine.Name)
			toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
			if err := ctrl.verifyMachine(toUpdate); err != nil {
				glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
			}
			return updateErr
		}
		return nil
	})

	if retryErr != nil {
		glog.Errorf("error while updating vm object while updating machine status with pod %v", retryErr)
	}
	return retryErr
}

func (ctrl *VirtualMachineController) deleteMachinePods(machine *api.VirtualMachine) error {
	machinePodSelector := labels.Set{"name": machine.Name}.AsSelector()

	pods, _ := ctrl.podLister.Pods(common.NamespaceVM).List(machinePodSelector)
	if len(pods) == 0 {
		return apierrors.NewNotFound(corev1.Resource("pods"), machine.Name+"-*")
	}
	needsDelete := false
	for _, v := range pods {
		if v.DeletionTimestamp == nil {
			needsDelete = true
		}
	}
	if needsDelete {
		return ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).DeleteCollection(
			&metav1.DeleteOptions{},
			metav1.ListOptions{LabelSelector: machinePodSelector.String()})
	}

	return nil
}

func (ctrl *VirtualMachineController) setTerminating(machine *api.VirtualMachine) error {
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(machine.Name, metav1.GetOptions{})
		toUpdate, err := ctrl.machineLister.Get(machine.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				glog.Errorf("unknown error encountered when setting terminating %v", err)
			}
		}
		if toUpdate.Status.State == api.StateTerminating {
			return nil
		}
		toUpdate.Status.State = api.StateTerminating
		glog.V(5).Infof("Setting terminating for %s", machine.Name)
		toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
		if err := ctrl.verifyMachine(toUpdate); err != nil {
			glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
		}
		return updateErr
	})

	if retryErr != nil {
		glog.Errorf("error while updating vm object while setting terminating %v", retryErr)
	}
	return retryErr
}

func (ctrl *VirtualMachineController) removeFinalizer(machine *api.VirtualMachine) error {
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(machine.Name, metav1.GetOptions{})
		toUpdate, err := ctrl.machineLister.Get(machine.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				glog.Errorf("unknown error encountered when setting terminating %v", err)
			}
		}
		if reflect.DeepEqual(toUpdate.Finalizers, []string{}) {
			return nil
		}
		toUpdate.Finalizers = []string{}
		glog.V(5).Infof("removing finalizer for %s", machine.Name)
		toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
		if err := ctrl.verifyMachine(toUpdate); err != nil {
			glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
		}
		return updateErr
	})

	if retryErr != nil {
		glog.Errorf("error while updating vm object while setting terminating %v", retryErr)
	}
	return retryErr
}

func (ctrl *VirtualMachineController) verifyMachine(machine *api.VirtualMachine) error {
	var err error
	glog.V(5).Infof("Verifying machine %s", machine.Name)
	for i := 0; i < 25; i++ {
		var fromCache *api.VirtualMachine
		fromCache, err = ctrl.machineLister.Get(machine.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				break
			}
			glog.Error(err)
			return err
		}
		if resourceVersionAtLeast(fromCache.ResourceVersion, machine.ResourceVersion) {
			glog.V(5).Infof("resource version matched for %s", machine.Name)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	glog.Errorf("resource version didn't match for in time%s", machine.Name)
	return nil

}

// borrowed from longhorn
func resourceVersionAtLeast(curr, min string) bool {
	// skip unit testing code
	if curr == "" || min == "" {
		return true
	}
	currVersion, err := strconv.ParseInt(curr, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse current resource version %v: %v", curr, err)
		return false
	}
	minVersion, err := strconv.ParseInt(min, 10, 64)
	if err != nil {
		logrus.Errorf("datastore: failed to parse minimal resource version %v: %v", min, err)
		return false
	}
	return currVersion >= minVersion
}