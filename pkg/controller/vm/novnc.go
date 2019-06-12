package vm

import (
	"fmt"
	"k8s.io/client-go/util/retry"

	"github.com/golang/glog"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vmapi "github.com/rancher/vm/pkg/apis/ranchervm/v1alpha1"
	"github.com/rancher/vm/pkg/common"
)

func (ctrl *VirtualMachineController) updateNovnc(vm *vmapi.VirtualMachine, podName string) (err error) {
	if vm.Spec.HostedNovnc {
		if err = ctrl.updateNovncPod(vm, podName); err != nil {
			glog.Warningf("error updating novnc pod %s: %v", vm.Name, err)
		}
		if err = ctrl.updateNovncService(vm); err != nil {
			glog.Warningf("error updating novnc service %s: %v", vm.Name, err)
		}
	} else {
		if err = ctrl.deleteConsolePod(vm); err != nil && !apierrors.IsNotFound(err) {
			glog.Warningf("error deleting novnc pod %s: %v", vm.Name, err)
		}
		if err = ctrl.deleteConsoleService(vm); err != nil && !apierrors.IsNotFound(err) {
			glog.Warningf("error deleting novnc service %s: %v", vm.Name, err)
		}
		if err = func() error {
			retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(vm.Name, metav1.GetOptions{})
				toUpdate, err := ctrl.machineLister.Get(vm.Name)
				if err != nil {
					if apierrors.IsNotFound(err) {
						return nil
					} else {
						glog.Errorf("unknown error encountered when updating %v", err)
					}
				}
				if toUpdate.Status.VncEndpoint == "" {
					return nil
				}
				toUpdate.Status.VncEndpoint = ""
				glog.V(5).Infof("setting vnc endpoint to nothing for %s", vm.Name)
				toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
				if err := ctrl.verifyMachine(toUpdate); err != nil {
					glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
				}
				return updateErr
			})

			if retryErr != nil {
				glog.Errorf("error while updating vm object while updating %v", retryErr)
			}
			return retryErr
		}(); err != nil {
			glog.Warningf("error removing vnc endpoint from vm %s/%s: %v", common.NamespaceVM, vm.Name, err)
		}
	}
	return
}

func (ctrl *VirtualMachineController) updateNovncPod(vm *vmapi.VirtualMachine, podName string) error {
	pod, err := ctrl.podLister.Pods(common.NamespaceVM).Get(vm.Name + "-novnc")
	switch {
	case !apierrors.IsNotFound(err):
		return err
	case err == nil:
		if pod.DeletionTimestamp == nil {
			return nil
		}
		fallthrough
	default:
		pod, err = ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).Create(ctrl.makeNovncPod(vm, podName))
	}
	return err
}

func (ctrl *VirtualMachineController) updateNovncService(vm *vmapi.VirtualMachine) error {
	svc, err := ctrl.svcLister.Services(common.NamespaceVM).Get(vm.Name + "-novnc")
	switch {
	case err == nil:
		break
	case apierrors.IsNotFound(err):
		if svc, err = ctrl.kubeClient.CoreV1().Services(common.NamespaceVM).Create(makeNovncService(vm)); err != nil {
			return err
		}
	default:
		return err
	}

	switch {
	case vm.Status.NodeIP == "":
		return nil
	case len(svc.Spec.Ports) != 1:
		return nil
	case svc.Spec.Ports[0].NodePort <= 0:
		return nil
	}
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		//toUpdate, err := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Get(vm.Name, metav1.GetOptions{})
		toUpdate, err := ctrl.machineLister.Get(vm.Name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			} else {
				glog.Errorf("unknown error encountered when updating %v", err)
			}
		}
		vncEndpoint := fmt.Sprintf("%s:%d", vm.Status.NodeIP, svc.Spec.Ports[0].NodePort)
		if toUpdate.Status.VncEndpoint == vncEndpoint {
			return nil
		}
		toUpdate.Status.VncEndpoint = vncEndpoint
		glog.V(5).Infof("Setting vnc endpoint for %s", vm.Name)
		toUpdate, updateErr := ctrl.vmClient.VirtualmachineV1alpha1().VirtualMachines().Update(toUpdate)
		if err := ctrl.verifyMachine(toUpdate); err != nil {
			glog.Errorf("error while verifying machine!!! %s", toUpdate.Name)
		}
		return updateErr
	})

	if retryErr != nil {
		glog.Errorf("error while updating vm object while updating %v", retryErr)
	}
	return retryErr
}

func (ctrl *VirtualMachineController) deleteConsolePod(vm *vmapi.VirtualMachine) error {
	//pod, err := ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).Get(vm.Name+"-novnc", metav1.GetOptions{})
	pod, err := ctrl.podLister.Pods(common.NamespaceVM).Get(vm.Name + "-novnc")
	if apierrors.IsNotFound(err) {
		return err
	}
	if pod.DeletionTimestamp == nil {
		return ctrl.kubeClient.CoreV1().Pods(common.NamespaceVM).Delete(
			vm.Name+"-novnc", &metav1.DeleteOptions{})
	}
	return nil
}

func (ctrl *VirtualMachineController) deleteConsoleService(vm *vmapi.VirtualMachine) error {
	//service, err := ctrl.kubeClient.CoreV1().Services(common.NamespaceVM).Get(vm.Name+"-novnc", metav1.GetOptions{})
	service, err := ctrl.svcLister.Services(common.NamespaceVM).Get(vm.Name+"-novnc")
	if apierrors.IsNotFound(err) {
		return err
	}
	if service.DeletionTimestamp == nil {
		return ctrl.kubeClient.CoreV1().Services(common.NamespaceVM).Delete(
			vm.Name+"-novnc", &metav1.DeleteOptions{})
	}
	return nil
}
