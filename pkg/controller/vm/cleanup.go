package vm

import (
	"fmt"
	api "github.com/rancher/vm/pkg/apis/ranchervm/v1alpha1"
	"github.com/rancher/vm/pkg/common"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewCleanupJob(vm *api.VirtualMachine, imagePullSecrets []corev1.LocalObjectReference) *batchv1.Job {
	jobName :=  GetJobName(vm, "cleanup")
	objectMeta := metav1.ObjectMeta{
		Name: jobName,
		Labels: map[string]string{
			"app": common.LabelApp,
		},
	}
	return &batchv1.Job{
		ObjectMeta: objectMeta,
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						common.MakeVolHostPath("vm-files", fmt.Sprintf("%s/%s", common.HostStateBaseDir, vm.Name)),
					},
					Containers: []corev1.Container{
						corev1.Container{
							Name:            common.LabelRoleCleanup,
							Image:           *common.ImageVM,
							ImagePullPolicy: corev1.PullAlways,
							Command:         []string{"sh", "-c"},
							Args:            []string{fmt.Sprintf("rm -rf /vm/*")},
							VolumeMounts: []corev1.VolumeMount{
								common.MakeVolumeMount("vm-files", "/vm", "", false),
							},
						},
					},
					RestartPolicy:    corev1.RestartPolicyNever,
					NodeName:         vm.Status.NodeName,
					ImagePullSecrets: imagePullSecrets,
				},
			},
		},
	}
}

func (ctrl *VirtualMachineController) createCleanupJob(machine *api.VirtualMachine) (error) {
	_, err := ctrl.jobLister.Jobs(common.NamespaceVM).Get(GetJobName(machine, "cleanup"))

	switch {
	case err == nil:
		return nil

	case apierrors.IsNotFound(err):
		job := NewCleanupJob(machine, ctrl.getImagePullSecrets())
		job, err = ctrl.kubeClient.BatchV1().Jobs(common.NamespaceVM).Create(job)
		return err
	}

	return fmt.Errorf("error getting job from lister for machine %s: %v", machine.Name, err)
}

func (ctrl *VirtualMachineController) checkCleanupJob(machine *api.VirtualMachine) (bool, error) {
	job, err := ctrl.jobLister.Jobs(common.NamespaceVM).Get(GetJobName(machine, "cleanup"))

	switch {
	case err == nil:
		return job.Status.Succeeded == 1, nil

	case apierrors.IsNotFound(err):
		return false, err
	}

	return false, fmt.Errorf("error getting job from lister for machine %s: %v", machine.Name, err)

}

func (ctrl *VirtualMachineController) deleteCleanupJob(machine *api.VirtualMachine) error {
	err := ctrl.kubeClient.BatchV1().Jobs(common.NamespaceVM).Delete(GetJobName(machine, "cleanup"), &metav1.DeleteOptions{
		PropagationPolicy: &fg,
	})
	return err
}