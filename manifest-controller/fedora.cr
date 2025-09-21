apiVersion: kubevirt.io/v1
kind: VirtualMachine
metadata:
  name: fedora-vm
  namespace: default
spec:
  running: true
  template:
    metadata:
      labels:
        kubevirt.io/domain: fedora-vm
    spec:
      domain:
        devices:
          disks:
            - name: containerdisk
              disk:
                bus: virtio
            - name: cloudinitdisk
              disk:
                bus: virtio
        resources:
          requests:
            memory: 1Gi
      volumes:
        - name: containerdisk
          containerDisk:
            image: quay.io/containerdisks/fedora:38
        - name: cloudinitdisk
          cloudInitNoCloud:
            userData: |
              #cloud-config
              password: fedora
              chpasswd: { expire: False }
              ssh_pwauth: true
              disable_root: false

