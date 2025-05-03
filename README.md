**Before you can use the redhat kubernetes installer, you have to install ansible:**

sudo dnf install -y python3 python3-pip

python3 -m venv ~/ansible-venv

source ~/ansible-venv/bin/activate

pip install --upgrade pip

pip install ansible

git clone https://github.com/mazsola2k/kubernetes-installer/

cd kubernetes-installer

**Run Ansible Installation - Standalone DEV K8S Cluster**

ansible-playbook k8s-redhat-playbook.yml




