#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function
__metaclass__ = type

DOCUMENTATION = r'''
---
module: progress_get_url
short_description: Download a file and return timing; progress is handled by the action plugin
version_added: "1.0.0"
description:
  - Downloads a file from a URL to a destination path. Visible progress output is implemented in the action plugin of the same name.
options:
  url:
    description: The URL to download.
    required: true
    type: str
  dest:
    description: Destination file path.
    required: true
    type: str
  mode:
    description: File mode to set on the downloaded file.
    required: false
    type: str
  force:
    description: Overwrite file if it exists.
    required: false
    type: bool
    default: false
  timeout:
    description: Timeout in seconds for the HTTP request.
    required: false
    type: int
    default: 1800
  headers:
    description: Optional HTTP headers to send.
    required: false
    type: dict
  validate_certs:
    description: Validate TLS certificates.
    required: false
    type: bool
    default: true
author:
  - GitHub Copilot
'''

EXAMPLES = r'''
- name: Download with progress
  progress_get_url:
    url: https://example.com/big.iso
    dest: /tmp/big.iso
    mode: '0644'
'''

RETURN = r'''
changed:
  description: Whether a download occurred.
  returned: always
  type: bool
elapsed_seconds:
  description: Elapsed time in seconds for the download.
  returned: success
  type: float
size:
  description: Total bytes downloaded.
  returned: success
  type: int
checksum:
  description: SHA256 checksum of downloaded file.
  returned: success
  type: str
'''

import os
import time
import hashlib
from ansible.module_utils.basic import AnsibleModule

try:
    import requests
except Exception:
    requests = None


def main():
    module = AnsibleModule(
        argument_spec=dict(
            url=dict(type='str', required=True),
            dest=dict(type='str', required=True),
            mode=dict(type='str', required=False),
            force=dict(type='bool', default=False),
            timeout=dict(type='int', default=1800),
            headers=dict(type='dict', required=False, default=None),
            validate_certs=dict(type='bool', default=True),
        ),
        supports_check_mode=False,
    )

    if requests is None:
        module.fail_json(msg='The requests Python package is required. Please install python3-requests.')

    url = module.params['url']
    dest = module.params['dest']
    mode = module.params.get('mode')
    force = module.params['force']
    timeout = module.params['timeout']
    headers = module.params.get('headers') or {}
    headers.setdefault('User-Agent', 'ansible-progress-get-url/1.0 (+https://ansible.com)')
    validate_certs = module.params['validate_certs']

    dest_abs = os.path.abspath(os.path.expanduser(dest))

    # If file exists and not forcing, do nothing
    if os.path.exists(dest_abs) and not force:
        try:
            size = os.path.getsize(dest_abs)
        except Exception:
            size = 0
        module.exit_json(changed=False, elapsed_seconds=0.0, size=size, checksum='')

    # Ensure directory exists
    os.makedirs(os.path.dirname(dest_abs), exist_ok=True)

    start = time.time()
    sha256 = hashlib.sha256()
    bytes_written = 0

    try:
        with requests.get(url, headers=headers, stream=True, timeout=timeout, verify=validate_certs) as r:
            r.raise_for_status()
            chunk_size = 1024 * 1024  # 1 MiB
            with open(dest_abs, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    f.write(chunk)
                    sha256.update(chunk)
                    bytes_written += len(chunk)
    except Exception as e:
        try:
            if os.path.exists(dest_abs):
                os.remove(dest_abs)
        except Exception:
            pass
        module.fail_json(msg=f'download failed: {e}')

    elapsed = time.time() - start
    if mode:
        try:
            os.chmod(dest_abs, int(mode, 8))
        except Exception:
            pass

    module.exit_json(changed=True, elapsed_seconds=elapsed, size=bytes_written, checksum=sha256.hexdigest())


if __name__ == '__main__':
    main()

