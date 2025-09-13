from __future__ import absolute_import, division, print_function
__metaclass__ = type

import os
import sys
import time
import hashlib

from ansible.plugins.action import ActionBase

try:
    import requests
except Exception:
    requests = None


class ActionModule(ActionBase):
    TRANSFERS_FILES = False

    def _human_size(self, n):
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        i = 0
        f = float(n)
        while f >= 1024 and i < len(units) - 1:
            f /= 1024.0
            i += 1
        return f"{f:.2f} {units[i]}"

    def _abspath(self, p, task_vars=None):
        if not p:
            return p
        p = os.path.expanduser(p)
        if os.path.isabs(p):
            return p
        # Prefer the playbook_dir from task vars, then controller CWD, else loader basedir
        if task_vars and 'playbook_dir' in task_vars:
            basedir = task_vars['playbook_dir']
            return os.path.abspath(os.path.join(basedir, p))
        # next try the current working directory
        try:
            cwd = os.getcwd()
            if cwd:
                return os.path.abspath(os.path.join(cwd, p))
        except Exception:
            pass
        basedir = getattr(self._loader, 'get_basedir', lambda: None)()
        if basedir:
            return os.path.abspath(os.path.join(basedir, p))
        return os.path.abspath(p)

    def _progress_line(self, msg):
        try:
            # Try to update the same line
            sys.stdout.write('\r' + msg)
            sys.stdout.flush()
        except Exception:
            # Fallback to standard display
            self._display.display(msg, log=False)

    def _progress_newline(self):
        try:
            sys.stdout.write('\n')
            sys.stdout.flush()
        except Exception:
            pass

    def run(self, tmp=None, task_vars=None):
        if task_vars is None:
            task_vars = {}

        args = self._task.args.copy()
        url = args.get('url')
        dest = args.get('dest')
        mode = args.get('mode')
        force = bool(args.get('force', False))
        timeout = int(args.get('timeout', 1800))
        headers = args.get('headers') or {}
        headers.setdefault('User-Agent', 'ansible-progress-get-url/1.0 (+https://ansible.com)')
        validate_certs = bool(args.get('validate_certs', True))

        if not url or not dest:
            return dict(failed=True, msg='url and dest are required')

        dest_abs = self._abspath(dest, task_vars=task_vars)

        # If requests is not available on the controller, fall back to executing the module
        if requests is None:
            return self._execute_module(module_name='progress_get_url', module_args=args, task_vars=task_vars, tmp=tmp)

        # Idempotency: skip if exists and not forcing
        if os.path.exists(dest_abs) and not force:
            try:
                size = os.path.getsize(dest_abs)
            except Exception:
                size = 0
            return dict(changed=False, elapsed_seconds=0.0, size=size, checksum='')

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(dest_abs) or '.', exist_ok=True)

        start = time.time()
        sha256 = hashlib.sha256()
        bytes_written = 0

        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout, verify=validate_certs) as r:
                r.raise_for_status()
                total = int(r.headers.get('content-length', '0')) if r.headers.get('content-length') else 0
                chunk_size = 1024 * 1024  # 1 MiB
                last_emit = 0
                with open(dest_abs, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        sha256.update(chunk)
                        bytes_written += len(chunk)

                        now = time.time()
                        if now - last_emit >= 0.2:
                            last_emit = now
                            elapsed = now - start
                            speed = bytes_written / elapsed if elapsed > 0 else 0
                            if total > 0:
                                pct = bytes_written / total * 100.0
                                msg = f"Downloading: {pct:6.2f}%  {self._human_size(bytes_written)}/{self._human_size(total)}  {self._human_size(speed)}/s  elapsed {elapsed:6.1f}s"
                            else:
                                msg = f"Downloading: {self._human_size(bytes_written)}  {self._human_size(speed)}/s  elapsed {elapsed:6.1f}s"
                            self._progress_line(msg)
                # final line
                self._progress_newline()
        except Exception as e:
            try:
                if os.path.exists(dest_abs):
                    os.remove(dest_abs)
            except Exception:
                pass
            return dict(failed=True, msg=f'download failed: {e}')

        elapsed = time.time() - start

        if mode:
            try:
                os.chmod(dest_abs, int(str(mode), 8))
            except Exception:
                # non-fatal
                pass

        return dict(changed=True, elapsed_seconds=elapsed, size=bytes_written, checksum=sha256.hexdigest())
