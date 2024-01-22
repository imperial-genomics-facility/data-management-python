import os, subprocess
from typing import Tuple
from igf_data.utils.fileutils import (
    get_temp_dir,
    check_file_path)

def bash_script_wrapper(script_path: str) -> Tuple[str, str]:
    try:
        log_dir = \
            get_temp_dir(use_ephemeral_space=True)
        stderr_file = os.path.join(log_dir, 'stderr.txt')
        stdout_file = os.path.join(log_dir, 'stdout.txt')
        try:
            check_file_path(script_path)
            command = ' '.join(["bash", script_path])
            with open(stdout_file , 'w') as fout:
                with open(stderr_file, 'w') as ferr:
                    subprocess.check_call(
                        command,
                        shell=True,
                        stdout=fout,
                        stderr=ferr)
        except subprocess.CalledProcessError:
            raise ValueError(
                f"Failed to run script {script_path}. check err file {stderr_file}")
        return stdout_file, stderr_file
    except Exception as e:
        raise ValueError(
            f"Failed bash wrapper, error: {e}")