import logging
import subprocess
from datetime import datetime

import json
import yaml

from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import mask_secrets

logger = logging.getLogger(__name__)


def write_fio_on_pod(pod_obj, file_size):
    """
    Writes IO of file_size size to a pod

    Args:
        pod_obj: pod object to write IO
        file_size: the size of the IO to be written opn pod

    """
    file_name = pod_obj.name
    logger.info(f"Starting IO on the POD {pod_obj.name}")
    now = datetime.now()
    # Going to run only write IO to write to PVC file size data before creating a clone
    pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

    # Wait for fio to finish
    fio_result = pod_obj.get_fio_results(timeout=3600)
    err_count = fio_result.get("jobs")[0].get("error")
    assert err_count == 0, f"IO error on pod {pod_obj.name}. FIO result: {fio_result}."
    logger.info("IO on the PVC Finished")
    later = datetime.now()
    diff = int((later - now).total_seconds() / 60)
    logger.info(f"Writing of {file_size} took {diff} mins")

    # Verify presence of the file on pvc
    file_path = pod.get_file_path(pod_obj, file_name)
    logger.info(f"Actual file path on the pod is {file_path}.")
    assert pod.check_file_existence(
        pod_obj, file_path
    ), f"File {file_name} does not exist"
    logger.info(f"File {file_name} exists in {pod_obj.name}.")


def run_command(
    cmd, timeout=600, out_format="string", secrets=None, ignore_error=False, **kwargs
):
    """
    Running command on the OS and return the STDOUT & STDERR outputs
    in case of argument is not string or list, return error message

    Args:
        cmd (str/list): the command to execute
        timeout (int): the command timeout in seconds, default is 10 Min.
        out_format (str): in which format to return the output: string / list
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.
        kwargs (dict): dictionary of argument as subprocess get

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        (out_format) the return format is depend on the out_format argument:
        str: all STDOUT and STDERR output as one string separated by NewLine
        list: all STDOUT and STDERR output as list of lines
        yaml: all data from STDOUT as YAML object
        json: all data from STDOUT as JSON object
        exit_code: only the command exit code will be return
        cpobj: A CompletedProcess object of the command that was executed
        CompletedProcess attributes:
        args: The list or str args passed to run().
        returncode (str): The exit code of the process, negative for signals.
        stdout     (str): The standard output (None if not captured).
        stderr     (str): The standard error (None if not captured).

    """

    masked_cmd = mask_secrets(cmd, secrets)
    logger.info(f"Executing command: {masked_cmd}")
    if isinstance(cmd, str):
        command = cmd.split()
    elif isinstance(cmd, list):
        command = cmd
    else:
        return "Error in command"

    for key in ["stdout", "stderr", "stdin"]:
        kwargs[key] = subprocess.PIPE

    if "out_format" in kwargs:
        out_format = kwargs["out_format"]
        del kwargs["out_format"]

    logger.info(f"Formatting output as {out_format}")
    cp = subprocess.run(command, timeout=timeout, **kwargs)
    output = cp.stdout.decode()
    err = cp.stderr.decode()

    masked_stdout = mask_secrets(output, secrets)
    if len(output) > 0:
        logger.debug(f"Command stdout: {masked_stdout}")
    else:
        logger.debug("Command stdout is empty")

    masked_stderr = mask_secrets(err, secrets)
    if len(err) > 0:
        logger.warning(f"Command stderr: {masked_stderr}")
    else:
        logger.debug("Command stderr is empty")

    logger.debug(f"Command return code: {cp.returncode}")

    # exit code is not zero
    if cp.returncode:
        logger.error(f"Command finished with non zero ({cp.returncode}): {err}")
        output += f"Error in command ({cp.returncode}): {err}"
        if not ignore_error:
            raise CommandFailed(
                f"Error during execution of command: {masked_cmd}."
                f"\nError is {masked_stderr}"
            )
        else:
            if out_format == "cpobj":
                output = cp
    else:
        if out_format == "exit_code":
            output = cp.returncode

        if out_format == "cpobj":
            output = cp

        if out_format == "list":
            output = output.split("\n")  # convert output to list
            output.pop()  # remove last empty element from the list

        if out_format == "json":
            output = json.loads(output)

        if out_format == "yaml":
            output = yaml.safe_load(output)

    return output
