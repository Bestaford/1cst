import io
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import time
import traceback
from logging import Logger, Handler
from os.path import join, isfile, isdir
from subprocess import Popen

import click

NAME = "1cst"
VERSION = "1.1.0"
URL = "https://github.com/bestaford/1cst"
EXCLUDED_APPS = ["BackgroundJob", "COMConnection"]

LOG_FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"
LOG_LEVEL = logging.INFO
LOG_DIR = ""


@click.command()
@click.option("--platform-path", "-P", default="", help="platform installation path")
@click.option("--cluster-user", "-u", default=None, help="cluster administrator name")
@click.option("--cluster-password", "-p", default=None, help="cluster administrator password")
@click.option("--infobase-user", "-iu", default=None, help="infobase administrator name")
@click.option("--infobase-password", "-ip", default=None, help="infobase administrator password")
@click.option("--log", "-l", default=os.getcwd(), help="log directory (default is working directory)")
@click.option("--all", "-a", "terminate_all", default=False, flag_value=True,
              help="terminate all sessions (including COM connections and background jobs)")
@click.option("--disable-scheduled-tasks", "-d", "disable_scheduled_tasks", default=False, flag_value=True,
              help="disables scheduled tasks for all infobases found in the cluster before session termination and "
                   "enables them afterward")
@click.option("--scheduled-tasks-timeout", "-t", default=60,
              help="waiting time between disconnecting scheduled tasks and terminating sessions (in seconds)")
@click.option("--verbose", "-v", "verbose", default=False, flag_value=True, help="verbose mode")
@click.option("--version", "-V", "version", default=False, flag_value=True, help="display version")
def main(platform_path,
         cluster_user,
         cluster_password,
         infobase_user,
         infobase_password,
         log,
         terminate_all,
         disable_scheduled_tasks,
         scheduled_tasks_timeout,
         verbose,
         version) -> None:
    """1cst - 1C server session termination"""

    if version:
        click.echo(f"{NAME} {VERSION}\n{URL}")
        return

    if verbose:
        global LOG_LEVEL
        LOG_LEVEL = logging.DEBUG

    if log:
        global LOG_DIR
        LOG_DIR = log

    start_time = time.time()

    get_logger().info("Started")
    get_logger().debug(f"Working directory: \"{os.getcwd()}\"")
    command_line = " ".join(sys.argv)
    get_logger().debug(f"Args: \"{command_line}\"")

    if isfile(get_executable_path(platform_path, "ras")):
        get_logger().info(f"Platform path: {platform_path}")
    else:
        if platform_path:
            get_logger().warning("Platform path is invalid, trying to find")
        else:
            get_logger().info("Platform path not specified, trying to find")
        platform_path = find_platform()
        if platform_path:
            get_logger().info(f"Found the latest version of the platform: \"{platform_path}\"")
        else:
            raise Exception("Platform is not found")

    ras = get_executable_path(platform_path, "ras")
    if not isfile(ras):
        raise Exception(
            "RAS was not found in this platform installation, "
            "please reinstall the platform with server components enabled")

    rac = get_executable_path(platform_path, "rac")
    if not isfile(rac):
        raise Exception(
            "RAC was not found in this platform installation, "
            "please reinstall the platform with server components enabled")

    cluster_auth = []
    if cluster_user:
        cluster_auth += [f"--cluster-user={cluster_user}"]
    if cluster_password:
        cluster_auth += [f"--cluster-pwd={cluster_password}"]

    infobase_auth = []
    if infobase_user:
        infobase_auth += [f"--infobase-user={infobase_user}"]
    if infobase_password:
        infobase_auth += [f"--infobase-pwd={infobase_password}"]

    get_logger().info("Starting RAS")
    ras_process = open_process([ras, "cluster"])

    time.sleep(1)

    cmd = [rac, "cluster", "list"]
    for cluster, host, port, cluster_name in get_clusters(get_output(open_process(cmd))):
        get_logger().info(f"Found cluster: [{cluster}, {host}, {port}, {cluster_name}]")

        cmd = [rac, "infobase", "summary", "list", f"--cluster={cluster}"] + cluster_auth
        infobases = get_infobases(get_output(open_process(cmd)))

        if disable_scheduled_tasks:
            for infobase, infobase_name, descr in infobases:
                get_logger().info(f"Disabling scheduled tasks for infobase: [{infobase}, {infobase_name}, {descr}]")
                cmd = [rac, "infobase", "update", f"--infobase={infobase}", "--scheduled-jobs-deny=on",
                       f"--cluster={cluster}"] + cluster_auth + infobase_auth
                get_output(open_process(cmd))

            get_logger().info(f"Waiting for background jobs to complete ({scheduled_tasks_timeout} seconds)")
            time.sleep(scheduled_tasks_timeout)

        get_logger().info("Searching for sessions to be terminated")
        cmd = [rac, "session", "list", f"--cluster={cluster}"] + cluster_auth
        for session, user, client, app in get_sessions(get_output(open_process(cmd))):
            if (not terminate_all) and (app in EXCLUDED_APPS):
                get_logger().info(f"Ignoring session: [{session}, {client}, {user}, {app}]")
                continue
            get_logger().info(f"Terminating session: [{session}, {client}, {user}, {app}]")
            cmd = [rac, "session", "terminate", f"--session={session}f", f"--cluster={cluster}"] + cluster_auth
            get_output(open_process(cmd))

        if disable_scheduled_tasks:
            for infobase, infobase_name, descr in infobases:
                get_logger().info(f"Enabling scheduled tasks for infobase: [{infobase}, {infobase_name}, {descr}]")
                cmd = [rac, "infobase", "update", f"--infobase={infobase}", "--scheduled-jobs-deny=off",
                       f"--cluster={cluster}"] + cluster_auth + infobase_auth
                get_output(open_process(cmd))

    time.sleep(1)

    get_logger().info("Closing RAS")
    os.kill(ras_process.pid, signal.SIGINT)

    end_time = time.time()
    get_logger().info(f"Finished ({end_time - start_time:.2f}s.)")


def find_platform() -> str:
    platform_path = None
    latest_version = 0
    root = get_platform_root()
    if isdir(root):
        for directory in os.listdir(root):
            if isdir(join(root, directory)):
                version = directory.replace(".", "")
                if version.isnumeric():
                    version = int(version)
                    if version > latest_version:
                        latest_version = version
                        platform_path = join(root, directory)

    return platform_path


def get_platform_root() -> str:
    if is_windows():
        return join(os.environ.get("programfiles"), "1cv8")
    if is_linux():
        return "/opt/1cv8/x86_64"


def get_executable_path(platform_path, filename) -> str:
    if is_windows():
        return str(join(platform_path, "bin", filename + ".exe"))
    if is_linux():
        return str(join(platform_path, filename))


def get_encoding() -> str:
    if is_windows():
        return "cp866"
    if is_linux():
        return "utf-8"


def open_process(command) -> Popen:
    command_line = " ".join(command)
    get_logger().debug(f"Opening process: \"{command_line}\"")

    return Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def get_output(process) -> str:
    file = os.path.split(process.args[0])[1]
    output = ""
    encoding = get_encoding()

    for line in io.TextIOWrapper(process.stdout, encoding=encoding):
        line = line.strip()
        if len(line) > 0:
            output = output + line + "\n"
            get_logger().debug(f"{file}: {line}")

    return output.strip()


def get_clusters(output) -> list:
    clusters = []
    cluster = ""
    host = ""
    port = ""

    for line in output.splitlines():
        if ":" in line:
            line = line.split(":")
            parameter = line[0].strip()
            value = line[1].strip()
            if parameter == "cluster":
                cluster = value
            if parameter == "host":
                host = value
            if parameter == "port":
                port = value
            if parameter == "name":
                name = value
                clusters.append((cluster, host, port, name))

    return clusters


def get_sessions(output) -> list:
    sessions = []
    session = ""
    user = ""
    client = ""

    for line in output.splitlines():
        if ":" in line:
            line = line.split(":")
            parameter = line[0].strip()
            value = line[1].strip()
            if parameter == "session":
                session = value
            if parameter == "user-name":
                user = value
            if parameter == "host":
                client = value
            if parameter == "app-id":
                app = value
                sessions.append((session, user, client, app))

    return sessions


def get_infobases(output) -> list:
    infobases = []
    infobase = ""
    name = ""

    for line in output.splitlines():
        if ":" in line:
            line = line.split(":")
            parameter = line[0].strip()
            value = line[1].strip()
            if parameter == "infobase":
                infobase = value
            if parameter == "name":
                name = value
            if parameter == "descr":
                descr = value
                infobases.append((infobase, name, descr))

    return infobases


def get_file_handler() -> Handler:
    file_handler = logging.handlers.RotatingFileHandler(join(LOG_DIR, f"{NAME}.log"), "a", 5 * 1000 * 1000, 5)
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    return file_handler


def get_stream_handler() -> Handler:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOG_LEVEL)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    return stream_handler


def get_logger() -> Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)

    if not logger.hasHandlers():
        logger.addHandler(get_file_handler())
        logger.addHandler(get_stream_handler())

    return logger


def is_windows() -> bool:
    return sys.platform == "win32"


def is_linux() -> bool:
    return sys.platform == "linux"


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        get_logger().debug(traceback.format_exc().strip())
        get_logger().critical(str(error))
        sys.exit(1)
