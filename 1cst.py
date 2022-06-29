import io
import logging
import logging.handlers
import os
import platform
import signal
import subprocess
import sys
import time
import traceback

import click

VERSION = "1cst 1.0.0.4"
LOG_FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"
LOG_LEVEL = logging.INFO
LOG_DIR = ""
EXCLUDED = ["BackgroundJob", "COMConnection"]


@click.command()
@click.option("--platform-path", "-P", default="", help="platform installation path")
@click.option("--cluster-user", "-u", default=None, help="cluster administrator")
@click.option("--cluster-password", "-p", default=None, help="cluster administrator password")
@click.option("--log", "-l", default=os.getcwd(), help="log directory (default is working directory)")
@click.option("--all", "-a", "terminate_all", default=False, flag_value=True,
              help="terminate all sessions (including COM connections and background jobs)")
@click.option("--verbose", "-v", "verbose", default=False, flag_value=True, help="verbose mode")
@click.option("--version", "-V", "version", default=False, flag_value=True, help="display version")
def main(platform_path, cluster_user, cluster_password, log, terminate_all, verbose, version):
    """1cst - 1C server session termination"""
    if version:
        click.echo(VERSION)
        sys.exit(0)
    if verbose:
        global LOG_LEVEL
        LOG_LEVEL = logging.DEBUG
    global LOG_DIR
    LOG_DIR = log
    start_time = time.time()
    get_logger().info("Started")
    command_line = " ".join(sys.argv)
    get_logger().debug(f"Working directory: \"{os.getcwd()}\"")
    get_logger().debug(f"Args: \"{command_line}\"")
    if os.path.isfile(os.path.join(platform_path, get_executable("ras"))):
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
            get_logger().error("Platform not found, exiting")
            sys.exit(1)
    auth = []
    if cluster_user:
        auth += [f"--cluster-user={cluster_user}"]
    if cluster_password:
        auth += [f"--cluster-pwd={cluster_password}"]
    ras = os.path.join(platform_path, get_executable("ras"))
    rac = os.path.join(platform_path, get_executable("rac"))
    get_logger().info("Starting RAS")
    ras_process = open_process([ras, "cluster"])
    time.sleep(1)
    args = [rac, "cluster", "list"]
    for cluster, host, port, name in get_clusters(get_output(open_process(args))):
        get_logger().info(f"Found cluster: [{cluster}, {host}, {port}, {name}]")
        args = [rac, "session", "list", f"--cluster={cluster}"] + auth
        for session, user, client, app in get_sessions(get_output(open_process(args))):
            if not terminate_all:
                if app in EXCLUDED:
                    get_logger().info(f"Ignoring session: [{session}, {client}, {user}, {app}]")
                    continue
            get_logger().info(f"Terminating session: [{session}, {client}, {user}, {app}]")
            args = [rac, "session", "terminate", f"--session={session}f", f"--cluster={cluster}"] + auth
            get_output(open_process(args))
    time.sleep(1)
    get_logger().info("Closing RAS")
    os.kill(ras_process.pid, signal.SIGINT)
    end_time = time.time()
    get_logger().info(f"Finished ({end_time - start_time:.2f}s.)")
    sys.exit(0)


def find_platform():
    platform_path = None
    latest_version = 0
    root = get_platform_root()
    if os.path.isdir(root):
        for directory in os.listdir(root):
            if os.path.isdir(os.path.join(root, directory)):
                version = directory.replace(".", "")
                if version.isnumeric():
                    version = int(version)
                    if version > latest_version:
                        latest_version = version
                        platform_path = os.path.join(root, directory)
    return platform_path


def get_platform_root():
    if platform.system() == "Windows":
        return os.path.join(os.environ.get("programfiles"), "1cv8")
    if platform.system() == "Linux":
        return "/opt/1cv8/x86_64"
    return os.path.dirname(__file__)


def get_executable(filename):
    if platform.system() == "Windows":
        return os.path.join("bin", filename + ".exe")
    return filename


def get_encoding():
    if platform.system() == "Windows":
        return "cp866"
    return "utf-8"


def open_process(command):
    command_line = " ".join(command)
    get_logger().debug(f"Opening process: \"{command_line}\"")
    return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def get_output(process):
    file = os.path.split(process.args[0])[1]
    output = ""
    encoding = get_encoding()
    for line in io.TextIOWrapper(process.stdout, encoding=encoding):
        line = line.strip()
        if len(line) > 0:
            output = output + line + "\n"
            get_logger().debug(f"{file}: {line}")
    return output.strip()


def get_clusters(output):
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


def get_sessions(output):
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


def get_file_handler():
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(LOG_DIR, "1cst.log"), "a", 5 * 1000 * 1000, 5)
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return file_handler


def get_stream_handler():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOG_LEVEL)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)
    if not logger.hasHandlers():
        logger.addHandler(get_file_handler())
        logger.addHandler(get_stream_handler())
    return logger


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        get_logger().debug(traceback.format_exc().strip())
        get_logger().critical(str(error))
        sys.exit(1)
