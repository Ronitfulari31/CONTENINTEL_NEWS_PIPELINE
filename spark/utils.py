import os
import shutil
import socket


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def configure_windows_environment(project_root: str) -> None:
    local_hadoop = os.path.join(project_root, ".hadoop")
    local_bin = os.path.join(local_hadoop, "bin")
    os.environ["HADOOP_HOME"] = local_hadoop
    os.environ["PATH"] = local_bin + os.pathsep + os.environ.get("PATH", "")

    java_home = os.environ.get("JAVA_HOME")
    java_executable = (
        os.path.join(java_home, "bin", "java.exe") if java_home else None
    )
    if java_home and os.path.isfile(java_executable):
        return

    java_bin = shutil.which("java")
    if java_bin:
        jdk_home = os.path.dirname(os.path.dirname(os.path.realpath(java_bin)))
        if os.path.isfile(os.path.join(jdk_home, "bin", "java.exe")):
            os.environ["JAVA_HOME"] = jdk_home


def configure_spark_home() -> None:
    spark_home = os.environ.get("SPARK_HOME")
    spark_submit = (
        os.path.join(spark_home, "bin", "spark-submit.cmd") if spark_home else None
    )
    if spark_home and os.path.isfile(spark_submit):
        return

    import pyspark

    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)


def setup_hadoop_env(project_root: str | None = None) -> str:
    """Compatibility helper used by Spark jobs to initialize Windows/Spark env."""
    if project_root is None:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    configure_windows_environment(project_root)
    configure_spark_home()
    return project_root
