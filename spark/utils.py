import os
import shutil


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
