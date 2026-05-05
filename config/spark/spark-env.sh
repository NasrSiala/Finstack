# Python configuration
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Logging — use bitnami paths that match docker-compose volume mounts
export SPARK_LOG_DIR=/opt/bitnami/spark/logs
export SPARK_WORKER_DIR=/opt/bitnami/spark/work

# History server
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/opt/bitnami/spark/logs"
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
# Load all JARs in the spark/jars directory into the JVM before PySpark starts
export SPARK_DIST_CLASSPATH="/opt/bitnami/spark/jars/*"
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
