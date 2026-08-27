#!/bin/bash
set -ex
spark_versions=(
  3.5.0 3.5.1 3.5.2 3.5.3 3.5.4 3.5.6 3.5.7 3.5.8 3.5.9
  4.0.0 4.0.1 4.0.2 4.0.3 4.0.4
  4.1.0 4.1.2 4.1.3
  4.2.0
)
# sbt doesn't read the JAVA_HOME env variable.
SBT_EXTRA=""
if [ ! -z "$JAVA_HOME" ]; then
  SBT_EXTRA="-java-home $JAVA_HOME"
fi
# Avoid m2 race conditions which "shouldn't" happen but do by running update in serial.
for spark_version in "${spark_versions[@]}"
do
  echo "Updating $spark_version"
  sbt $SBT_EXTRA -DsparkVersion=$spark_version +update
done
for spark_version in "${spark_versions[@]}"
    do
      echo "Building $spark_version"
      build_dir="/tmp/spark-testing-base-$spark_version-magic"
      rm -rf "${build_dir}"
      mkdir -p "${build_dir}"
      cp -af ./ "${build_dir}"
      cd ${build_dir}
      sleep 2
      nice -n 10 sbt  $SBT_EXTRA -DsparkVersion=$spark_version version clean +publishSigned sonaUpload sonaRelease &
      cd -
done
wait
echo $?
