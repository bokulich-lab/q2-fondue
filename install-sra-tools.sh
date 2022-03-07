#!/usr/bin/env bash

TOOLKIT_VER="3.0.0"

if [[ "$OSTYPE" == "linux"* ]]; then
  LINUX_VER=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
  if [[ "$LINUX_VER" == 'Ubuntu"' ]]; then
    OS_VER="ubuntu64"
  elif [[ "$LINUX_VER" == '"CentOS Linux"' ]]; then
    OS_VER="centos_linux64"
  else
    echo "Detected OS version (${LINUX_VER}) is not supported. Aborting."
    unset_vars
    exit 1
  fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_VER="mac64"
else
  echo "Detected OS version (${OSTYPE}) is not supported. Aborting."
  unset_vars
  exit 1
fi

TOOLKIT_URL="http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/${TOOLKIT_VER}/sratoolkit.${TOOLKIT_VER}-${OS_VER}.tar.gz"

echo "Fetching SRA Tools from ${TOOLKIT_URL}..."
curl -L "${TOOLKIT_URL}" > sratoolkit.tar.gz

echo "Extracting..."
tar -xzf sratoolkit.tar.gz
rm sratoolkit.tar.gz
mv "sratoolkit.${TOOLKIT_VER}-${OS_VER}/" "sratoolkit/"

echo "Installing SRA Tools in $CONDA_PREFIX..."
find sratoolkit/bin/ -type f -maxdepth 1 -exec mv -i {} $CONDA_PREFIX/bin/ \;
find sratoolkit/bin/ -type l -maxdepth 1 -exec mv -i {} $CONDA_PREFIX/bin/ \;
rm -r sratoolkit

echo "Testing installation..."
if [[ $(which prefetch) == "$CONDA_PREFIX/bin"* ]]; then
  unset_vars
  echo "Success!"
else
  echo "Installation failed."
  unset_vars
  exit 1
fi
