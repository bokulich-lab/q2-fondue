#!/usr/bin/env bash

TOOLKIT_VER="3.0.0"

if [[ "$OSTYPE" == "linux"* ]]; then
  LINUX_VER=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
  if [[ "$LINUX_VER" == '"Ubuntu"' || "$LINUX_VER" == '"Debian GNU/Linux"' ]]; then
    OS_VER="ubuntu64"
  elif [[ "$LINUX_VER" == '"CentOS Linux"' ]]; then
    OS_VER="centos_linux64"
  else
    echo "Detected OS version (${LINUX_VER}) is not supported. Aborting."
    exit 1
  fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
  OS_VER="mac64"
else
  echo "Detected OS version (${OSTYPE}) is not supported. Aborting."
  exit 1
fi

TOOLKIT_URL="http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/${TOOLKIT_VER}/sratoolkit.${TOOLKIT_VER}-${OS_VER}.tar.gz"

echo "Fetching SRA Tools from ${TOOLKIT_URL}..."
curl -L "${TOOLKIT_URL}" > sratoolkit.tar.gz

echo "Extracting..."
tar -xzf sratoolkit.tar.gz
rm sratoolkit.tar.gz
mv "sratoolkit.${TOOLKIT_VER}-${OS_VER}/" "sratoolkit/"

if [[ "$PREFIX" == "" ]]; then
  echo "Setting PREFIX=$CONDA_PREFIX"
  PREFIX="$CONDA_PREFIX"
fi

echo "Installing SRA Tools in $PREFIX..."
if [[ ! -d "$PREFIX/bin/" ]]; then
  mkdir $PREFIX/bin/
fi
find sratoolkit/bin/ -maxdepth 1 -type f -exec mv -f {} $PREFIX/bin/ \;
find sratoolkit/bin/ -maxdepth 1 -type l -exec mv -f {} $PREFIX/bin/ \;
rm -r sratoolkit

echo "Testing installation..."
if [[ $(which prefetch) == "$PREFIX/bin"* ]]; then
  echo "Success!"
else
  echo "Installation failed."
  exit 1
fi

echo "Configuring SRA Toolkit:"
SRA_CACHE_LOC="$HOME/.prefetch_cache"
echo "Creating prefetch cache directory under $SRA_CACHE_LOC..."
mkdir "$SRA_CACHE_LOC"
echo "Running vdb-config..."
vdb-config -s "/repository/user/main/public/root=$SRA_CACHE_LOC"
vdb-config --prefetch-to-user-repo
echo "Configuration completed."
