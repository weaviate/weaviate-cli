#!/bin/bash

# Set the distro as variable
UNAME=$(uname | tr "[:upper:]" "[:lower:]")
WEAVIATE_DIR="/bin/weaviate-cli"
WEAVIATE_ZIP="weaviate-cli.zip"
NEED_TO_INSTALL=" " # Keep the space, need for validation

# If Linux, try to determine specific distribution
if [ "$UNAME" == "linux" ]; then

    # If available, use LSB to identify distribution
    if [ -f /etc/lsb-release -o -d /etc/lsb-release.d ]; then
        export DISTRO=$(lsb_release -i | cut -d: -f2 | sed s/'^\t'//)

    # Otherwise, use release info file
    else
        export DISTRO=$(ls -d /etc/[A-Za-z]*[_-][rv]e[lr]* | grep -v "lsb" | cut -d'/' -f3 | cut -d'-' -f1 | cut -d'_' -f1)
    fi
fi
[ "$DISTRO" == "" ] && export DISTRO=$UNAME

# Validate if python version 3 is installed
if command -v python3 &>/dev/null; then
    echo "Python 3 is installed. Continue"
else
    echo "No Python3 found, try to install"
    NEED_TO_INSTALL="$NEED_TO_INSTALL python3"
fi

# Validate if pip3 is installed
if command -v pip3 &>/dev/null; then
    echo "PIP3 is installed. Continue"
else
    echo "No PIP3 found, try to install"
    NEED_TO_INSTALL="$NEED_TO_INSTALL python3-pip"
fi

# Validate if wget is installed
if command -v wget &>/dev/null; then
    echo "wget is installed. Continue"
else
    echo "No wget found, try to install"
    NEED_TO_INSTALL="$NEED_TO_INSTALL wget"
fi


# Validate if unzip is installed
if command -v unzip &>/dev/null; then
    echo "unzip is installed. Continue"
else
    echo "No unzip found, try to install"
    NEED_TO_INSTALL="$NEED_TO_INSTALL unzip"
fi

# Do the actuall install
if [ "$NEED_TO_INSTALL" == " " ]; then
    echo "All depenencies are available, continue the installation..."
else
    echo "Not all depenencies are available, try to install..."
    if [ $DISTRO == "darwin" ]; then
        brew install $NEED_TO_INSTALL
    elif [ $DISTRO == "debian os" ]; then
        sudo apt-get -qq update
        sudo apt-get -qq -y install $NEED_TO_INSTALL
    elif [ $DISTRO == "Ubuntu" ]; then
        sudo apt-get update
        sudo apt-get -qq -y install $NEED_TO_INSTALL
    else
        echo "We don't have any helpers for: $DISTRO"
        echo "Please raise an issue at: https://github.com/creativesoftwarefdn/weaviate-cli/issues"
        echo "Or manually install. Docs available at: https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/use/weaviate-cli-tool.md"
        exit 1
    fi
fi

# Create the directory and set access
sudo mkdir -p $WEAVIATE_DIR
sudo chmod -R 755 $WEAVIATE_DIR

# empty the dir
sudo rm -rf $WEAVIATE_DIR/*

# download the library
sudo wget -q -O $WEAVIATE_DIR/$WEAVIATE_ZIP https://github.com/creativesoftwarefdn/weaviate-cli/archive/master.zip

# unzip and move
sudo unzip $WEAVIATE_DIR/$WEAVIATE_ZIP -d $WEAVIATE_DIR
sudo mv $WEAVIATE_DIR/weaviate-cli-master/* $WEAVIATE_DIR
sudo rm -r $WEAVIATE_DIR/weaviate-cli-master/

# Install PIP stuff
CURRENT_DIR=$(pwd)
cd $WEAVIATE_DIR/
pip3 install -r requirements.txt
cd $CURRENT_DIR

# Create alias
alias weaviate-cli="python3 $WEAVIATE_DIR/weaviate.py"
echo alias weaviate-cli=\"python3 $WEAVIATE_DIR/weaviate.py\" >> ~/.bashrc
source ~/.bashrc

# unset vars
unset WEAVIATE_DIR
unset WEAVIATE_ZIP
unset CURRENT_DIR

# done!
echo "Done installing weaviate-cli tool. Run with: $ weaviate-cli --help"