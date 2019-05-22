#!/bin/bash
CURRENTDIR=$(pwd)
cd ~
git clone https://github.com/semi-technologies/weaviate-cli
echo 'export $PATH=$PATH:'$(pwd)'/weaviate-cli/bin' >> ~/.profile
echo 'export $PATH=$PATH:'$(pwd)'/weaviate-cli/bin' >> ~/.bash_profile
cd $CURRENTDIR
clear
echo "Weaviate-cli installation is done."
echo "Restart your CLI."
echo "run: weaviate-cli --version"