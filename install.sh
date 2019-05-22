#!/bin/bash
CURRENTDIR=$(pwd)
cd ~
git clone https://github.com/semi-technologies/weaviate-cli
echo export $PATH=$PATH:~/weaviate-cli/bin >> ~/.profile
cd $CURRENTDIR
clear
echo "Weaviate-cli installation is done - run weaviate-cli --version" &