#!/bin/bash
CURRENTDIR=$(pwd)
cd ~
git clone https://github.com/semi-technologies/weaviate-cli
echo ~/weaviate-cli/bin >> ~/.profile
cd $CURRENTDIR
clear
echo "Weaviate-cli installation is done - open a new CLI and run weaviate-cli --version"