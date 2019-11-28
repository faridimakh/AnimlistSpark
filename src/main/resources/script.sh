#!/bin/bash
if [ -d "$1" ] && [ -d "$2" ] && [ -d "$3" ]
then
    echo 1
else
    echo 0
fi