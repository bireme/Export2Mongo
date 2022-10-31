#!/bin/bash

cd /home/javaapps/sbt-projects/DCDup || exit

sbt "runMain e2m.Isis2Mongo $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10} ${11} ${12} ${13}"
ret="$?"

cd - || exit

exit $ret

