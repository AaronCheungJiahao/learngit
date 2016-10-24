#!/bin/bash

a=$(ls $1 | awk -F '_' '{print $7}')
echo "$a" | awk '{s[$1]+=1}END{for(i in s){print i,s[i]}}'



#ls output15new | awk -F '_' '{print $7}' | awk '{s[$1]+=1}END{for(i in s){print i,s[i]}}'
