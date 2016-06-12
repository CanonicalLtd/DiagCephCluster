x=`cat /proc/cpuinfo | grep lm`
if [ -z "${x}" ]; then     
    echo "32bit"; 
else echo "64bit";
fi
