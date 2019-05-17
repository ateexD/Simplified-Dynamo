for i in {1..10}
do
    ./simpledynamo-grading.osx -p 6 ./SimpleDynamo/app/build/outputs/apk/debug/app-debug.apk | tail -n 1
done

#for i in "5554" "5556" "5558" "5560" "5562"
#do
#    adb -s "emulator-"$i  uninstall edu.buffalo.cse.cse486586.simpledynamo
#done
