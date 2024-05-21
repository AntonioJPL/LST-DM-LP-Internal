#splits the date entered as argument
an2=`echo $1 | cut -c 1,2`
an=`echo $1 | cut -c 3,4`
mo=`echo $1 | cut -c 6,7`
jo=`echo $1 | cut -c 9,10`
#Deletes any existing file named cmd.date
rm -f cmd.${an2}${an}-${mo}-${jo}
#Goes through the CurrentsentinelleOPCUA file and appends the found lines containing the text given on the grep command
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "Start Tracking RA" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep " GoToPosition Zd=" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "command sent" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "action error" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "in progress" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "Done" >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "Track start time"  >> cmd.${an2}${an}-${mo}-${jo}
cat ./LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo} | grep "Drive Regulation Parameters"  >> cmd.${an2}${an}-${mo}-${jo}

#Creates a link between 2 files
ln -s ./LoadPinLog/loadpin_log_${an}_${mo}_${jo}.txt loadpin_log_${an}_${mo}_${jo}.txt 
#Deletes the file
rm -f R_loadpin_log_${an}_${mo}_${jo}.txt
#No idea how the for loop works but it checks the cab value and appends the result to the R_loadpin file
for cab in 107 207 ; do
   grep " ${cab} " loadpin_log_${an}_${mo}_${jo}.txt >> "R_loadpin_log_${an}_${mo}_${jo}.txt"
done
#create several links between files and the current folder
ln -s ./DrivePositioning/DrivePosition_log_${an2}${an}${mo}${jo}.txt .
ln -s ./DrivePositioning/Accuracy_log_${an2}${an}${mo}${jo}.txt .
ln -s ./DrivePositioning/Track_log_${an2}${an}${mo}${jo}.txt .
ln -s ./DrivePositioning/Torque_log_${an2}${an}${mo}${jo}.txt .
ln -s ./DrivePositioning/BendingModelCorrection_log_${an2}${an}${mo}${jo}.txt .
#Executes the Python script passing the filenames values
python3 DisplayTrack-NoCheck.py cmd.${an2}${an}-${mo}-${jo} DrivePosition_log_${an2}${an}${mo}${jo}.txt loadpin_log_${an}_${mo}_${jo}.txt Track_log_${an2}${an}${mo}${jo}.txt Torque_log_${an2}${an}${mo}${jo}.txt ${an2}${an}-${mo}-${jo}
#Delete all the created files for the script
rm -f loadpin_log_${an}_${mo}_${jo}.txt cmd.${an2}${an}-${mo}-${jo} R_loadpin_log_${an}_${mo}_${jo}.txt DrivePosition_log_${an2}${an}${mo}${jo}.txt Accuracy_log_${an2}${an}${mo}${jo}.txt R_loadpin_log_${an2}${an}${mo}${jo}.txt Track_log_${an2}${an}${mo}${jo}.txt Torque_log_${an2}${an}${mo}${jo}.txt BendingModelCorrection_log_${an2}${an}${mo}${jo}.txt 
