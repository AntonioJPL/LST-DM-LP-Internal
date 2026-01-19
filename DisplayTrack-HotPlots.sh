#!/bin/bash
CONDA_INIT_SCRIPT="/opt/lst-drive/conda/conda_init.sh"
source "$CONDA_INIT_SCRIPT"
conda activate drive-mon

LOGPATH="/fefs/onsite/monitoring/driveLST1"
LOGPATH2="/fefs/onsite/monitoring/auxLST1"

#splits the date entered as argument
an2=`echo $1 | cut -c 1,2`
an=`echo $1 | cut -c 3,4`
mo=`echo $1 | cut -c 6,7`
jo=`echo $1 | cut -c 9,10`
#Deletes any existing file named cmd.date
rm -f cmd.${an2}${an}-${mo}-${jo}
file='none'
target_file="${LOGPATH}/LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo}"
#Checks if the target_file exists and in case it does not exists it assigns the NewDocker path to the file variable
if [ -e "$target_file" ]; then
  file="$target_file"
else
  file="${LOGPATH}/LSTDriveLog/NewDockerCurrentsentinelleOPCUA.log"
  if [ ! -e "$file" ]; then
    jo=$((10#$jo - 1))
    file="${LOGPATH}/LSTDriveLog/V3_DockerCurrentsentinelleOPCUA.log.${an2}${an}${mo}${jo}"
  fi
fi
#Goes through the file and appends the found lines containing the text given on the grep command
grep "Start Tracking RA" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep " GoToPosition Zd=" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "command sent" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "action error" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "in progress" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "Done" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "Track start time" "$file" >> cmd.${an2}${an}-${mo}-${jo}
grep "Drive Regulation Parameters" "$file" >> cmd.${an2}${an}-${mo}-${jo}

#Creates a link between 2 files
ln -s ${LOGPATH2}/LoadPinLog/loadpin_log_${an}_${mo}_${jo}.txt loadpin_log_${an}_${mo}_${jo}.txt 
#Deletes the file
rm -f R_loadpin_log_${an}_${mo}_${jo}.txt
#No idea how the for loop works but it checks the cab value and appends the result to the R_loadpin file
for cab in 107 207 ; do
   grep " ${cab} " loadpin_log_${an}_${mo}_${jo}.txt >> "R_loadpin_log_${an}_${mo}_${jo}.txt"
done
#create several links between files and the current folder
ln -s ${LOGPATH}/DrivePositioning/DrivePosition_log_${an2}${an}${mo}${jo}.txt .
ln -s ${LOGPATH}/DrivePositioning/Accuracy_log_${an2}${an}${mo}${jo}.txt .
ln -s ${LOGPATH}/DrivePositioning/Track_log_${an2}${an}${mo}${jo}.txt .
ln -s ${LOGPATH}/DrivePositioning/Torque_log_${an2}${an}${mo}${jo}.txt .
ln -s ${LOGPATH}/DrivePositioning/BendingModelCorrection_log_${an2}${an}${mo}${jo}.txt .
#Executes the Python script passing the filenames values
python3 DisplayTrack-HotPlots.py cmd.${an2}${an}-${mo}-${jo} DrivePosition_log_${an2}${an}${mo}${jo}.txt R_loadpin_log_${an}_${mo}_${jo}.txt Track_log_${an2}${an}${mo}${jo}.txt Torque_log_${an2}${an}${mo}${jo}.txt ${an2}${an}-${mo}-${jo}
#Delete all the created files for the script
rm -f loadpin_log_${an}_${mo}_${jo}.txt cmd.${an2}${an}-${mo}-${jo} R_loadpin_log_${an}_${mo}_${jo}.txt DrivePosition_log_${an2}${an}${mo}${jo}.txt Accuracy_log_${an2}${an}${mo}${jo}.txt R_loadpin_log_${an2}${an}${mo}${jo}.txt Track_log_${an2}${an}${mo}${jo}.txt Torque_log_${an2}${an}${mo}${jo}.txt BendingModelCorrection_log_${an2}${an}${mo}${jo}.txt 
