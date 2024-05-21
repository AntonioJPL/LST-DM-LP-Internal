from LibDisplayTrackHotPlots import *

#Prints an array of document names being 0 the Python script name and the others are created files from the bash script
print(sys.argv)
if len(sys.argv) <6:
    sys.exit()

f1 = sys.argv[1]
f2 = sys.argv[2]
f3 = sys.argv[3]
f4 = sys.argv[4]
f5 = sys.argv[5]
f6 = sys.argv[6]
getAllDate(f1,f2,f3,f4,f5,f6,0)

