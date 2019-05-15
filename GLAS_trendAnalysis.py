# Created: 5/7/2019
# Purpose: To get growth rates from stack's zonal stats
# PROCESS:
# given an input csv, iterate through the unique PCA classes and filter data
# from remaining data, record and plot* growth rates:
# *record in a csv (based on class_heightMetric.csv) and plot median values + growth rate (from least squares min fit on median)


import os, sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time
import GLAS_zonalStats_to_database as zs # for csv to db function

def regression(X, Y, order=1): # doing linear

    coeffs = np.polyfit(X, Y, order) # returns [ m  b ]
##    print coeffs
##    m,b = coeffs.tolist()

    # get r-squared and fit [function(x) that predicts y]
    fit = np.poly1d(coeffs)
##    #print fit
##    yhat = fit(X)
##    ybar = np.sum(Y)/float(len(Y))
##
##    ssreg = np.sum((yhat-ybar)**2) # same as np.sum([(yihat-ybar)**2 for yihat in yhat])
##    sstot = np.sum((Y-ybar)**2) # same as np.sum([(yi-ybar)**2 for yi in Y])
##    if sstot ==0:
##        print Y
##        print ybar
##    R2 = float(ssreg)/sstot

    #return m,b,R2,fit
    return fit

# Set some variables
# The main zonal stats database/csv where all data is kept
#databaseCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20190328/outputs/WV01_20120817_102001001CD26500_102001001C4C8800__stats.csv' # temp
databaseCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/GLAS_Stacks_zonalStats_15m-2m.csv'
# Create variables for representative columns - this may change between runs
heightMetric = sys.argv[1]#'GLAS' # metric being used for height, which column(s) are used for height will depend on the metric. 1) GLAS; 2) DSM; 3) Combined
# FOR NOW, these are the column names to get the heights:
# GLAS: 'MedH' (for now)
# DSM: '2__median' (sr05_4m-sr05-min_1m-sr05-max_dz_eul_type_warp.tif median)
# Combined: '3__median' (DSM median) - 'elev_groun' (ground height from GLAS)

swapDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/spaceForTimeSwap'

tccCol = '6__median' # median of LS7_CC_type_warp.tif; aka Tree Canopy Cover
inClassCol = '10__max' # input class val from PCA class tiff. Use max for now
classCol = 'class' # simplified class row which will be calculated below
distYrCol = '7__max' # disturbance year; use max for now
timeSinceDistCol = 'timeSinceDist' # in years; aka. data date - disturbance date

# Filter thresholds, etc.
minHeightFilter = 0.1 # must grow 0.1 meters + per year
maxHeightFilter = 1   # cannot grow > 1 m + per year
minTccFilter = 10 # blanket filter ... TCC must be 10% or more at all times
maxTccFilter = 30 # TCC must be less than 30%
minN = 30 # minumum number of samples per year needed to be included in trend

order = 1 # messing around

# Read input database into Pandas dataframe
db_df = pd.read_csv(databaseCsv)
#import pdb; pdb.set_trace()
# filter based on TCC; slope (?), other blanket filters; disturbance year not none
db_df = db_df[(db_df[tccCol] >= minTccFilter) & (db_df[tccCol] <= maxTccFilter)] # 10% <= TCC <= 30%
db_df = db_df[db_df[distYrCol] != 'None'] # Has a disturbance year

# Create time since disturbance columm; this depends on which height metric we are using
# Also create an official representative height column called height
if heightMetric == 'GLAS':
    dataDateCol = db_df['shotYear']
    db_df['height'] = pd.to_numeric(db_df['MedH']) #* GLAS_only FOR NOW; probably change later; float type
else:
    dataDateCol = db_df['stackName'].str.slice(start=5, stop=9) # get year from stack name i.e WV01_yyyymm_etc...
    if heightMetric == 'DSM':
        # there may be 'None' in the 2__median column. Filter those out first
        db_df = db_df[db_df['2__median'] != 'None']
        db_df['height'] = pd.to_numeric(db_df['2__median'])
    elif heightMetric == 'Combined':
        # there may be 'None' in the 3__median column. Filter those out first
        db_df = db_df[db_df['3__median'] != 'None']
        db_df['height'] = pd.to_numeric(db_df['3__median']) - pd.to_numeric(db_df['elev_groun'])

# time since disturbance = dataDate - distDate
db_df[timeSinceDistCol] = pd.to_numeric(dataDateCol, downcast='integer') - (pd.to_numeric(db_df[distYrCol], downcast='integer')+1900)
db_df = db_df[db_df[timeSinceDistCol] > 0] # only want rows with time since disturbance > 0

# Lastly, filter using the min and max height filters
#db_df = db_df[(db_df['height']/db_df['timeSinceDist'] >= minHeightFilter) & (db_df['height']/db_df['timeSinceDist'] <= maxHeightFilter)]
# After speakinhg with Chris, this may not be the way to do it. Try this:
db_df = db_df[(db_df['height'] >= (1.37 + minHeightFilter*db_df['timeSinceDist'])) & (db_df['height'] <= (2.0 + maxHeightFilter*db_df['timeSinceDist']))]

# Get the unique PCA classes to iterate through; add extra columm to simplify
# *Check math here
#db_df[classCol] = (db_df[inClassCol]/100000000).round().astype('int8') -- had to convert to float before rounding (below)
db_df[classCol] = (db_df[inClassCol]/100000000).astype('float32').round().astype('int8')
uClasses = db_df[classCol].unique()

# temporary, create csv of overview --> class, year, nSamples, median Height
##tempSummaryCsv = os.path.join(swapDir, 'classYearSummary__{}.csv'.format(heightMetric))
##with open(tempSummaryCsv, 'w') as oc:
##    oc.write('Class,TimeSinceDist,nSamples,medianHeight\n')


# At this point, we have filtered down the points and we want to visualize this spatially
# So write the filtered points to csv, then use csv to make a shp
filteredCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/spaceForTimeSwap/filteredPointCsvs/{}__filteredPoints.csv'.format(heightMetric)

##db_df.to_csv(filteredCsv, index=False)
##print zs.database_to_shp(filteredCsv)

# iterate through classes
for eco in uClasses:


    class_df = db_df[db_df[classCol]==eco]
    uYears = class_df['timeSinceDist'].unique() # unique time steps for class

    #import pdb; pdb.set_trace()
    # for all points
    X_all = []#[1] # X = time since disturbance
    Y_all = []#[1.37] # Y = height in meters
    medDict = {} # for median values
    valDict = {} # for all values
    for yr in uYears:

        year_df = class_df[class_df['timeSinceDist']==yr] # dataframe for eco class/year
        if int(eco) == 20 and int(yr) == 14.0:
            import pdb; pdb.set_trace()
        # get the number of rows from year_df
        nSamples = len(year_df)
        if nSamples < minN:
            #X.append(int(yr))
 #       else:
            print "Class {}, year {} has only {} samples\n".format(eco, yr, nSamples)
            continue # move on

        year_heights = year_df['height']

        # get all values: -- this won't work, it will replace the key every time. try lists
        for i in year_heights:
            X_all.append(int(yr))
            Y_all.append(float(i))


        # get median value of heights from year_df
        medHeight = year_heights.median()
        #Y.append(float(medHeight))
        valDict[int(yr)] = float(medHeight)
        print yr, medHeight

##    print "Class {}".format(eco)
##    print X
##    print Y
##    print np.unique(X)
##    print np.unique(Y)
##    print np.polyfit(X, Y, 1)
##    print np.poly1d(np.polyfit(X, Y, 1))
    #plt.plot(np.unique(X), np.poly1d(np.polyfit(X, Y, 1))(np.unique(X)))
##        #print eco, yr, nSamples, medHeight
##        with open(tempSummaryCsv, 'a') as oc:
##            oc.write('{},{},{},{}\n'.format(eco, yr, nSamples, medHeight))

    #import pdb; pdb.set_trace()
    # convert to np array:
##    X = np.asarray(X)
##    Y = np.asarray(Y)
    X = np.asarray(valDict.keys())
    Y = np.asarray(valDict.values())
##    m,b,R2,fit = regression(X, Y)
##    print fit
##    print R2
    if len(X) < 2: # no year/only one year for class passed the minN threshold
        print "Not enough years with samples for class {}".format(eco)
        continue

##    # temporary:
##    X = X_all
##    Y = Y_all

    fit = regression(X, Y, order) # try doing all. order 2

    #import pdb; pdb.set_trace()
    outFig = os.path.join(swapDir, 'plot_{}_class{}__order{}.png'.format(heightMetric, eco, order))
    fig = plt.figure(figsize=(12,8.27))
    #fig = Figure(figsize=(12,8.27))
    ax = fig.add_subplot(111)
    #ax.plot(X, m*X + b, color = 'blue')
    ax.plot(X, fit(X), color = 'blue')
    ax.scatter(X_all, Y_all, color='black', s=15) # look at all points too
    ax.scatter(X, Y, color='green')
    ax.set_title('TITLE', fontsize=17, fontweight='bold')#, fontdict=fonts)
##    ax.set_xlim(min(X)-1, max(X)+1)
##    ax.set_ylim(-5, 35)
    ax.set_xlabel('Time Since Disturbance (years)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Stand Height (m)', fontsize=14, fontweight='bold')
    plt.subplots_adjust(top=0.88)
    fig.savefig(outFig)
    print len(X)
    print "Wrote to {}\n".format(outFig)

        # create df to be written to CSV --> some attributes, height, timeSinceDist, other/all attributes temp for verification
        # figure out how to send to least squares for growth rates

        # write to big CSV??? heightMetric, Class, growth rate, p-value
    #sys.exit()


# iterate through points: filter based on height per year, write to CSV

# then what?

# with remaining data, iterate over the years (time since dist.) and:
# get the median height and if N >= 100, record in dict/list, to be plotted --> if N < 100, I think we just do NoData
# use median heights to get

# repeat for next PCA class

# run again for different height metric








"""
start = time.time()

runN = '8'
# set data path variables
ddir = 'D:\\Maggie\\GEOG660_lidar'
figdir = os.path.join(ddir, 'Graphs', 'run_{}'.format(runN))
tbldir = os.path.join(ddir, 'Tables', 'run_{}'.format(runN))
database = os.path.join(ddir, 'GLAS_layers_zonalStats.csv')

for d in [figdir, tbldir]:
    if not os.path.exists(d): os.makedirs(d)

# set thresholds and parameters
slopeThresh = 10 # No slope larger than threshold counts
lowConfidenceChange = True # turn off if we want to exclude LC change
upperGrowthThresh = 1.0 # if a stand has grown more than 1.5m each year, throw it out
lowerGrowthThresh = 0.2 # stand must grow at least 0.2 m a year or else it's trash


summaryTable = os.path.join(ddir, 'Summaries', 'regression_summary_run{}.csv'.format(runN))
with open(summaryTable, 'w') as st: # write the header:
    st.write('Input .csv ={}, Slope threshold = {},LowConfidence = {},upperGrowthThresh={},lowerGrowthThresh={}\n'.format(database, slopeThresh,lowConfidenceChange,upperGrowthThresh,lowerGrowthThresh))
    st.write('Name,ecoCode,permaCode,beforeFilter,afterFilter,m,b,Fit,R2\n')

# function to do regression for X and Y, and get R2
def regression(X, Y, order=1): # doing linear

    coeffs = np.polyfit(X, Y, order) # returns [ m  b ]
    print coeffs
    m,b = coeffs.tolist()

    # get r-squared and fit [function(x) that predicts y]
    fit = np.poly1d(coeffs)
    #print fit
    yhat = fit(X)
    ybar = np.sum(Y)/float(len(Y))

    ssreg = np.sum((yhat-ybar)**2) # same as np.sum([(yihat-ybar)**2 for yihat in yhat])
    sstot = np.sum((Y-ybar)**2) # same as np.sum([(yi-ybar)**2 for yi in Y])
    if sstot ==0:
        print Y
        print ybar
    R2 = float(ssreg)/sstot

    return m,b,R2,fit

ECOREGIONS_DICT = {
    # ecoID: ecoName
    1:'Alaska-St. Elias Range tundra',
    2:'Alberta-British Columbia foothills forests',
    3:'Alberta Mountain forests',
    4:'British Columbia mainland coastal forests',
    5:'Canadian Aspen forests and parklands',
    6:'Cascade Mountains leeward forests',
    7:'Central British Columbia Mountain forest',
    8:'Central Pacific coastal forests',
    9:'Fraser Plateau and Basin complex',
    10:'Muskwa-Slave Lake forests',
    11:'North Central Rockies forests',
    12:'Northern Cordillera forests',
    13:'Northern Pacific coastal forests',
    14:'Northern transitional alpine forests',
    15:'Okanagan dry forests',
    16:'Pacific Coastal Mountain icefields and tundra',
    17:'Palouse grasslands',
    18:'Puget lowland forests',
    19:'Queen Charlotte Islands',
    20:'Rock and Ice',
    21:'Yukon Interior dry forests',
    99:'All Ecoregions'}


PERMAFROST_DICT = {
    # new dict: 'permaName': [permaNumCodeList] ... permaCode = permaName[0]
    'Discontinuous': [3],
    'Isolated': [2, 5],
    'Sporadic': [1, 4, 9],
    'Land': [8],
    'Ocean/Inland Seas': [7],
    'Glaciers': [6],
    'All': [1,2,3,4,5,6,7,8,9]}

##PERMAFROST_DICT = {
##    # new dict: 'permaName': [permaNumCodeList] ... permaCode = permaName[0]
##    'Discontinuous': [3],
##    }
##
# read header and database into lists. header is a list of field names, allLines is a list of strings representing rows
with open(database, 'r') as od:
    header = [f.strip() for f in od.readline().split(',')]
    allLines =[f.strip() for f in od.readlines()] # read rest
print header

# first iterate through landscape options (ecoregion/permafrost combo) and read rows
e=0
for ecoCode, ecoName in ECOREGIONS_DICT.iteritems():
##    print type(ecoCode), ecoName
##    if int(ecoCode) != 11:
##        continue


    # if we want to just do permafrost type, (I, S, D, L)
    # need to loop through and accept permafrost type where permaExtDesc == 'S' or etc

    for permaName, permaNumCodeList in PERMAFROST_DICT.iteritems():
##        print '---------------------------------'
##        print permaName, permaNumCodeList
##        if not permaName == 'Land':
##            continue

##        e+=1
##        if e == 2:
##            sys.exit()

        permaCode = permaName[0] # D is permaCode, Discontinuous is permaName

        if permaCode == 'O' or permaCode == 'G':
            continue # skip water and glaciers

        print "Ecoregion: {} ({})".format(ecoCode, ecoName)
        print "Permafrost: {} ({})\n".format(permaCode, permaName)

        # set up landscapeCombo-specific vars and outputs
        landscapeCode = 'eco-{}_perma-{}'.format(ecoCode, permaCode) # i.e. eco-11_perma-D for example -- used to name files etc.
        plotTitle = 'Ecoregion: {}\nPermafrost: {}'.format(ecoName, permaName) # the title of the graph -- full descriptions
        outFig = os.path.join(figdir, '{}__plot.png'.format(landscapeCode))
        outTbl = os.path.join(tbldir, '{}__table.csv'.format(landscapeCode))

        comboCntBefore = 0 # to count the number of lines for the land combo before filter
        comboCnt = 0 # to count the number of lines for the land combo after filter

        X = [] # to store X values (time since dist)
        Y = [] # to store Y values (height)

        for lineStr in allLines:

            line = lineStr.split(",")
##            print (int(line[header.index("perm_med")]) in permaNumCodeList)
##            print int(line[header.index("perm_med")])
##            print permaNumCodeList


##            if not int(line[header.index("ecoR_med")]) == int(ecoCode) or not (int(line[header.index("perm_med")]) in permaNumCodeList):
##
####                print int(line[header.index("ecoR_med")]), ecoCode
####                print int(line[header.index("perm_med")]), permaNumCodeList
####                print permaNumCodeList
####                sys.exit()
##                continue
##            else:
##                comboCntBefore += 1
##                print line
##

            if int(line[header.index("ecoR_med")]) != int(ecoCode):
##                print "Eco code not right"
#                print int(line[header.index("ecoR_med")]), ecoCode
                if ecoCode != 99:
                    continue
            if not (int(line[header.index("perm_med")]) in permaNumCodeList):
##                print "perma not right"
##
####                print int(line[header.index("ecoR_med")]), ecoCode
##                print int(line[header.index("perm_med")]), permaNumCodeList
##                sys.exit()
                continue
            else:
                comboCntBefore += 1
                #print line


            # 1 first check: be sure there is not a wide mixture of disturbance years
            distYrMin = int(line[header.index("distYr_mn")])
            distYrMax = int(line[header.index("distYr_mx")])
            distYrMed = int(line[header.index("distYr_med")])

            if distYrMed == -9999:
                continue
            if distYrMax - distYrMin > 1:
                print "Can't use point. Disturbance year range too large (more than a year apart). Skipping\n"
                continue # skip
            # now take the median to represent year. Add 1990 to convert disturbance layer year to actual year
            distYear = distYrMed + 1900
            print distYear

            # 2 next check slope:
            slope = float(line[header.index("slope_ave")])
            if slope > slopeThresh:
                print "Cannot use point. Slope too large ({})Skipping\n".format(slope)
                continue # skip

            # 3 check to be sure disturbance occured before GLAS shot
            GLASyear = int(line[header.index("GLAS_year")])
            timeSinceDist = GLASyear - distYear # years since disturbance has to be GTE 1
            if timeSinceDist <= 0:
                print "GLAS shot occurred before or same year as disturbance. Skipping\n"
                print GLASyear, distYear
                continue

            # 4 if lowConfidenceChange is turned off, remove those whose median change type is low confidence (6)
            distType = int(line[header.index("distTp_med")]) # for this there are only 2 types, so we can get the median and consider it the dominating change type
            if not lowConfidenceChange:
                if distType == 6:
                    print "Cannot use point. Low confidence change type. Skipping\n"
                    continue

            # 5 lastly, make sure height is within growth threshold
            height = float(line[header.index("rh100")])
            if height < timeSinceDist * lowerGrowthThresh:
                print "Height of {}m is too short for {} years".format(height, timeSinceDist)
                continue
            if height > timeSinceDist * upperGrowthThresh:
                print "Height of {}m is too tall for {} years".format(height, timeSinceDist)
                continue
            # if we've made it here, we want to and add info to X (time since Dist) and Y (height)
            comboCnt += 1
            print timeSinceDist
            print height
            X.append(timeSinceDist)
            Y.append(height)

        print "Data points for {} before filtering: {} and after: {}".format(landscapeCode, comboCntBefore, comboCnt)

        if comboCnt == 0:
            print " Cannot do regression with no points\n"
            with open(summaryTable, 'a') as st:
                st.write("{},{},{},{},{},'--,'--,'--,'--\n".format(landscapeCode,ecoCode,permaCode,comboCntBefore,comboCnt))
            continue

        X = np.asarray(X) # convert python list to numpy
        Y = np.asarray(Y)

        # now get slope (m) and intercept (b) and R2
        m,b,R2,fit = regression(X, Y, order=1)
        #print m,b,R2,fit # fit prints {m} x + {b} -- represents f(x)

        with open(summaryTable, 'a') as st:
            st.write("{},{},{},{},{},{},{},{},{}\n".format(landscapeCode,ecoCode,permaCode,comboCntBefore,comboCnt,m,b,str(fit).strip(),R2))

       #plotTitle = 'North Atlanta Dunes National Forest \nDiscontinuous Permafrost with thick overburden and blah blah ice\n '
        print "Drawing plot {}".format(outFig)
        fig = plt.figure(figsize=(12,8.27))
        #fig = Figure(figsize=(12,8.27))
        ax = fig.add_subplot(111)
        ax.plot(X, m*X + b, color = 'blue')
        ax.scatter(X, Y, color='green')
        ax.set_title(plotTitle, fontsize=17, fontweight='bold')#, fontdict=fonts)
        ax.set_xlim(min(X)-1, max(X)+1)
        ax.set_ylim(-5, 35)
        ax.set_xlabel('Time Since Disturbance (years)', fontsize=14, fontweight='bold')
        ax.set_ylabel('Stand Height (m)', fontsize=14, fontweight='bold')
        plt.subplots_adjust(top=0.88)
        fig.savefig(outFig)

        # write to table csv:
        with open(outTbl, 'w') as ot:
            for i in range(0, len(X)):
                ot.write('{},{}\n'.format(X[i], Y[i]))
        print '----------------------------------------\n'


print '\nFinished run{} in {} minutes'.format(runN,(time.time()-start)/60)


# TO DO:

# 1) write X and Y to csv ecorR_permaM__table.csv


# 2) add the following to summaryList
# as we are looping through different landsiteTypes/combos, we should be building csv of:
# landscapeName, ecoR, perma, n points, m, b, regression Equation, R2
# for each landscape combo, a small csv will be written with points and a plot with best fit line will be saved to image:
## tables/landscapeName_table.csv, plots/landscapeName_plots.csv

# 3) after going thru all combos, write summaryList to csv

# 4) try to also do just ecoregion and just permafrost. so straitfy by just ecor and perma. can prob do this in for loop



## #works but try another way
## #now we try to plot and save to fig:
##fig, ax = plt.subplots()
####fig.title('Title') doest work
###fig.suptitle('Growth after Disturbance\n', fontsize=30, ha='center')
####plt.tight_layout()
##ax.plot(X, m*X + b, color = 'red')
##ax.scatter(X, Y)
##ax.set_title('Growth after \nDisturbance', fontsize=30) # cuts off at top
##ax.set_xlabel('Time Since Disturbance', fontsize=20)
##ax.set_ylabel('Stand Height (m)')
##ax.xaxis.label.set_size(20)
##fig.savefig(figure)


### also works
##fig = plt.figure()
###plt.title('Title', y=1.0)
##plt.suptitle('Growth after \nDisturbance\n', fontsize=30, ha='center')
##plt.plot(X, m*X + b, color = 'red')
##plt.scatter(X, Y)
##plt.xlabel('Time Since Disturbance (years)')
##plt.ylabel('Stand Height (m)')
##fig.savefig(figure)


"""