#!/bin/bash
#
# Produce L1 DEMs and CLR maps by differencing L0 DSMs from a reference to change cloud elevs to no data
#
sceneName=$(echo $1 | cut -d'/' -f 2)

# Ref DEM
hmaDEM=/att/nobackup/pmontesa/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif

topDir=/att/pubrepo/hma_data/ASTER
now="$(date +'%Y%m%d%T')"

# Process the AST_L1A dir indicated with the sceneName
L1Adir=$topDir/L1A
cd $topDir

hostN=`/bin/hostname -s`

#find L1A/*/outASP -type f -name out-DEM.tif > scenes.jjason.outASP-DEM
python ~/code/gen_csv_chunks.py scenes.jjason.outASP-DEM nodes_himat

if [ ! -d ${L1Adir}_out/L0 ]; then
  mkdir ${L1Adir}_out/L0
fi

if [ ! -d ${L1Adir}_out/L0/logs ]; then
	mkdir ${L1Adir}_out/L0/logs
fi

# Read in sceneList of AST L1A scenes from an orderZip

	 cd $L1Adir

	 logFile=${L1Adir}_out/L0/logs/$batch_$hostN_$sceneName.log

	 outPrefix=$sceneName/outASP/out

 if [ -f $outPrefix-DEM-clr-shd.tif ]; then
      echo "   Colormap exists." | tee -a $logFile

		if [ ! -d ${L1Adir}_out/L0/clr ]; then
		  mkdir ${L1Adir}_out/L0/clr
		fi
		if [ ! -d ${L1Adir}_out/L0/dsm ]; then
		  mkdir ${L1Adir}_out/L0/dsm
		fi
		if [ ! -d ${L1Adir}_out/L0/drg ]; then
		  mkdir ${L1Adir}_out/L0/drg
		fi

      echo "  Create output VRT files: CLR, DEM, DRG..." | tee -a $logFile
      gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM-clr-shd.tif ${L1Adir}_out/L0/clr/${sceneName}_CLR.vrt
      gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/L0/drg/${sceneName}_DRG.vrt
      gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM.tif ${L1Adir}_out/L0/dsm/${sceneName}_DEM.vrt
      echo "[7] Finished processing ${sceneName}." | tee -a $logFile
  fi
  

  dem_list="${hmaDEM} ${L1Adir}/$outPrefix-DEM.tif"
  #pix_res=gdalinfo ${L1Adir}/$outPrefix-DEM.tif | grep "Pixel Size" | sed "s/Pixel Size = (//g; s/,/ /g" | awk -F "\"* \"*" '{print $1}'
  echo $dem_list | tee -a $logFile
  #echo $pix_res

  echo " Create warped ref DEM"
  warptool.py -tr last -te intersection $dem_list -outdir ${L1Adir}/$sceneName/outASP
  #dem_list_warp=$(echo $dem_list | sed 's/.tif/_warp.tif/g')

  out_warp_ref=$(echo ${L1Adir}/$sceneName/outASP/$(basename $hmaDEM) | sed 's/.tif/_warp.tif/g')

  if [ ! -d ${L1Adir}_out/L1 ]; then
  	 mkdir ${L1Adir}_out/L1
  fi
  if [ ! -d ${L1Adir}_out/L1/clr ]; then
  	 mkdir ${L1Adir}_out/L1/clr
  fi
  if [ ! -d ${L1Adir}_out/L1/dsm ]; then
  	 mkdir ${L1Adir}_out/L1/dsm
  fi
  
  # Produce Level 1 DEMs: no clouds --> Calc diff from ref DEM; threshold; remove cloud elev pixels
  #
  if gdalinfo ${L1Adir}/$outPrefix-DEM_L1.tif ; then

	echo "Already done:"
	echo ${L1Adir}/$outPrefix-DEM_L1.tif

  else

  	echo " Running gdal_calc.py" | tee -a $logFile
  	gdal_calc.py -A ${L1Adir}/$outPrefix-DEM.tif -B $out_warp_ref --outfile=${L1Adir}/$outPrefix-DEM_L1.tif --calc="-99*((A-B)>100)+A*(A-B<=100)" --NoDataValue=-99

  	hillshade ${L1Adir}/$outPrefix-DEM_L1.tif -o ${L1Adir}/$outPrefix-DEM_L1-hlshd-e25.tif -e 25
  	colormap ${L1Adir}/$outPrefix-DEM_L1.tif -s ${L1Adir}/$outPrefix-DEM_L1-hlshd-e25.tif -o ${L1Adir}/$outPrefix-DEM_L1-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt
  
  	echo "  Create output VRT files: CLR, DEM, DRG..." | tee -a $logFile
  	gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM_L1-clr-shd.tif ${L1Adir}_out/L1/clr/${sceneName}_CLR_L1.vrt
  	gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM_L1.tif ${L1Adir}_out/L1/dsm/${sceneName}_DEM_L1.vrt
  
  	rm ${L1Adir}/$outPrefix-DEM_L1-hlshd-e25.tif

  	gdaladdo -r average ${L1Adir}/$outPrefix-DEM_L1-clr-shd.tif 2 4 8 16

  	rm $out_warp_ref
  fi
