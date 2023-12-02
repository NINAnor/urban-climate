/*
Author: Zander Venter

This code:
1. Extracts maximum LST for the summer of 2020/1
2. Extracts elevation from a Norwegian national DTM
3. Extracts WorldCover land cover fractions

The LST code is derived from Ermida et al and should be referenced as such:
Ermida, S.L., Soares, P., Mantas, V., Göttsche, F.-M., Trigo, I.F., 2020.
    Google Earth Engine open-source code for Land Surface Temperature estimation from the Landsat series.
    Remote Sensing, 12 (9), 1471; https://doi.org/10.3390/rs12091471

*/


/*
  // Initialize global environment ///////////////////////////////////////////////////////////////////////
*/

// Bring in municipalities for Norway
var municipalities = ee.FeatureCollection('users/zandersamuel/NINA/Vector/Norway_administrative_kommuner_2022');

// define the municipality you are working on
var selectedMunicipality = 'Bodø'

// Define area of interest
var aoi = municipalities.filter(ee.Filter.eq('navn',selectedMunicipality)).geometry().bounds()

// center map on aoi
Map.centerObject(aoi)

// link to the code that computes the Landsat LST
var LandsatLST = require('users/sofiaermida/landsat_smw_lst:modules/Landsat_LST.js')

// select date range, and landsat satellite
var satellite = 'L8';
var date_start = '2020-06-15';
var date_end = '2020-07-31';
var use_ndvi = true;

// Visualization palette
var cmap1 = ['blue', 'cyan', 'green', 'yellow', 'red'];


/*
  // 1. Extract LST ///////////////////////////////////////////////////////////////////////
*/

// get landsat collection with added variables: NDVI, FVC, TPW, EM, LST
var LandsatColl = LandsatLST.collection(satellite, date_start, date_end, aoi, use_ndvi)
var proj = LandsatColl.first().select(0).projection()
print(LandsatColl)

// select the first feature
var exImage = LandsatColl.max().clip(aoi);


Map.addLayer(exImage.select('LST'),{min:270, max:303, palette:cmap1}, 'LST',0)
Map.addLayer(exImage.multiply(0.0000275).add(-0.2),{bands: ['SR_B4', 'SR_B3', 'SR_B2'], min:0, max:0.3}, 'RGB',0)


var lst100m = exImage.select('LST')
  .setDefaultProjection(proj)
  .reduceResolution(ee.Reducer.mean())
  .reproject(proj.atScale(100));
Map.addLayer(lst100m,{min:270, max:303, palette:cmap1}, 'LST 100m',0)

// Export to drive
Export.image.toDrive({
  image: lst100m,
  description: 'LST',
  scale: 100,
  region: aoi
});


/*
  // 2. Extract elevation ///////////////////////////////////////////////////////////////////////
*/
// Kartverket elevation 10m - Fenoscandinavia
var dtm10 = ee.Image("users/rangelandee/NINA/Raster/Fenoscandia_DTM_10m").rename('DTM');
Map.addLayer(dtm10, {min:0, max:400}, 'dtm 10m', 0);

var dtm100m = dtm10
  .setDefaultProjection(dtm10.projection())
  .reduceResolution(ee.Reducer.mean(), true)
  .reproject(proj.atScale(100));
Map.addLayer(dtm100m, {min:0, max:400}, 'dtm 100m', 0);

// Export to drive
Export.image.toDrive({
  image: dtm100m,
  description: 'elevation',
  scale: 100,
  region: aoi
});

/*
  // 3. Extract land cover fracs /////////////////////////////////////////////////////////////////
*/

// World Cover 2021
var wc = ee.ImageCollection("ESA/WorldCover/v200").first();
Map.addLayer(wc, {bands: ['Map']}, "Landcover", 0);

// Land cover masks
var lcMasks = wc.eq(20).or(wc.eq(30)).or(wc.eq(60)).or(wc.eq(90)).rename('fractionGrass')
  .addBands(wc.eq(40).rename('fractionCropland'))
  .addBands(wc.eq(50).rename('fractionBuilt'))
  .addBands(wc.eq(70).or(wc.eq(80)).rename('fractionWater'));


var lcFracs = lcMasks
  .setDefaultProjection(wc.projection())
  .reduceResolution(ee.Reducer.mean(), true)
  .reproject(proj.atScale(100));
Map.addLayer(lcFracs.select('fractionBuilt'), {min:0, max:1}, 'fractionBuilt', 0);

// Export to drive
Export.image.toDrive({
  image: lcFracs,
  description: 'landcoverFractions',
  scale: 100,
  region: aoi
});

Map.addLayer(aoi, {}, 'aoi', 0)
