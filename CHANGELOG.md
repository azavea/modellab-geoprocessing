## 0.4.2

- Fix color breaks for NDVI, VARI, and VGI.

## 0.4.1

- The static configuration files for layers and color breaks have been moved into the .jar file.
- The Geotrellis dependency has been updated to enable floating-point color breaks.
- Many new color breaks have been added.
- A bug relating to how "LocalUnary*" operations are echoed has been fixed.
- A "ConvertToFloat" operation has been added to allow floating-point rasters to result from integer-raster inputs.

## 0.4.0

- Added operation for masking values between specified ranges.
- Ensure application shuts down if AWS credentials cannot be determined.

## 0.3.0

- Added the remaining focal and local functions that are implemented in GeoTrellis.
- Added an endpoint to return a cell value neighborhood summary for a given lat,lng pair.
- Fixed an issue where neighborhood shape wasn't properly being recognized when serializing functions to hashes.
- Removed demo Librato credentials.

## 0.2.3

- A bug affecting the Focal Slope and Focal Aspect functions has been corrected.
- Extra keys supplied in the JSON structure given to the layer-registration endpoint are now passed through in the endpoint's response.
- New sample layers have been added.

## 0.2.2

- A minor error in the Travis configuration file was corrected.

## 0.2.1

- A minor error in the Travis configuration file was corrected.

## 0.2.0

- The routes used for registering breaks and layers have been changed, as have those for accessing tiles.

## 0.1.0

- Initial release.

