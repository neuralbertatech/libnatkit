param ($location, $version)

New-Item -ItemType Directory -Force -Path $location
cd $location
nuget install librdkafka.redist -Version $version