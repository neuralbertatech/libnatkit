@echo off

set location=%1
set version=%2

if not exist %location% mkdir %location%
cd %location%
nuget install librdkafka.redist -Version %version%