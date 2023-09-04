param ($location, $destination)

Copy-item -Force -Recurse -Verbose $location\build\native\include -Destination $destination
Copy-item -Force -Recurse -Verbose $location\build\native\lib -Destination $destination