#!/bin/bash
cd tests/Ficus.Tests
dotnet run --framework netcoreapp2.0 --stress-timeout 45 --stress-memory-limit 8192 --stress $@
