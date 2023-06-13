#!/bin/bash
cd tests/Ficus.Tests
dotnet run --framework net6.0 --stress-timeout 45 --stress-memory-limit 8192 --stress $@
