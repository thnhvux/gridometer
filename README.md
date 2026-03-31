# gridometer
Run instruction: 

1. Fetch historical demand data using `python scripts/fetch_demand.py`

2. (Optional at this point) Fetch adequacy data using `python scripts/fetch_adequacy.py`

3. Build the program via CMake
For MinGW users:
`cd build`
`cmake -G "MinGW Makefiles" ..`
`cmake --build .`

 4. Run the executable `.\gridometer.exe`
