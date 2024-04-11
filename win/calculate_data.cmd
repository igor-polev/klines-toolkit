@ECHO OFF
START "Klines Toolkit - Data processing thread 1" /D "C:\Users\IGOR\OneDrive\CRYPTO\SCRIPTS\Klines Toolkit" /BELOWNORMAL /AFFINITY 0x1 python kt_script.py -p 4 -t 0 -w
ECHO Thread 1 started
TIMEOUT /T 10
START "Klines Toolkit - Data processing thread 1" /D "C:\Users\IGOR\OneDrive\CRYPTO\SCRIPTS\Klines Toolkit" /BELOWNORMAL /AFFINITY 0x2 python kt_script.py -p 4 -t 1 -w
ECHO Thread 2 started
TIMEOUT /T 3
START "Klines Toolkit - Data processing thread 1" /D "C:\Users\IGOR\OneDrive\CRYPTO\SCRIPTS\Klines Toolkit" /BELOWNORMAL /AFFINITY 0x4 python kt_script.py -p 4 -t 2 -w
ECHO Thread 3 started
TIMEOUT /T 3
START "Klines Toolkit - Data processing thread 1" /D "C:\Users\IGOR\OneDrive\CRYPTO\SCRIPTS\Klines Toolkit" /BELOWNORMAL /AFFINITY 0x8 python kt_script.py -p 4 -t 3 -w
ECHO Thread 4 started
