@echo off
REM Direct compilation with javac, bypassing Gradle

setlocal enabledelayedexpansion

REM Compile main source files
cd /d "%~dp0"

echo Compiling main source files...
javac -d "bin\main" -encoding UTF-8 ^
  "src\main\java\pdc\Message.java" ^
  "src\main\java\pdc\Master.java" ^
  "src\main\java\pdc\Worker.java" ^
  "src\main\java\pdc\MatrixGenerator.java"

if %ERRORLEVEL% NEQ 0 (
  echo COMPILATION FAILED
  exit /b 1
)

echo.
echo Compiling test files...
javac -d "bin\test" -cp "bin\main" -encoding UTF-8 ^
  "src\test\java\pdc\MasterTest.java" ^
  "src\test\java\pdc\WorkerTest.java" 2>&1

if %ERRORLEVEL% NEQ 0 (
  echo TEST COMPILATION FAILED
  exit /b 1
)

echo.
echo BUILD COMPLETE - All files compiled successfully!
echo Main classes: bin\main\pdc\*.class
echo Test classes: bin\test\pdc\*.class
