@echo off
setlocal EnableDelayedExpansion

PUSHD %~dp0..
SET SCRIPT_ROOT=%CD%
POPD
SET "CLASSPATH=%SCRIPT_ROOT%\conf;%SCRIPT_ROOT%\lib\*"
SET THREADCOUNT_LOAD=32
SET THREADCOUNT_RUN=256
SET RECORDCOUNT=1000000
SET OPCOUNT=10000000
SET RUNTIME=180
SET RUN_MODE=0
SET LOAD_MODE=0
SET WORKLOAD=

:parse
IF "%~1"=="" GOTO endparse
IF "%~1"=="-w" (
SHIFT
SET WORKLOAD=%~1
)
IF "%~1"=="-l" SET LOAD_MODE=1
IF "%~1"=="-r" SET RUN_MODE=1
IF "%~1"=="-R" (
SHIFT
SET RECORDCOUNT=%~1
)
IF "%~1"=="-O" (
SHIFT
SET OPCOUNT=%~1
)
IF "%~1"=="-T" (
SHIFT
SET RUNTIME=%~1
)
SHIFT
GOTO parse
:endparse

SET n=0
IF [%WORKLOAD%]==[] (
    FOR %%w in (a, b, c, d, e, f) do (
      SET /A n+=1
      SET workloads[!n!]=workloads\workload%%w
    )
) ELSE (
    SET /A n+=1
    SET workloads[1]=%WORKLOAD%
)

for /L %%i in (1,1,%n%) do (
  SET "LOADOPTS=-db site.ycsb.db.couchbase3.Couchbase3Client -P !workloads[%%i]! -threads %THREADCOUNT_LOAD% -p recordcount=%RECORDCOUNT% -s -load"
  SET "RUNOPTS=-db site.ycsb.db.couchbase3.Couchbase3Client -P !workloads[%%i]! -threads %THREADCOUNT_RUN% -p recordcount=%RECORDCOUNT% -p operationcount=%OPCOUNT% -p maxexecutiontime=%RUNTIME% -s -t"
  IF %RUN_MODE% EQU 0 (
    java -cp "%CLASSPATH%" site.ycsb.Client !LOADOPTS!
  )
  IF %LOAD_MODE% EQU 0 (
    java -cp "%CLASSPATH%" site.ycsb.Client !RUNOPTS!
  )
)
