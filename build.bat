@echo off

if "%1"=="linux" (
SET CGO_ENABLED=0
SET GOOS=linux
) else (
SET CGO_ENABLED=1
SET GOOS=windows
)
go build -ldflags="-w"