@echo off
uv run --python 3.12 --no-project python "%~dp0manage.py" %*
exit /b %ERRORLEVEL%
