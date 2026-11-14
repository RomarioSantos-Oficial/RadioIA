@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo Criando ambiente Python local...
    python -m venv .venv
    if errorlevel 1 (
        echo Nao foi possivel criar o ambiente. Instale Python 3.10 ou superior.
        pause
        exit /b 1
    )
)

call ".venv\Scripts\activate.bat"
echo Instalando ou atualizando dependencias...
python -m pip install --disable-pip-version-check -r requirements.txt
if errorlevel 1 (
    echo Falha ao instalar dependencias.
    pause
    exit /b 1
)

where ffprobe >nul 2>nul
if errorlevel 1 (
    echo AVISO: FFprobe nao foi encontrado no PATH. Instale FFmpeg para importar duracoes de audio e video.
    echo O painel ainda abrira, mas a importacao de midias precisa do FFprobe.
)

echo Abrindo o painel em http://localhost:8000/admin
start "" /b powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep -Seconds 2; Start-Process 'http://localhost:8000/admin'"
echo Para encerrar o playout, pressione Ctrl+C nesta janela.
python -m uvicorn app:app --host 127.0.0.1 --port 8000
pause
