
# TianGong AI Multi-agents

## Env Preparing

### Using VSCode Dev Contariners

[Tutorial](https://code.visualstudio.com/docs/devcontainers/tutorial)

Python 3 -> Additional Options -> 3.11-bullseye -> ZSH Plugins (Last One) -> Trust @devcontainers-contrib -> Keep Defaults

Setup `venv`:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Install requirements:

```bash
pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt --upgrade

pip freeze > requirements_freeze.txt
```

```bash
sudo apt install python3.11-dev
sudo apt install libmagic-dev
sudo apt install poppler-utils
sudo apt install tesseract-ocr
sudo apt install libreoffice
sudo apt install pandoc
```

Install Cuda (optional):

```bash
sudo apt install nvidia-cuda-toolkit
```

### Auto Build

The auto build will be triggered by pushing any tag named like release-v$version. For instance, push a tag named as v0.0.1 will build a docker image of 0.0.1 version.

```bash
#list existing tags
git tag
#creat a new tag
git tag v0.0.1
#push this tag to origin
git push origin v0.0.1
```

### RUN

```bash
nohup .venv/bin/python3.11 src/calculation.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_0.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_1.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_2.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_3.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_4.py > /dev/null 2>&1 &
nohup .venv/bin/python3.11 src/calculation_5.py > /dev/null 2>&1 &

nohup .venv/bin/python3.11 src/analysis_all.py > log.txt 2>&1 &

nohup .venv/bin/python3.11 src/analysis_large_remove_refs_id.py > log_large.txt 2>&1 &
nohup .venv/bin/python3.11 src/analysis_large_all.py > log_large_all.txt 2>&1 &


sudo pkill -f python3.11
```
