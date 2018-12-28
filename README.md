# liquidity

Анализ ликвидности Московской биржи

## Требования к установлению для Ubuntu 18.04

```
sudo apt update -y
sudo apt install -y python3.7 python3.7-dev build-essential libssl-dev libffi-dev python-virtualenv sqlite3
```

## Требования к программы установки

```
python3.7 -m virtualenv -p `which python3.7` venv/cpython37
source venv/cpython37/bin/activate
pip install -r requirements.txt
```

```
python main.py
```
