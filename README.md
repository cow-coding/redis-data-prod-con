# Redis Producer Consumer Base code

## requirements

```
pip install -r requirements.txt
```

## How to use

1. Execute `consumer.py`
2. Execute `producer-xxxxx.py`

## Arguments

### `consumer.py`

```
python consumer.py --time_interval [time]
```

- `time_interval` insert batch data time interval

### `producer-xxxxx.py`

```
python producer-xxxxx.py --data_path [path] --token_idx [idx]
```

- `data_path` : data save path (not use)
- `token_idx` : if you use github oAuth tokens, write down token list in \*.yaml file. And this argument is start token idx
