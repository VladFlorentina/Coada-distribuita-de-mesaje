# Coada-distribuita-de-mesaje

Proiect de retele pentru o coada distribuita de mesaje, implementat in Python 3.9+.

## Ce contine

- nod hibrid client + server
- protocol simplu bazat pe mesaje JSON
- subscribe / unsubscribe pe chei
- publish si livrare catre consumatori
- comanda executata local in functie de cheie
- suport pentru Docker Compose cu 3 noduri

## Instalare locala

```bash
python -m pip install -e .[dev]
```

## Testare

```bash
python -m pytest
```

## Rulare nod

```bash
python -m dmq node \
	--node-id node1 \
	--bind-host 127.0.0.1 \
	--bind-port 5001 \
	--advertise-host 127.0.0.1 \
	--advertise-port 5001 \
	--key-command demo=upper
```

## Exemple de comanda

Subscribe:

```bash
python -m dmq subscribe --target node1@127.0.0.1:5001 --subscriber node2@127.0.0.1:5002 --key demo
```

Publish:

```bash
python -m dmq publish --target node1@127.0.0.1:5001 --key demo --payload "hello world"
```

Binary publish from file:

```bash
python -m dmq publish --target node1@127.0.0.1:5001 --key demo --payload-file sample.bin
```

Publish from hex or base64:

```bash
python -m dmq publish --target node1@127.0.0.1:5001 --key demo --payload-hex 68656c6c6f
python -m dmq publish --target node1@127.0.0.1:5001 --key demo --payload-base64 aGVsbG8=
```

## Membrul 3

Pentru partea de mesaje si procesare, folosim chei care au comenzi diferite in nod:

- `demo=upper`
- `demo=reverse`
- `demo=length`

Astfel poti demonstra rapid procesarea diferita a aceluiasi payload pe noduri diferite sau pe chei diferite.

## Docker

```bash
docker compose up --build
```

## Plan de lucru

Planul complet este in [PLAN DE LUCRU.md](PLAN%20DE%20LUCRU.md).
