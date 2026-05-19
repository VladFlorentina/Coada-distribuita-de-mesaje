# Demo live (interactiv) – Coada distribuita de mesaje

Acest ghid este pentru un video cu interactiune in terminal (meniuri), in stilul "client/server".

## Varianta recomandata: 1 server + 2 clienti interactivi

Pornesti 3 terminale PowerShell in folderul proiectului (cu venv activ).

### Terminal 1 – node1 (server)

```powershell
python -m dmq node --node-id node1 --bind-host 127.0.0.1 --bind-port 5001 --advertise-host 127.0.0.1 --advertise-port 5001 --key-command demo=upper --log-level INFO
```

### Terminal 2 – node2 (client interactiv)

```powershell
python -m dmq interactive --node-id node2 --bind-host 127.0.0.1 --bind-port 5002 --advertise-host 127.0.0.1 --advertise-port 5002 --seed node1@127.0.0.1:5001 --target node1@127.0.0.1:5001 --key-command demo=reverse --log-level INFO
```

In meniul din terminalul node2:
- `2` Subscribe la cheie (scrii `demo`)
- `4` Publish mesaj (cheie `demo`, payload `hello world`)

Alternativ, daca vrei sa tastezi tu comenzi (REPL), porneste cu `--mode repl`:

```powershell
python -m dmq interactive --mode repl --node-id node2 --bind-host 127.0.0.1 --bind-port 5002 --advertise-host 127.0.0.1 --advertise-port 5002 --seed node1@127.0.0.1:5001 --target node1@127.0.0.1:5001 --key-command demo=reverse --log-level INFO
```

Comenzile pe care le bagi tu apar exact in acest terminal, dupa prompt-ul `dmq>`:

```text
dmq> subscribe demo
dmq> publish demo "hello world"
dmq> status
```

### Terminal 3 – node3 (client interactiv)

```powershell
python -m dmq interactive --node-id node3 --bind-host 127.0.0.1 --bind-port 5003 --advertise-host 127.0.0.1 --advertise-port 5003 --seed node2@127.0.0.1:5002 --seed node1@127.0.0.1:5001 --target node1@127.0.0.1:5001 --key-command demo=length --log-level INFO
```

In meniul din terminalul node3:
- `2` Subscribe la cheie (scrii `demo`)

Sau REPL (ca sa scrii comenzi):

```powershell
python -m dmq interactive --mode repl --node-id node3 --bind-host 127.0.0.1 --bind-port 5003 --advertise-host 127.0.0.1 --advertise-port 5003 --seed node2@127.0.0.1:5002 --seed node1@127.0.0.1:5001 --target node1@127.0.0.1:5001 --key-command demo=length --log-level INFO
```

### Ce trebuie sa se vada in video

- Cand publici din node2 (optiunea 4), in terminalele node2/node3 apar loguri de forma:
  - `Processed deliver ... command=reverse result=...`
  - `Processed deliver ... command=length result=...`
- Daca dezabonezi node3 (optiunea 3 in node3) si mai publici o data, doar node2 mai proceseaza.

## Daca porturile 5001-5003 sunt ocupate

Inlocuiesti peste tot:
- `5001/5002/5003` cu `5101/5102/5103`
- seed/target la fel.
