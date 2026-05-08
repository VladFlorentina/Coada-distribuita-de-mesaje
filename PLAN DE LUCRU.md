# PLAN DE LUCRU

## STADIU ACTUAL AL IMPLEMENTARII (Actualizat)

### CE A FOST IMPLEMENTAT (Functional)
1. **Arhitectura Hibridă (Client + Server):** Implementată prin clasa `DistributedNode`. Nodul acceptă conexiuni simultane și se leagă la primul *seed* valid găsit.
2. **Protocolul de Bază & Discovery (Handshake):** Mesajul `HELLO` și portul *callback* funcționează perfect. Mesajele `PEER_ANNOUNCE` distribuie corect topologia în rețea.
3. **Mecanismul Pub/Sub:** Operațiunile de `SUBSCRIBE` și `UNSUBSCRIBE` se propagă în rețea, iar prevenirea buclelor este gestionată nativ.
4. **Procesare și Comenzi (Simulate):** Payload-ul primest comenzi prin clasa `CommandRegistry` (comenzi: `echo`, `upper`, `lower`, `reverse`, `length`). Este conform cerinței (simulat prin switch-case).
5. **Set-up de Testare & Rulare:** Docker Compose este gata, conținând cele 3 noduri (`node1`, `node2`, `node3`) înlănțuite corespunzător.

### CE MAI TREBUIE IMPLEMENTAT / RAFINAT
1. **Heartbeat / Keep-Alive (Faza 5 - Robustețe):** Lipsesc mecanisme active (Pings) pentru ștergerea nodurilor care pică "silențios". Curățarea se face momentan doar la încercarea (eșuată) de trimitere a unui mesaj. Funcțional pentru demo, dar e punctul sensibil al sistemului.
2. **Scripturile de Demonstrație (Faza 6 & 9.4):** E nevoie de o suită de comenzi sau un script automat (`demo.sh` / `demo.py`) care să reproducă fidel cei 7 pași pentru video, demonstrând conectarea, abonarea, publicarea, comanda executată și consecința dezabonării/deconectării.
3. **Validări avansate de payload:** Limita maximă e setată din cod (`max_payload_size = 64 * 1024`), dar tratarea vizuală a excepției la introducerea de valori incorecte sau a absenței unei chei trebuie finisate pentru un output "curat" de consolă în demo.

---

## 1. Ce trebuie construit

Proiectul este un sistem distribuit de tip coada de mesaje, in care fiecare nod are dublu rol: client si server. Fiecare nod:

- incearca sa se conecteze la primul server disponibil dintr-o lista de servere
- accepta la randul lui conexiuni de la alti clienti
- poate face subscribe si unsubscribe pe chei
- poate primi mesaje binare asociate unei chei si le poate livra consumatorilor abonati

Ideea principala este urmatoarea:

- producatorul trimite un mesaj cu o cheie
- nodul care primeste mesajul identifica consumatorii abonati la acea cheie
- mesajul este livrat catre acei consumatori
- consumatorul proceseaza mesajul executand comanda asociata cheii

## 2. Cerinte functionale

### 2.1 Conectare in retea

- fiecare client are o lista de servere
- la pornire, incearca sa se conecteze pe rand la servere pana reuseste la primul
- fiecare nod este si server si asteapta cereri de conectare si cereri de procesare

### 2.2 Anuntarea portului de contact

- cand un client se conecteaza la un server, ii comunica portul pe care poate fi contactat inapoi
- cand un server accepta o conexiune noua de la un client, trebuie sa:
  - informeze serverul upstream
  - informeze ceilalti clienti conectati la el despre noul punct de contact

### 2.3 Subscriere si renuntare la subscriere

- clientul poate subscrie sau renunta la subscriere pentru chei diferite
- cand un server primeste o cerere de subscribe sau unsubscribe, trebuie sa:
  - contacteze serverul upstream
  - contacteze toti clientii conectati la el
  - transmita cine a facut operatia si pentru ce cheie

### 2.4 Acceptarea mesajelor binare

- clientii accepta mesaje binare identificate printr-o cheie
- un client poate trimite mesajul catre nodul la care este conectat

### 2.5 Livrarea catre consumatori

- cand un nod primeste un mesaj, trebuie sa identifice local consumatorii abonati la cheia respectiva
- apoi se conecteaza la acesti consumatori si le trimite mesajul spre procesare

### 2.6 Procesarea mesajului

- la primirea unui mesaj pentru procesare, un nod consumator executa comanda aferenta cheii mesajului
- comanda primeste ca argument datele mesajului
- rezultatul poate fi afisat local

## 3. Cerinte tehnice minime

- fiecare nod ruleaza simultan ca server si client
- trebuie sa existe stocare locala pentru:
  - conexiunile cunoscute
  - subscrierile cheie -> lista consumatori
- trebuie definit un protocol clar pentru:
  - handshake si port callback
  - subscribe/unsubscribe cu propagare
  - trimiterea mesajului binar
  - livrarea catre consumatori
- deconectarile trebuie tratate corect:
  - daca un client se deconecteaza, este eliminat din listele de consumatori
  - sistemul nu trebuie sa se blocheze
- trebuie sa existe o protectie minima impotriva flood-ului:
  - limita de marime pentru mesaj
  - sau timeout la procesare

## 4. Scenarii minime de demonstrat in video

1. Pornire a cel putin 3 noduri (in Docker sau procese separate).
2. Conectare in lant (un nod se conecteaza la primul server disponibil).
3. Subscriere la o cheie de catre un nod consumator si propagarea informatiei.
4. Trimiterea unui mesaj binar cu acea cheie de catre un producator.
5. Livrarea mesajului catre consumator(i) si executia comenzii asociate cheii.
6. Dezabonare si demonstratie ca mesajele ulterioare nu mai sunt livrate acelui consumator.
7. Deconectarea unui consumator si curatarea listelor astfel incat livrarea ulterioara sa nu blocheze sistemul.

## 5. Ghid de evaluare

### 5.1 Functionalitate de baza

- nod hibrid client + server si conectare la primul server disponibil
- subscriere si dezabonare pe chei cu propagare corecta a informatiei
- trimitere mesaj binar, livrare catre consumatorii abonati si procesare

### 5.2 Topologie si propagare

- anuntarea portului callback si distribuirea informatiei catre upstream si peers
- evidenta locala corecta pentru relatia cheie -> consumatori si livrare catre destinatarii corecti

### 5.3 Robustete si comportament corect

- server concurent stabil
- tratarea deconectarilor fara blocaje
- validari pentru chei lipsa, payload prea mare si cereri invalide

## 6. Clarificari

- comanda asociata cheii poate fi reala sau simulata
- trebuie sa se vada procesare diferita pentru chei diferite
- sistemul nu trebuie sa garanteze exact-once; best-effort este suficient
- interfata poate fi doar consola

## 7. Plan de lucru

### Faza 1: definire arhitectura si protocol

Se stabilesc mesajele, regulile de conectare, formatul handshake-ului, anuntarea callback-ului si modul in care se propaga subscribe/unsubscribe.

### Faza 2: nucleul nodului

Se implementeaza nodul hibrid client + server, listener-ul TCP, conectarea la primul server disponibil, evidenta conexiunilor si structurile de date locale.

### Faza 3: pub/sub si livrare

Se implementeaza subscribe/unsubscribe, mapa cheie -> consumatori, trimiterea mesajelor binare si livrarea catre consumatorii potriviti.

### Faza 4: procesarea mesajelor

Se implementeaza comanda asociata fiecarei chei, tratarea datelor primite si afisarea rezultatului procesarii.

### Faza 5: robustete

Se adauga tratarea deconectarilor, timeout-uri, limite de marime pentru mesaje si validari pentru cereri invalide.

### Faza 6: Docker si demonstratie

Se construieste scenariul de rulare cu cel putin 3 noduri, in Docker sau procese separate, si se pregateste demo-ul cerut.

## 8. Tehnologii de lucru

- Python 3.9+ cu networking standard
- pytest pentru teste
- Docker Desktop pentru rularea si demonstrarea celor 3 noduri
- logging cu un format clar si consecvent
- TinyDB doar daca se doreste persistenta; nu este obligatoriu pentru primul milestone

## 9. Ce mai este de facut pentru proiectul complet

### 9.1 Stabilizare protocol

- definirea finala a tuturor mesajelor si a campurilor obligatorii
- clarificarea raspunsurilor standard pentru handshake, subscribe, unsubscribe, publish si deliver
- documentarea stricta a formatului pentru date binare si a limitelor de marime

### 9.2 Robustete retea

- tratarea mai clara a reconectarii si a cazurilor in care un seed nu este disponibil
- curatarea sigura a peer-ilor si subscriber-ilor morti in toate ramurile de executie
- verificarea comportamentului la timeout si la conexiuni refuzate

### 9.3 Livrare si procesare

- confirmarea faptului ca mesajele binare sunt livrate corect catre toate destinatiile abonate
- verificarea scenariilor cu unsubscribe si cu republish dupa curatare
- adaugarea unor teste suplimentare pentru mai multi consumatori si chei diferite

### 9.4 Demo complet

- scenariu complet cu cel putin 3 noduri pornite in ordine clara
- demonstratie pentru conectare, subscribe, publish, deliver, unsubscribe si cleanup
- script sau lista de comenzi pentru rulare rapida in video

### 9.5 Integrare si livrare

- verificarea rulajului prin Docker Compose de la zero
- curatarea mesajelor de log astfel incat demo-ul sa fie usor de urmarit
- revizuirea README-ului final si a planului de prezentare

## 10. Cum se finalizeaza proiectul

- se verifica toate scenariile minime din cerinte
- se ruleaza suita de teste dupa fiecare modificare importanta
- se fixeaza orice regresie de protocol sau livrare inainte de prezentare
- se pregateste o varianta stabila pentru demonstratie si predare

## 11. Ce trebuie stabilit inainte de prezentare

- formatul exact al mesajelor folosite in demo
- ce chei vor fi folosite pentru demonstratie
- ce comanda executa fiecare cheie
- cum se pornesc cele 3 noduri
- cum se face scenariul complet pentru video
