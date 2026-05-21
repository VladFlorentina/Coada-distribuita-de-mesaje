@echo off

echo COMENZI UTILE (ordine pentru demo):
echo 1) Subscribe node2 (cheia demo):
echo    docker compose exec node1 dmq subscribe --target node1:5001 --subscriber node2@node2:5002 --key demo
echo 2) Subscribe node3 (cheia demo):
echo    docker compose exec node1 dmq subscribe --target node1:5001 --subscriber node3@node3:5003 --key demo
echo 3) Publish mesaj (trebuie sa livreze la node2 si node3):
echo    docker compose exec node1 dmq publish --target node1:5001 --key demo --payload "hello docker"
echo 4) Unsubscribe node2:
echo    docker compose exec node1 dmq unsubscribe --target node1:5001 --subscriber node2@node2:5002 --key demo
echo 5) Publish din nou (trebuie sa livreze doar la node3):
echo    docker compose exec node1 dmq publish --target node1:5001 --key demo --payload "hello again"
echo 6) Stop node3 (simuleaza deconectare consumator):
echo    docker compose stop node3
echo 7) Publish dupa stop (nu blocheaza, apare failure pt node3):
echo    docker compose exec node1 dmq publish --target node1:5001 --key demo --payload "after stop"
echo 8) Porneste node3 (optional, dupa demo):
echo    docker compose start node3
echo 9) Oprire totala:
echo    docker compose down

echo.
echo Scrie comanda dorita mai sus si apasa Enter.
