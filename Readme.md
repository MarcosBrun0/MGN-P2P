### 📡 Projeto P2P com DHT e Compartilhamento de Arquivos

Este projeto implementa uma rede peer-to-peer (P2P) com compartilhamento de arquivos e descoberta distribuída de peers via **DHT (Kademlia)**. Arquivos são enviados automaticamente entre peers ao serem adicionados à pasta monitorada.

### 🧩 Funcionalidades

- Conexão entre peers via socket TCP
- Compartilhamento automático de arquivos
- Descoberta de arquivos por nome usando DHT (Kademlia)
- Monitoramento de diretório usando `watchdog`


### ▶️ Como Executar

1. Instale as dependências:

```bash
pip install -r requirements.txt


```
Execute o primeiro peer:
```commandline
python p2p.py 5000
or
python3 p2p.py 5000
```

